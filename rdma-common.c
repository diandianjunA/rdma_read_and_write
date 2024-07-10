#include "rdma-common.h"

static const int RDMA_BUFFER_SIZE = 1024;

struct message {
    enum {
        MSG_MR,
        MSG_DONE
    } type;

    union {
        struct ibv_mr mr;
    } data;
};

struct context {
    struct ibv_context *ctx;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct ibv_comp_channel *comp_channel;

    pthread_t cq_poller_thread;
};

struct connection {
    struct rdma_cm_id *id;
    struct ibv_qp *qp;

    int connected;

    struct ibv_mr *recv_mr;
    struct ibv_mr *send_mr;
    struct ibv_mr *rdma_local_mr;
    struct ibv_mr *rdma_remote_mr;

    struct ibv_mr peer_mr;

    struct message *recv_msg;
    struct message *send_msg;

    char *rdma_local_region;
    char *rdma_remote_region;

    enum {
        SS_INIT,
        SS_MR_SENT,
        SS_RDMA_SENT,
        SS_DONE_SENT
    } send_state;

    enum {
        RS_INIT,
        RS_MR_RECV,
        RS_DONE_RECV
    } recv_state;
};

static void build_context(struct ibv_context *verbs);

static void build_qp_attr(struct ibv_qp_init_attr *qp_attr);

static char *get_peer_message_region(struct connection *conn);

static void on_completion(struct ibv_wc *);

static void *poll_cq(void *);

static void post_receives(struct connection *conn);

static void register_memory(struct connection *conn);

static void send_message(struct connection *conn);

static struct context *s_ctx = NULL;
static enum mode s_mode = M_WRITE;

void die(const char *reason) {
    fprintf(stderr, "%s\n", reason);
    exit(EXIT_FAILURE);
}

void build_connection(struct rdma_cm_id *id) {
    struct connection *conn;
    struct ibv_qp_init_attr qp_attr;

    build_context(id->verbs);
    build_qp_attr(&qp_attr);

    // 创建一个queue pair, 一个queue pair包括一个发送queue和一个接收queue.
    // 指定使用前面创建的cq作为完成队列。该qp创建时就指定关联到前面创建的pd上。
    TEST_NZ(rdma_create_qp(id, s_ctx->pd, &qp_attr));

    id->context = conn = (struct connection *) malloc(sizeof(struct connection));

    conn->id = id;
    conn->qp = id->qp;

    conn->send_state = SS_INIT;
    conn->recv_state = RS_INIT;

    conn->connected = 0;

    register_memory(conn);
    post_receives(conn);
}

void build_context(struct ibv_context *verbs) {
    if (s_ctx) {
        if (s_ctx->ctx != verbs)
            die("cannot handle events in more than one context.");

        return;
    }

    s_ctx = (struct context *) malloc(sizeof(struct context));

    s_ctx->ctx = verbs;

    // 创建一个protection domain。
    // protection domain可以看作是一个内存保护单位，
    // 在内存区域和队列直接建立一个关联关系，防止未授权的访问。
    TEST_Z(s_ctx->pd = ibv_alloc_pd(s_ctx->ctx));
    // 和之前创建的event channel类似，
    // 这也是一个event channel，
    // 但只用来报告【完成队列】里面的事件。
    // 当【完成队列】里有新的任务完成时，就通过这个channel向应用程序报告。
    TEST_Z(s_ctx->comp_channel = ibv_create_comp_channel(s_ctx->ctx));
    // 创建【完成队列】，创建时就指定使用前面的channel。
    TEST_Z(s_ctx->cq = ibv_create_cq(s_ctx->ctx, 10, NULL, s_ctx->comp_channel, 0)); /* cqe=10 is arbitrary */
    TEST_NZ(ibv_req_notify_cq(s_ctx->cq, 0));
    TEST_NZ(pthread_create(&s_ctx->cq_poller_thread, NULL, poll_cq, NULL));
}

void build_params(struct rdma_conn_param *params) {
    memset(params, 0, sizeof(*params));

    params->initiator_depth = params->responder_resources = 1;
    params->rnr_retry_count = 7; /* infinite retry */
}

void build_qp_attr(struct ibv_qp_init_attr *qp_attr) {
    memset(qp_attr, 0, sizeof(*qp_attr));

    qp_attr->send_cq = s_ctx->cq;
    qp_attr->recv_cq = s_ctx->cq;
    qp_attr->qp_type = IBV_QPT_RC;

    qp_attr->cap.max_send_wr = 10;
    qp_attr->cap.max_recv_wr = 10;
    qp_attr->cap.max_send_sge = 1;
    qp_attr->cap.max_recv_sge = 1;
}

void destroy_connection(void *context) {
    struct connection *conn = (struct connection *) context;

    rdma_destroy_qp(conn->id);

    ibv_dereg_mr(conn->send_mr);
    ibv_dereg_mr(conn->recv_mr);
    ibv_dereg_mr(conn->rdma_local_mr);
    ibv_dereg_mr(conn->rdma_remote_mr);

    free(conn->send_msg);
    free(conn->recv_msg);
    free(conn->rdma_local_region);
    free(conn->rdma_remote_region);

    rdma_destroy_id(conn->id);

    free(conn);
}

void *get_local_message_region(void *context) {
    if (s_mode == M_WRITE)
        return ((struct connection *) context)->rdma_local_region;
    else
        return ((struct connection *) context)->rdma_remote_region;
}

char *get_peer_message_region(struct connection *conn) {
    if (s_mode == M_WRITE)
        return conn->rdma_remote_region;
    else
        return conn->rdma_local_region;
}

void on_completion(struct ibv_wc *wc) {
    struct connection *conn = (struct connection *) (uintptr_t) wc->wr_id;

    if (wc->status != IBV_WC_SUCCESS)
        die("on_completion: status is not IBV_WC_SUCCESS.");

    if (wc->opcode & IBV_WC_RECV) {
        conn->recv_state++;

        if (conn->recv_msg->type == MSG_MR) {
            memcpy(&conn->peer_mr, &conn->recv_msg->data.mr, sizeof(conn->peer_mr));
            post_receives(conn); /* only rearm for MSG_MR */

            if (conn->send_state == SS_INIT) /* received peer's MR before sending ours, so send ours back */
                send_mr(conn);
        }

    } else {
        conn->send_state++;
        printf("send completed successfully.\n");
    }

    if (conn->send_state == SS_MR_SENT && conn->recv_state == RS_MR_RECV) {
        struct ibv_send_wr wr, *bad_wr = NULL;
        struct ibv_sge sge;

        if (s_mode == M_WRITE) {
            printf("received MSG_MR. writing message to remote memory...\n");
        } else {
            printf("received MSG_MR. reading message from remote memory...\n");
        }
        memset(&wr, 0, sizeof(wr));

        wr.wr_id = (uintptr_t) conn;
        wr.opcode = (s_mode == M_WRITE) ? IBV_WR_RDMA_WRITE : IBV_WR_RDMA_READ;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.wr.rdma.remote_addr = (uintptr_t) conn->peer_mr.addr;
        wr.wr.rdma.rkey = conn->peer_mr.rkey;

        sge.addr = (uintptr_t) conn->rdma_local_region;
        sge.length = RDMA_BUFFER_SIZE;
        sge.lkey = conn->rdma_local_mr->lkey;

        TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));

        conn->send_msg->type = MSG_DONE;
        send_message(conn);
        clock_t start_t = clock();
        printf("time stamp start read: %f \n", (double) start_t / CLOCKS_PER_SEC);
    } else if (conn->send_state == SS_DONE_SENT && conn->recv_state == RS_DONE_RECV) {
        clock_t end_t = clock();
        printf("time stamp end read: %f \n", (double) end_t / CLOCKS_PER_SEC);
        printf("remote buffer: %s\n", get_peer_message_region(conn));
        rdma_disconnect(conn->id);
    }
}

void on_connect(void *context) {
    ((struct connection *) context)->connected = 1;
}

void *poll_cq(void *ctx) {
    struct ibv_cq *cq;
    struct ibv_wc wc;

    while (1) {
        TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
        ibv_ack_cq_events(cq, 1);
        TEST_NZ(ibv_req_notify_cq(cq, 0));

        while (ibv_poll_cq(cq, 1, &wc))
            on_completion(&wc);
    }

    return NULL;
}

void post_receives(struct connection *conn) {
    struct ibv_recv_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    wr.wr_id = (uintptr_t) conn;
    wr.next = NULL;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    sge.addr = (uintptr_t) conn->recv_msg;
    sge.length = sizeof(struct message);
    sge.lkey = conn->recv_mr->lkey;

    TEST_NZ(ibv_post_recv(conn->qp, &wr, &bad_wr));
}

void register_memory(struct connection *conn) {
    conn->send_msg = malloc(sizeof(struct message));
    conn->recv_msg = malloc(sizeof(struct message));

    conn->rdma_local_region = malloc(RDMA_BUFFER_SIZE);
    conn->rdma_remote_region = malloc(RDMA_BUFFER_SIZE);

    // 注册内存区域。RDMA使用的内存，必须事先进行注册。
    // 这个是可以理解的，DMA的内存在边界对齐，能否被swap等方面，都有要求。
    TEST_Z(conn->send_mr = ibv_reg_mr(
            s_ctx->pd,
            conn->send_msg,
            sizeof(struct message),
            0));

    TEST_Z(conn->recv_mr = ibv_reg_mr(
            s_ctx->pd,
            conn->recv_msg,
            sizeof(struct message),
            IBV_ACCESS_LOCAL_WRITE));

    TEST_Z(conn->rdma_local_mr = ibv_reg_mr(
            s_ctx->pd,
            conn->rdma_local_region,
            RDMA_BUFFER_SIZE,
            ((s_mode == M_WRITE) ? 0 : IBV_ACCESS_LOCAL_WRITE)));

    TEST_Z(conn->rdma_remote_mr = ibv_reg_mr(
            s_ctx->pd,
            conn->rdma_remote_region,
            RDMA_BUFFER_SIZE,
            ((s_mode == M_WRITE) ? (IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE) : IBV_ACCESS_REMOTE_READ)));
}

void send_message(struct connection *conn) {
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    memset(&wr, 0, sizeof(wr));

    wr.wr_id = (uintptr_t) conn;
    wr.opcode = IBV_WR_SEND;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.send_flags = IBV_SEND_SIGNALED;

    sge.addr = (uintptr_t) conn->send_msg;
    sge.length = sizeof(struct message);
    sge.lkey = conn->send_mr->lkey;

    while (!conn->connected);

    TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
}

void send_mr(void *context) {
    struct connection *conn = (struct connection *) context;

    conn->send_msg->type = MSG_MR;
    memcpy(&conn->send_msg->data.mr, conn->rdma_remote_mr, sizeof(struct ibv_mr));

    send_message(conn);
}

void set_mode(enum mode m) {
    s_mode = m;
}
