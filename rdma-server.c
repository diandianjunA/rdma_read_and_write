#include "rdma-common.h"

static int on_connect_request(struct rdma_cm_id *id);
static int on_connection(struct rdma_cm_id *id);
static int on_disconnect(struct rdma_cm_id *id);
static int on_event(struct rdma_cm_event *event);
static void usage(const char *argv0);

int main(int argc, char **argv)
{
  struct sockaddr_in6 addr;
  struct rdma_cm_event *event = NULL;
  struct rdma_cm_id *listener = NULL;
  struct rdma_event_channel *ec = NULL;
  uint16_t port = 0;

  if (argc != 2)
    usage(argv[0]);

  if (strcmp(argv[1], "write") == 0)
    set_mode(M_WRITE);
  else if (strcmp(argv[1], "read") == 0)
    set_mode(M_READ);
  else
    usage(argv[0]);

  memset(&addr, 0, sizeof(addr));
  addr.sin6_family = AF_INET6;

  // 创建一个event channel，event channel是RDMA设备在操作完成后，
  // 或者有连接请求等事件发生时，用来通知应用程序的通道。其内部就是一个file descriptor,
  // 因此可以进行poll等操作。
  TEST_Z(ec = rdma_create_event_channel());
  // 创建一个rdma_cm_id, 概念上等价与socket编程时的listen socket。
  TEST_NZ(rdma_create_id(ec, &listener, NULL, RDMA_PS_TCP));
  // 绑定一个本地的地址和端口，以进行listen操作。
  TEST_NZ(rdma_bind_addr(listener, (struct sockaddr *)&addr));
  // 开始侦听客户端的连接请求
  TEST_NZ(rdma_listen(listener, 10)); /* backlog=10 is arbitrary */

  port = ntohs(rdma_get_src_port(listener));

  printf("listening on port %d.\n", port);

  // rdma_get_cm_event（channel，&event）
  // 这个调用就是作用在第一步创建的event channel上面，
  // 要从event channel中获取一个事件。
  // 这是个阻塞调用，只有有事件时才会返回。
  // 在一切正常的情况下，函数返回时会得到一个 RDMA_CM_EVENT_CONNECT_REQUEST事件，
  // 也就是说，有客户端发起连接了。
  // 在事件的参数里面，会有一个新的rdma_cm_id传入。
  while (rdma_get_cm_event(ec, &event) == 0) {
    struct rdma_cm_event event_copy;

    memcpy(&event_copy, event, sizeof(*event));
    // 对于每个从event channel得到的事件，都要调用ack函数，
    // 否则会产生内存泄漏。这一步的ack是对应前面的rdma_get_cm_event。
    // 每一次get调用，都要有对应的ack调用。
    rdma_ack_cm_event(event);

    if (on_event(&event_copy))
      break;
  }

  rdma_destroy_id(listener);
  rdma_destroy_event_channel(ec);

  return 0;
}

int on_connect_request(struct rdma_cm_id *id)
{
  struct rdma_conn_param cm_params;

  printf("received connection request.\n");
  build_connection(id);
  build_params(&cm_params);
  sprintf(get_local_message_region(id->context), "message from passive/server side with pid %d", getpid());
  // 至此，做好了全部的准备工作，可以调用accept接受客户端的这个请求了。
  TEST_NZ(rdma_accept(id, &cm_params));
  return 0;
}

int on_connection(struct rdma_cm_id *id)
{
  on_connect(id->context);

  return 0;
}

int on_disconnect(struct rdma_cm_id *id)
{
  printf("peer disconnected.\n");

  destroy_connection(id->context);
  return 0;
}

int on_event(struct rdma_cm_event *event)
{
  int r = 0;

  if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST)
    r = on_connect_request(event->id);
  else if (event->event == RDMA_CM_EVENT_ESTABLISHED)
    r = on_connection(event->id);
  else if (event->event == RDMA_CM_EVENT_DISCONNECTED)
    r = on_disconnect(event->id);
  else
    die("on_event: unknown event.");

  return r;
}

void usage(const char *argv0)
{
  fprintf(stderr, "usage: %s <mode>\n  mode = \"read\", \"write\"\n", argv0);
  exit(1);
}
