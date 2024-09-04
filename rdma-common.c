#include "rdma-common.h"

// 修改这个以后记得在服务器上也修改一下
// 不batch的情况下，BUFFER_SIZE = 4096 的时候，WRITE 40MB需要的时间大概是3w us，
// 同样不batch，size = 16 * 4096 的时候，WRITE 40MB需要的时间是14512 us
// 预计后续实现了Doorbell Batching的话，还能比这个更快
// 感觉这样可以初步验证“被动上锁”的想法，因为需要支持同步操作的话，那么resize需要 1~2 s 这样的时间
// 如果上锁的话，虽然这个过程中不允许写和插入，但是整体处理起来要简单很多，而且可能也就是需要一小段时间(几千 us)
// 有一个点需要考虑：那就是读kv。但是实际上也是可以批量读取的，因为上锁以后不允许修改
// 但是还是需要重新设计一下插入和更新的流程，因为如果先检查锁（比如说在把桶读回来后发现没上锁，然后修改，但是可能在这个过程中触发了resize上锁，那么这个修改就有可能丢失）
// 如果把 CAS 改成 WRITE 会不会好弄一些，因为不会失败
static const int RDMA_BUFFER_SIZE = 4096 * 16;

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
static char * get_peer_message_region(struct connection *conn);
static void on_completion(struct ibv_wc *);
static void * poll_cq(void *);
static int poll_cq_no_recursive(void *ctx);
static void post_receives(struct connection *conn);
static void register_memory(struct connection *conn);
static void send_message(struct connection *conn);

static struct context *s_ctx = NULL;
static enum mode s_mode = M_WRITE;

void die(const char *reason)
{
  fprintf(stderr, "%s\n", reason);
  exit(EXIT_FAILURE);
}

void build_connection(struct rdma_cm_id *id)
{
  struct connection *conn;
  struct ibv_qp_init_attr qp_attr;

  build_context(id->verbs);
  build_qp_attr(&qp_attr);

  TEST_NZ(rdma_create_qp(id, s_ctx->pd, &qp_attr));

  id->context = conn = (struct connection *)malloc(sizeof(struct connection));

  conn->id = id;
  conn->qp = id->qp;

  conn->send_state = SS_INIT;
  conn->recv_state = RS_INIT;

  conn->connected = 0;

  register_memory(conn);
  post_receives(conn);
}

void build_context(struct ibv_context *verbs)
{
  if (s_ctx) {
    if (s_ctx->ctx != verbs)
      die("cannot handle events in more than one context.");

    return;
  }

  s_ctx = (struct context *)malloc(sizeof(struct context));

  s_ctx->ctx = verbs;

  TEST_Z(s_ctx->pd = ibv_alloc_pd(s_ctx->ctx));
  TEST_Z(s_ctx->comp_channel = ibv_create_comp_channel(s_ctx->ctx));
  TEST_Z(s_ctx->cq = ibv_create_cq(s_ctx->ctx, 10, NULL, s_ctx->comp_channel, 0)); /* cqe=10 is arbitrary */
  TEST_NZ(ibv_req_notify_cq(s_ctx->cq, 0));

  TEST_NZ(pthread_create(&s_ctx->cq_poller_thread, NULL, poll_cq, NULL));
}

void build_params(struct rdma_conn_param *params)
{
  memset(params, 0, sizeof(*params));

  params->initiator_depth = params->responder_resources = 1;
  params->rnr_retry_count = 7; /* infinite retry */
}

void build_qp_attr(struct ibv_qp_init_attr *qp_attr)
{
  memset(qp_attr, 0, sizeof(*qp_attr));

  qp_attr->send_cq = s_ctx->cq;
  qp_attr->recv_cq = s_ctx->cq;
  qp_attr->qp_type = IBV_QPT_RC;

  qp_attr->cap.max_send_wr = 10;
  qp_attr->cap.max_recv_wr = 10;
  qp_attr->cap.max_send_sge = 1;
  qp_attr->cap.max_recv_sge = 1;
}

void destroy_connection(void *context)
{
  struct connection *conn = (struct connection *)context;

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

void * get_local_message_region(void *context)
{
  if (s_mode == M_WRITE)
    return ((struct connection *)context)->rdma_local_region;
  else
    return ((struct connection *)context)->rdma_remote_region;
}

char * get_peer_message_region(struct connection *conn)
{
  if (s_mode == M_WRITE)
    return conn->rdma_remote_region;
  else
    return conn->rdma_local_region;
}

/*******
 * 
 * 
 * 可以在这里加上一些函数
 * 
 * 
 */

void bulky_write(struct connection *conn){
  struct ibv_send_wr *bad_wr = NULL;
  const int k = 1; // 每一批次的指令数量
  struct ibv_send_wr wr[k];
  struct ibv_sge sg[k];
  const int rounds = 40 * 1024 * 1024 / RDMA_BUFFER_SIZE;
  for(int i = 0; i < rounds; i += k){
    // k = 1 的时候是没有问题的，结果大概是 3w us，但是 k = 16 就不行了
    // 怀疑：每个wr都要有自己的sge？
    for(int j = 0; j < k && i < rounds; j++){
      memset(&sg[j], 0, sizeof(struct ibv_sge));
      memset(&wr[j], 0, sizeof(struct ibv_send_wr));

      sg[j].addr = (uintptr_t)conn->rdma_local_region;
      sg[j].length = RDMA_BUFFER_SIZE;
      sg[j].lkey = conn->rdma_local_mr->lkey;

      wr[j].next = (j == k - 1 || i == rounds - 1) ? NULL : &wr[j + 1];
      wr[j].wr_id = (uintptr_t)conn;
      wr[j].opcode = IBV_WR_RDMA_WRITE;
      wr[j].sg_list = &sg[j];
      wr[j].num_sge = 1;
      // 发现好像是这里的标志设置的不对
      if(j == k - 1)
        wr[j].send_flags = IBV_SEND_SIGNALED;
      wr[j].wr.rdma.remote_addr = (uintptr_t)conn->peer_mr.addr;
      wr[j].wr.rdma.rkey = conn->peer_mr.rkey;
    }
    //需要研究一下怎么把这个弄成一个batch，每次只发一条、poll一条还是太慢了，而且还会报错
    //这样的时间大致是0.46s
    // 妈的这里返回值12代表什么啊
    int rc = ibv_post_send(conn->qp, &wr[0], &bad_wr);
    struct ibv_wc wc;
    //这个cq应该不是通过channel获得的吧。。。
    //channel的cq应该是建立连接的时候用的，我估计得自己搞一个，因为它原来的代码没有poll
    //但是创建qp的时候是给了这个cq的，按理来说没问题呀
    //printf("rc = %d\n",rc);
    assert(rc == 0);
    int new_cnt = 0, cnt = 0;
    // 加上这一行也没啥用，难道说不是CQ的问题吗
    struct ibv_cq *cq = NULL;
    cq = conn->qp->send_cq;
    do {
      // 如果不需要sync的话，那就不 poll_cq
      new_cnt = ibv_poll_cq(cq,1,&wc);
      cnt += new_cnt;
      if (new_cnt < 0) {
        printf("Failed to poll completions from the CQ\n");
        exit(-1);
      }
      if(new_cnt == 0)
        continue;
      if(wc.status != IBV_WC_SUCCESS){
        exit(-1);
      }
    } while(cnt < 1);
    assert(wc.status == IBV_WC_SUCCESS);
  }
}

void read_kv(struct connection *conn){
  const int cas_rounds = 163840;
  struct ibv_send_wr *bad_wr = NULL;
  const int k = 1; // 每一批次的指令数量
  struct ibv_send_wr wr[k];
  struct ibv_sge sg[k];
  for(int i = 0; i < cas_rounds; i += k){
      // 每一轮实际上是一次sync读+一次CAS
      for(int j = 0; j < k && i < cas_rounds; j++){
        memset(&sg[j], 0, sizeof(struct ibv_sge));
        memset(&wr[j], 0, sizeof(struct ibv_send_wr));

        sg[j].addr = (uintptr_t)conn->rdma_local_region;
        sg[j].length = 256; // 先算kv块的大小是256字节
        sg[j].lkey = conn->rdma_local_mr->lkey;

        wr[j].next = (j == k - 1 || i == cas_rounds - 1) ? NULL : &wr[j + 1];
        wr[j].wr_id = (uintptr_t)conn;
        // 为啥write可以，read就不行了
        // 尼玛真逆天，要么只能写要么只能读
        // 实测如果命令行给read，那么就可以
        wr[j].opcode = IBV_WR_RDMA_READ; 
        wr[j].sg_list = &sg[j];
        wr[j].num_sge = 1;
        // 发现好像是这里的标志设置的不对
        if(j == k - 1)
          wr[j].send_flags = IBV_SEND_SIGNALED;
        wr[j].wr.rdma.remote_addr = (uintptr_t)conn->peer_mr.addr;
        wr[j].wr.rdma.rkey = conn->peer_mr.rkey;
      }
      //需要研究一下怎么把这个弄成一个batch，每次只发一条、poll一条还是太慢了，而且还会报错
      //这样的时间大致是0.46s
      // 妈的这里返回值12代表什么啊
      int rc = ibv_post_send(conn->qp, &wr[0], &bad_wr);
      struct ibv_wc wc;
      //这个cq应该不是通过channel获得的吧。。。
      //channel的cq应该是建立连接的时候用的，我估计得自己搞一个，因为它原来的代码没有poll
      //但是创建qp的时候是给了这个cq的，按理来说没问题呀
      //printf("rc = %d\n",rc);
      assert(rc == 0);
      int new_cnt = 0, cnt = 0;
      // 加上这一行也没啥用，难道说不是CQ的问题吗
      struct ibv_cq *cq = NULL;
      cq = conn->qp->send_cq;
      do {

        new_cnt = ibv_poll_cq(cq,1,&wc);
        cnt += new_cnt;
        if (new_cnt < 0) {
          printf("Failed to poll completions from the CQ\n");
          exit(-1);
        }
        if(new_cnt == 0)
          continue;
        if(wc.status != IBV_WC_SUCCESS){
          exit(-1);
        }
      } while(cnt < 1);
      assert(wc.status == IBV_WC_SUCCESS);
    }
}

void compare_and_swap(struct connection *conn){ // 先用粒度小一些的read模拟吧，实际上cas和read的吞吐率在没有冲突的情况下是差不多的
  const int cas_rounds = 1000000;
  struct ibv_send_wr *bad_wr = NULL;
  const int k = 1; // 每一批次的指令数量
  struct ibv_send_wr wr[k];
  struct ibv_sge sg[k];
  for(int i = 0; i < cas_rounds; i += k){
      // 每一轮实际上是一次sync读+一次CAS
      for(int j = 0; j < k && i < cas_rounds; j++){
        memset(&sg[j], 0, sizeof(struct ibv_sge));
        memset(&wr[j], 0, sizeof(struct ibv_send_wr));

        sg[j].addr = (uintptr_t)conn->rdma_local_region;
        sg[j].length = 8; // 先算kv块的大小是256字节
        sg[j].lkey = conn->rdma_local_mr->lkey;

        wr[j].next = (j == k - 1 || i == cas_rounds - 1) ? NULL : &wr[j + 1];
        wr[j].wr_id = (uintptr_t)conn;
        // 为啥write可以，read就不行了
        // 尼玛真逆天，要么只能写要么只能读
        // 实测如果命令行给read，那么就可以
        wr[j].opcode = IBV_WR_RDMA_READ; 
        wr[j].sg_list = &sg[j];
        wr[j].num_sge = 1;
        // 发现好像是这里的标志设置的不对
        if(j == k - 1)
          wr[j].send_flags = IBV_SEND_SIGNALED;
        wr[j].wr.rdma.remote_addr = (uintptr_t)conn->peer_mr.addr;
        wr[j].wr.rdma.rkey = conn->peer_mr.rkey;
      }
      //需要研究一下怎么把这个弄成一个batch，每次只发一条、poll一条还是太慢了，而且还会报错
      //这样的时间大致是0.46s
      // 妈的这里返回值12代表什么啊
      int rc = ibv_post_send(conn->qp, &wr[0], &bad_wr);
      struct ibv_wc wc;
      //这个cq应该不是通过channel获得的吧。。。
      //channel的cq应该是建立连接的时候用的，我估计得自己搞一个，因为它原来的代码没有poll
      //但是创建qp的时候是给了这个cq的，按理来说没问题呀
      //printf("rc = %d\n",rc);
      assert(rc == 0);
      int new_cnt = 0, cnt = 0;
      // 加上这一行也没啥用，难道说不是CQ的问题吗
      struct ibv_cq *cq = NULL;
      cq = conn->qp->send_cq;
      do {

        new_cnt = ibv_poll_cq(cq,1,&wc);
        cnt += new_cnt;
        if (new_cnt < 0) {
          printf("Failed to poll completions from the CQ\n");
          exit(-1);
        }
        if(new_cnt == 0)
          continue;
        if(wc.status != IBV_WC_SUCCESS){
          exit(-1);
        }
      } while(cnt < 1);
      assert(wc.status == IBV_WC_SUCCESS);
    }
}

/*****
 * 
 * 
 * 其他的不要修改
 * 
 * 
 */

void on_completion(struct ibv_wc *wc)
{
  struct connection *conn = (struct connection *)(uintptr_t)wc->wr_id;

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

  //这个地方直接写死了，想办法自己写个函数来处理，同时令poll_cq成功一次就退出
  if (conn->send_state == SS_MR_SENT && conn->recv_state == RS_MR_RECV) {

    if (s_mode == M_WRITE)
      printf("received MSG_MR. writing message to remote memory...\n");
    else
      printf("received MSG_MR. reading message from remote memory...\n");

    //memset(&wr, 0, sizeof(wr));

/************************************************ 
 * 
 * 
 * 
 * 
 * 从这里开始修改
 * 
 * 
 * 
 * 
*/

//GALA:就是要修改这个函数，把它变成想要的
//后台有一个线程一直poll，所以不用担心这个
//但是也有个问题，那就是如果需要sync的话，看下怎么处理
//搜索poll_cq，把后台的停掉，前台调用这个函数

    // bulky write， 10K 轮
    struct timeval start, end;
    gettimeofday(&start, NULL);

    bulky_write(conn);
    
    gettimeofday(&end,NULL);
    long dur = 1000000 * (end.tv_sec - start.tv_sec) + end.tv_usec - start.tv_usec;
    printf("bulky write took %ld us.\n", dur);

    //
    //
    // READ KVs
    gettimeofday(&start, NULL);

    read_kv(conn); // 目前只实现了sync read，实际上如果假设每次cas都成功的话，那么需要的时间应该也是差不多的
    
    gettimeofday(&end,NULL);
    dur = 1000000 * (end.tv_sec - start.tv_sec) + end.tv_usec - start.tv_usec;
    printf("READ kvs took %ld us.\n", dur);

    //
    //
    // CAS 100w 次
    gettimeofday(&start, NULL);

    compare_and_swap(conn); // 目前只实现了sync read，实际上如果假设每次cas都成功的话，那么需要的时间应该也是差不多的
    
    gettimeofday(&end,NULL);
    dur = 1000000 * (end.tv_sec - start.tv_sec) + end.tv_usec - start.tv_usec;
    printf("CAS took %ld us.\n", dur);

/************************************************ 
 * 
 * 
 * 
 * 
 * 修改结束
 * 
 * 
 * 
 * 
*/

    conn->send_msg->type = MSG_DONE;
    send_message(conn);
    // 调用这个函数,但是暂时不确定是否正确
    // 要把这一部分改成循环
    int cnt = 0;
    while(poll_cq_no_recursive(NULL) && cnt <= 0)
      cnt ++;

  } else if (conn->send_state == SS_DONE_SENT && conn->recv_state == RS_DONE_RECV) {
    printf("remote buffer: %s\n", get_peer_message_region(conn));
    rdma_disconnect(conn->id);
  }
}

void on_connect(void *context)
{
  ((struct connection *)context)->connected = 1;
}

// TODO：写一个新的poll_cq函数，避免重复调用on_completion
int poll_cq_no_recursive(void *ctx){
  struct ibv_cq *cq;
  struct ibv_wc wc;
  //printf("1111\n");

  while (1) {
    TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
    ibv_ack_cq_events(cq, 1);
    TEST_NZ(ibv_req_notify_cq(cq, 0));

    return ibv_poll_cq(cq, 1, &wc);

    // while (ibv_poll_cq(cq, 1, &wc)){
    //   //on_completion(&wc);
    //   return NULL;
    // }
  }

  return 0;
}

void * poll_cq(void *ctx)
{
  struct ibv_cq *cq;
  struct ibv_wc wc;

  while (1) {
    TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
    ibv_ack_cq_events(cq, 1);
    TEST_NZ(ibv_req_notify_cq(cq, 0));

    while (ibv_poll_cq(cq, 1, &wc)){
      on_completion(&wc);
      //return NULL;
    }
  }

  return NULL;
}

void post_receives(struct connection *conn)
{
  struct ibv_recv_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;

  wr.wr_id = (uintptr_t)conn;
  wr.next = NULL;
  wr.sg_list = &sge;
  wr.num_sge = 1;

  sge.addr = (uintptr_t)conn->recv_msg;
  sge.length = sizeof(struct message);
  sge.lkey = conn->recv_mr->lkey;

  TEST_NZ(ibv_post_recv(conn->qp, &wr, &bad_wr));
}

void register_memory(struct connection *conn)
{
  conn->send_msg = malloc(sizeof(struct message));
  conn->recv_msg = malloc(sizeof(struct message));

  conn->rdma_local_region = malloc(RDMA_BUFFER_SIZE);
  conn->rdma_remote_region = malloc(RDMA_BUFFER_SIZE);

  TEST_Z(conn->send_mr = ibv_reg_mr(
    s_ctx->pd, 
    conn->send_msg, 
    sizeof(struct message), 
    IBV_ACCESS_LOCAL_WRITE));

  TEST_Z(conn->recv_mr = ibv_reg_mr(
    s_ctx->pd, 
    conn->recv_msg, 
    sizeof(struct message), 
    // IBV_ACCESS_LOCAL_WRITE | ((s_mode == M_WRITE) ? IBV_ACCESS_REMOTE_WRITE : IBV_ACCESS_REMOTE_READ)));
    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ));

  TEST_Z(conn->rdma_local_mr = ibv_reg_mr(
    s_ctx->pd, 
    conn->rdma_local_region, 
    RDMA_BUFFER_SIZE, 
    IBV_ACCESS_LOCAL_WRITE));

  TEST_Z(conn->rdma_remote_mr = ibv_reg_mr(
    s_ctx->pd, 
    conn->rdma_remote_region, 
    RDMA_BUFFER_SIZE, 
    // IBV_ACCESS_LOCAL_WRITE | ((s_mode == M_WRITE) ? IBV_ACCESS_REMOTE_WRITE : IBV_ACCESS_REMOTE_READ)));
    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ));
}

void send_message(struct connection *conn)
{
  struct ibv_send_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;

  memset(&wr, 0, sizeof(wr));

  wr.wr_id = (uintptr_t)conn;
  wr.opcode = IBV_WR_SEND;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;

  sge.addr = (uintptr_t)conn->send_msg;
  sge.length = sizeof(struct message);
  sge.lkey = conn->send_mr->lkey;

  while (!conn->connected);

  TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
}

void send_mr(void *context)
{
  struct connection *conn = (struct connection *)context;

  conn->send_msg->type = MSG_MR;
  memcpy(&conn->send_msg->data.mr, conn->rdma_remote_mr, sizeof(struct ibv_mr));

  send_message(conn);
}

void set_mode(enum mode m)
{
  s_mode = m;
}
