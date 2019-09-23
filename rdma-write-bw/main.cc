#include <fcntl.h>
#include <gflags/gflags.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <pcg/pcg_random.hpp>
#include <random>
#include <thread>
#include <vector>
#include "../libhrd_cpp/hrd.h"

DEFINE_uint64(is_client, 0, "Is this process a client?");

static constexpr size_t kBufSize = MB(1);  // Registered buffer size

// If true, we use a devdax-mapped buffer. If false, we use DRAM hugepages.
static constexpr bool kUsePmem = true;
static constexpr const char* kPmemFile = "/dev/dax0.0";

uint8_t* get_pmem_buf() {
  int fd = open(kPmemFile, O_RDWR);
  rt_assert(fd >= 0, "devdax open failed");

  size_t pmem_size = round_up<MB(2)>(kBufSize);  // Smaller sizes may fail
  void* buf =
      mmap(nullptr, pmem_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  rt_assert(buf != MAP_FAILED, "mmap failed for devdax");
  rt_assert(reinterpret_cast<size_t>(buf) % 256 == 0);
  memset(buf, 0, pmem_size);

  return reinterpret_cast<uint8_t*>(buf);
}

void run_server() {
  uint8_t* pmem_buf = nullptr;
  if (kUsePmem) pmem_buf = get_pmem_buf();

  struct hrd_conn_config_t conn_config;
  conn_config.num_qps = 1;
  conn_config.use_uc = false;
  conn_config.prealloc_buf = kUsePmem ? pmem_buf : nullptr;
  conn_config.buf_size = kBufSize;
  conn_config.buf_shm_key = kUsePmem ? -1 : 3185;

  auto* cb = hrd_ctrl_blk_init(0 /* id */, 0 /* port */, 0 /* numa */,
                               &conn_config, nullptr /* dgram config */);
  memset(const_cast<uint8_t*>(cb->conn_buf), 0, kBufSize);

  hrd_publish_conn_qp(cb, 0, "server");
  printf("main: Server published. Waiting for client.\n");

  hrd_qp_attr_t* clt_qp = nullptr;
  while (clt_qp == nullptr) {
    clt_qp = hrd_get_published_qp("client");
    if (clt_qp == nullptr) usleep(200000);
  }

  printf("main: Server %s found client! Connecting..\n", "server");
  hrd_connect_qp(cb, 0, clt_qp);
  hrd_publish_ready("server");
  printf("main: Server ready. Going to sleep.\n");

  while (true) sleep(1);
}

void run_client() {
  hrd_conn_config_t conn_config;
  conn_config.num_qps = 1;
  conn_config.use_uc = false;
  conn_config.prealloc_buf = nullptr;
  conn_config.buf_size = kBufSize;
  conn_config.buf_shm_key = 3185;

  auto* cb = hrd_ctrl_blk_init(0 /* id */, 0 /* port */, 0 /* numa */,
                               &conn_config, nullptr /* dgram config */);
  memset(const_cast<uint8_t*>(cb->conn_buf), 31, kBufSize);

  hrd_publish_conn_qp(cb, 0, "client");
  printf("main: Client published. Waiting for server.\n");

  hrd_qp_attr_t* srv_qp = nullptr;
  while (srv_qp == nullptr) {
    srv_qp = hrd_get_published_qp("server");
    if (srv_qp == nullptr) usleep(2000);
  }

  printf("main: Client found server. Connecting..\n");
  hrd_connect_qp(cb, 0, srv_qp);
  printf("main: Client connected!\n");

  hrd_wait_till_ready("server");

  struct ibv_send_wr write_wr, *bad_send_wr;
  struct ibv_sge write_sge;
  struct ibv_wc wc;
  size_t num_iters = 0;

  printf("#write_size_GB bandwidth_GBps\n");  // Stats header
  while (true) {
    struct timespec start;
    clock_gettime(CLOCK_REALTIME, &start);

    write_sge.addr = reinterpret_cast<uint64_t>(&cb->conn_buf[0]);
    write_sge.length = kBufSize;
    write_sge.lkey = cb->conn_buf_mr->lkey;

    write_wr.opcode = IBV_WR_RDMA_WRITE;
    write_wr.num_sge = 1;
    write_wr.sg_list = &write_sge;
    write_wr.send_flags = IBV_SEND_SIGNALED; /* unsignaled */
    ;

    write_wr.wr.rdma.remote_addr = srv_qp->buf_addr;
    write_wr.wr.rdma.rkey = srv_qp->rkey;
    write_wr.next = nullptr;

    int ret = ibv_post_send(cb->conn_qp[0], &write_wr, &bad_send_wr);
    rt_assert(ret == 0);
    hrd_poll_cq(cb->conn_cq[0], 1, &wc);  // Block till the write completes
    num_iters++;

    double sec = sec_since(start);
    printf("%.2f %.2f\n", kBufSize * 1.0 / GB(1),
           kBufSize * 1.0 / (GB(1) * sec));
  }
}

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  FLAGS_is_client == 1 ? run_client() : run_server();
  return 0;
}
