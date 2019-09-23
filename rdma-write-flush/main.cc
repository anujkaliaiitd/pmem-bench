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
#include "latency.h"

DEFINE_uint64(is_client, 0, "Is this process a client?");

static constexpr size_t kBufSize = KB(128);  // Registered buffer size
static constexpr size_t kMinWriteSize = 64;
static constexpr size_t kMaxWriteSize = 1024;

// If true, we use a devdax-mapped buffer. If false, we use DRAM hugepages.
static constexpr bool kUsePmem = true;
static constexpr const char* kPmemFile = "/dev/dax0.0";

// Number of writes to flush. The (WRITE+READ) combos for all writes are
// issued in one postlist. Only the last READ in the postlist is signaled, so
// kNumWrites cannot be too large. Else we'll run into signaling issues.
static constexpr size_t kNumWritesToFlush = 1;

// If true, we issue only one signaled write and no reads
static constexpr bool kJustAWrite = true;

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

/// Get a random offset in the registered buffer with at least \p msg_size room
size_t get_256_aligned_random_offset(pcg64_fast& pcg, size_t msg_size) {
  size_t iters = 0;
  while (true) {
    size_t rand_offset = (pcg() % kBufSize);
    if (likely(kBufSize - rand_offset > msg_size)) return rand_offset;
    iters++;
    if (unlikely(iters > 10)) printf("Random offset took over 10 iters\n");
  }
}

void run_client() {
  Latency latency;
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

  // The +1s are for simpler postlist chain pointer math
  static constexpr size_t kArrSz = kNumWritesToFlush + 1;
  struct ibv_send_wr write_wr[kArrSz], read_wr[kArrSz];
  struct ibv_send_wr* bad_send_wr;
  struct ibv_sge write_sge[kArrSz], read_sge[kArrSz];
  struct ibv_wc wc;

  size_t write_size = kMinWriteSize;  // Increases by powers of two
  size_t num_iters = 0;

  // Remote memory is divided into write_size chunks. The RDMA writes use these
  // chunks in order.
  size_t write_chunk_idx = 0;

  // pcg64_fast pcg(pcg_extras::seed_seq_from<std::random_device>{});

  printf("#write_size median_us 5th_us 99th_us 999th_us\n");  // Stats header
  while (true) {
    if (num_iters == KB(256)) {
      printf("%zu %.1f %.1f %.1f %.1f\n", write_size, latency.perc(.50) / 10.0,
             latency.perc(.05) / 10.0, latency.perc(.99) / 10.0,
             latency.perc(.999) / 10.0);
      latency.reset();

      write_size *= 2;
      if (write_size > kMaxWriteSize) write_size = kMinWriteSize;

      num_iters = 0;
      write_chunk_idx = 0;
    }

    struct timespec start;
    clock_gettime(CLOCK_REALTIME, &start);

    // Enter the loop below with room for at least (kNumWritesToFlush + 1)
    // chunks. We don't use the last chunk because we read from there.
    if (write_chunk_idx + 1 >=
        (kBufSize / write_size) - kNumWritesToFlush - 1) {
      write_chunk_idx = 0;
    }

    // WRITE
    for (size_t i = 0; i < kNumWritesToFlush; i++) {
      const size_t remote_offset = write_chunk_idx * write_size;
      write_chunk_idx++;

      write_sge[i].addr =
          reinterpret_cast<uint64_t>(&cb->conn_buf[i * write_size]);
      write_sge[i].length = write_size;
      write_sge[i].lkey = cb->conn_buf_mr->lkey;

      write_wr[i].opcode = IBV_WR_RDMA_WRITE;
      write_wr[i].num_sge = 1;
      write_wr[i].sg_list = &write_sge[i];
      write_wr[i].send_flags = 0 /* unsignaled */;
      if (write_size <= kHrdMaxInline) {
        write_wr[i].send_flags |= IBV_SEND_INLINE;
      }

      write_wr[i].wr.rdma.remote_addr = srv_qp->buf_addr + remote_offset;
      write_wr[i].wr.rdma.rkey = srv_qp->rkey;

      // READ. We can read from any address.
      read_sge[i].addr =
          reinterpret_cast<uint64_t>(&cb->conn_buf[kBufSize - sizeof(size_t)]);
      read_sge[i].length = sizeof(size_t);  // Indepenent of write size
      read_sge[i].lkey = cb->conn_buf_mr->lkey;

      read_wr[i].opcode = IBV_WR_RDMA_READ;
      read_wr[i].num_sge = 1;
      read_wr[i].sg_list = &read_sge[i];
      read_wr[i].send_flags = 0;  // Unsignaled. The last read is signaled.
      read_wr[i].wr.rdma.remote_addr =
          srv_qp->buf_addr + kBufSize - sizeof(size_t);
      read_wr[i].wr.rdma.rkey = srv_qp->rkey;

      // Make a chain
      write_wr[i].next = &read_wr[i];
      read_wr[i].next = &write_wr[i + 1];
    }

    if (!kJustAWrite) {
      read_wr[kNumWritesToFlush - 1].send_flags = IBV_SEND_SIGNALED;
      read_wr[kNumWritesToFlush - 1].next = nullptr;
    } else {
      write_wr[0].send_flags |= IBV_SEND_SIGNALED;
      write_wr[0].next = nullptr;
    }

    int ret = ibv_post_send(cb->conn_qp[0], &write_wr[0], &bad_send_wr);
    rt_assert(ret == 0);
    hrd_poll_cq(cb->conn_cq[0], 1, &wc);  // Block till the RDMA read completes
    num_iters++;

    double us = ns_since(start) / 1000.0;
    latency.update(us * 10);
  }
}

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  FLAGS_is_client == 1 ? run_client() : run_server();
  return 0;
}
