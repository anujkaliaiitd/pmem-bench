#include <fcntl.h>
#include <gflags/gflags.h>
#include <libpmem.h>
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
DEFINE_uint64(machine_id, 0, "Index among client machines (for clients)");
DEFINE_uint64(min_write_size, 0, "Client's min RDMA write size");
DEFINE_uint64(max_write_size, 0, "Client's max RDMA write size");
DEFINE_uint64(window_size, 0, "Number of writes outstanding at client");

static constexpr size_t kPmemFileSize = GB(4);

// If true, server zeroes out its buffer and reports write throughput
static constexpr bool kZeroServerBuf = true;

// If true, we use a devdax-mapped buffer. If false, we use DRAM hugepages.
static constexpr bool kUsePmem = true;
static constexpr const char* kPmemFile = "/dev/dax0.0";

// If true, we use read-after-write to force persistence
static constexpr bool kReadAfterWrite = true;

static constexpr bool kVerbose = false;

// Map the devdax buffer at the server
uint8_t* get_pmem_buf_server() {
  int fd = open(kPmemFile, O_RDWR);
  rt_assert(fd >= 0, "devdax open failed");

  void* buf =
      mmap(nullptr, kPmemFileSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  rt_assert(buf != MAP_FAILED, "mmap failed for devdax");
  rt_assert(reinterpret_cast<size_t>(buf) % 256 == 0);

  return reinterpret_cast<uint8_t*>(buf);
}

void server_func() {
  uint8_t* pmem_buf = nullptr;
  if (kUsePmem) {
    pmem_buf = get_pmem_buf_server();

    // Fill in the persistent buffer, also sanity-check local write throughput
    if (kZeroServerBuf) {
      printf("main: Zero-ing pmem buffer\n");
      struct timespec start;
      clock_gettime(CLOCK_REALTIME, &start);
      pmem_memset_persist(pmem_buf, 0, kPmemFileSize);
      printf("main: Zero-ed %f MB of pmem at %.1f GB/s\n",
             kPmemFileSize * 1.0 / MB(1),
             kPmemFileSize / (1000000000.0 * sec_since(start)));
    }
  }

  struct hrd_conn_config_t conn_config;
  conn_config.num_qps = 1;
  conn_config.use_uc = false;
  conn_config.prealloc_buf = kUsePmem ? pmem_buf : nullptr;
  conn_config.buf_size = kPmemFileSize;
  conn_config.buf_shm_key = kUsePmem ? -1 : 3185;

  auto* cb = hrd_ctrl_blk_init(0 /* id */, 0 /* port */, 0 /* numa */,
                               &conn_config, nullptr /* dgram config */);

  // Publish server QP
  auto srv_qp_name = std::string("server");
  hrd_publish_conn_qp(cb, 0, srv_qp_name.c_str());

  printf("main: Server published. Waiting for client\n");

  auto conn_name = std::string("client");
  hrd_qp_attr_t* conn_qp = nullptr;
  while (conn_qp == nullptr) {
    conn_qp = hrd_get_published_qp(conn_name.c_str());
    if (conn_qp == nullptr) {
      usleep(200000);
      continue;
    }

    printf("main: Server found client! Connecting..\n");
    hrd_connect_qp(cb, 0, conn_qp);
  }

  hrd_publish_ready("server");
  printf("main: Server ready. Going to sleep.\n");

  while (true) sleep(1);
}

void client_func() {
  hrd_conn_config_t conn_config;

  conn_config.num_qps = 1;
  conn_config.use_uc = false;
  conn_config.prealloc_buf = nullptr;
  conn_config.buf_size = FLAGS_max_write_size;
  conn_config.buf_shm_key = 3185;

  auto* cb = hrd_ctrl_blk_init(0 /* id */, 0 /* port */, 0 /* numa */,
                               &conn_config, nullptr /* dgram config */);
  memset(const_cast<uint8_t*>(cb->conn_buf), 31, FLAGS_max_write_size);

  hrd_publish_conn_qp(cb, 0, "client");
  printf("main: Client published. Waiting for server.\n");

  hrd_qp_attr_t* srv_qp = nullptr;
  while (srv_qp == nullptr) {
    srv_qp = hrd_get_published_qp("server");
    if (srv_qp == nullptr) usleep(2000);
  }

  printf("main: Found server. Connecting..\n");
  hrd_connect_qp(cb, 0, srv_qp);
  printf("main: Client connected!\n");

  hrd_wait_till_ready("server");

  struct timespec start;
  size_t total_bytes_written = 0;
  size_t pending_ops = 0;
  size_t remote_offset = 0;
  size_t cur_write_size = FLAGS_min_write_size;

  clock_gettime(CLOCK_REALTIME, &start);

  while (true) {
    if (pending_ops < FLAGS_window_size) {
      struct ibv_send_wr write_wr, read_wr, *bad_send_wr;
      struct ibv_sge write_sge, read_sge;

      // RDMA-write kClientWriteSize bytes
      write_sge.addr = reinterpret_cast<uint64_t>(&cb->conn_buf[0]);
      write_sge.length = cur_write_size;
      write_sge.lkey = cb->conn_buf_mr->lkey;

      write_wr.opcode = IBV_WR_RDMA_WRITE;
      write_wr.num_sge = 1;
      write_wr.sg_list = &write_sge;
      write_wr.send_flags = kReadAfterWrite ? 0 : IBV_SEND_SIGNALED;

      if (remote_offset + cur_write_size > kPmemFileSize) remote_offset = 0;
      write_wr.wr.rdma.remote_addr = srv_qp->buf_addr + remote_offset;
      write_wr.wr.rdma.rkey = srv_qp->rkey;
      write_wr.next = kReadAfterWrite ? &read_wr : nullptr;

      remote_offset += cur_write_size;

      if (kReadAfterWrite) {
        // RDMA-read 8 bytes from the end of the written buffer
        read_sge.addr = reinterpret_cast<uint64_t>(&cb->conn_buf[0]);
        read_sge.length = sizeof(size_t);
        read_sge.lkey = cb->conn_buf_mr->lkey;

        read_wr.opcode = IBV_WR_RDMA_READ;
        read_wr.num_sge = 1;
        read_wr.sg_list = &read_sge;
        read_wr.send_flags = IBV_SEND_SIGNALED;
        read_wr.wr.rdma.remote_addr =
            write_wr.wr.rdma.remote_addr + cur_write_size - sizeof(size_t);
        read_wr.wr.rdma.rkey = srv_qp->rkey;
        read_wr.next = nullptr;
      }

      int ret = ibv_post_send(cb->conn_qp[0], &write_wr, &bad_send_wr);
      rt_assert(ret == 0);
      pending_ops++;

      if (kVerbose) printf("Client posted. Pending = %zu\n", pending_ops);
    }

    if (pending_ops == FLAGS_window_size) {
      struct ibv_wc wc;
      hrd_poll_cq(cb->conn_cq[0], 1, &wc);
      pending_ops--;

      if (kVerbose) printf("Client polled. Pending = %zu\n", pending_ops);
      total_bytes_written += cur_write_size;
    }

    if (total_bytes_written >= GB(4)) {
      double secs = sec_since(start);

      printf("Client: size %zu, %.2f Gbps.\n", cur_write_size,
             total_bytes_written * 8 / (1000000000 * secs));

      total_bytes_written = 0;
      clock_gettime(CLOCK_REALTIME, &start);
    }
  }
}

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (FLAGS_is_client == 1) {
    auto client_thread = std::thread(client_func);
    client_thread.join();
  } else {
    auto t = std::thread(server_func);
    t.join();
  }
}
