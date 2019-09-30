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
DEFINE_uint64(num_client_processes, 1, "Number of client processes");
DEFINE_uint64(num_threads_per_client, 1, "Threads per client process");
DEFINE_uint64(num_qps_per_client_thread, 1, "QPs per client thread");

// Size of the buffer registered at the server
static constexpr size_t kServerBufSize = GB(4);

// Size of RDMA writes issued by each client
static constexpr size_t kClientWriteSize = MB(64);

// If true, we use a devdax-mapped buffer. If false, we use DRAM hugepages.
static constexpr bool kUsePmem = true;
static constexpr const char* kPmemFile = "/dev/dax0.0";

// If true, we use read-after-write to force persistence
static constexpr bool kReadAfterWrite = false;

// Map the devdax buffer at the server
uint8_t* get_pmem_buf_server() {
  int fd = open(kPmemFile, O_RDWR);
  rt_assert(fd >= 0, "devdax open failed");

  size_t pmem_size = round_up<MB(2)>(kServerBufSize);  // Smaller sizes may fail
  void* buf =
      mmap(nullptr, pmem_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  rt_assert(buf != MAP_FAILED, "mmap failed for devdax");
  rt_assert(reinterpret_cast<size_t>(buf) % 256 == 0);

  return reinterpret_cast<uint8_t*>(buf);
}

void server_func() {
  size_t num_client_connections = FLAGS_num_client_processes *
                                  FLAGS_num_threads_per_client *
                                  FLAGS_num_qps_per_client_thread;

  rt_assert(kServerBufSize >= kClientWriteSize * num_client_connections,
            "Server buffer too small to accommodate all client connections");
  uint8_t* pmem_buf = nullptr;
  if (kUsePmem) pmem_buf = get_pmem_buf_server();

  struct hrd_conn_config_t conn_config;
  conn_config.num_qps = num_client_connections;
  conn_config.use_uc = false;
  conn_config.prealloc_buf = kUsePmem ? pmem_buf : nullptr;
  conn_config.buf_size = kServerBufSize;
  conn_config.buf_shm_key = kUsePmem ? -1 : 3185;

  auto* cb = hrd_ctrl_blk_init(0 /* id */, 0 /* port */, 0 /* numa */,
                               &conn_config, nullptr /* dgram config */);

  // Fill in the persistent buffer, also sanity-check local write throughput
  printf("main: Zero-ing pmem buffer\n");
  struct timespec start;
  clock_gettime(CLOCK_REALTIME, &start);
  pmem_memset_persist(const_cast<uint8_t*>(cb->conn_buf), 0, kServerBufSize);
  printf("main: Zero-ed %f MB of pmem at %.1f GB/s\n",
         kServerBufSize * 1.0 / MB(1),
         kServerBufSize / (1000000000.0 * sec_since(start)));

  // Publish server QPs
  for (size_t i = 0; i < num_client_connections; i++) {
    auto srv_qp_name = std::string("server-") + std::to_string(i);
    hrd_publish_conn_qp(cb, i, srv_qp_name.c_str());
  }

  printf("main: Server published. Waiting for %zu client connections.\n",
         num_client_connections);

  for (size_t i = 0; i < num_client_connections; i++) {
    auto clt_qp_name = std::string("client-") + std::to_string(i);
    hrd_qp_attr_t* clt_qp = nullptr;
    while (clt_qp == nullptr) {
      clt_qp = hrd_get_published_qp(clt_qp_name.c_str());
      if (clt_qp == nullptr) {
        usleep(200000);
        continue;
      }

      printf("main: Server found client connection %zu! Connecting..\n", i);
      hrd_connect_qp(cb, i, clt_qp);
    }
  }

  hrd_publish_ready("server");
  printf("main: Server ready. Going to sleep.\n");

  while (true) sleep(1);
}

void client_func(size_t global_thread_id) {
  hrd_conn_config_t conn_config;
  conn_config.num_qps = 1;
  conn_config.use_uc = false;
  conn_config.prealloc_buf = nullptr;
  conn_config.buf_size = kClientWriteSize;
  conn_config.buf_shm_key = 3185 + global_thread_id;

  auto* cb = hrd_ctrl_blk_init(0 /* id */, 0 /* port */, 0 /* numa */,
                               &conn_config, nullptr /* dgram config */);
  memset(const_cast<uint8_t*>(cb->conn_buf), 31, kClientWriteSize);

  auto clt_qp_name = std::string("client-") + std::to_string(global_thread_id);
  hrd_publish_conn_qp(cb, 0, clt_qp_name.c_str());
  printf("main: Client %zu published. Waiting for server.\n", global_thread_id);

  auto srv_qp_name = std::string("server-") + std::to_string(global_thread_id);
  hrd_qp_attr_t* srv_qp = nullptr;
  while (srv_qp == nullptr) {
    srv_qp = hrd_get_published_qp(srv_qp_name.c_str());
    if (srv_qp == nullptr) usleep(2000);
  }

  printf("main: Client %zu found server. Connecting..\n", global_thread_id);
  hrd_connect_qp(cb, 0, srv_qp);
  printf("main: Client connected!\n");

  hrd_wait_till_ready("server");

  struct ibv_send_wr write_wr, read_wr, *bad_send_wr;
  struct ibv_sge write_sge, read_sge;
  struct ibv_wc wc;
  size_t total_bytes_written = 0;
  struct timespec start;
  clock_gettime(CLOCK_REALTIME, &start);

  while (true) {
    // RDMA-write kClientWriteSize bytes
    write_sge.addr = reinterpret_cast<uint64_t>(&cb->conn_buf[0]);
    write_sge.length = kClientWriteSize;
    write_sge.lkey = cb->conn_buf_mr->lkey;

    write_wr.opcode = IBV_WR_RDMA_WRITE;
    write_wr.num_sge = 1;
    write_wr.sg_list = &write_sge;
    write_wr.send_flags = kReadAfterWrite ? 0 : IBV_SEND_SIGNALED;

    write_wr.wr.rdma.remote_addr =
        srv_qp->buf_addr + global_thread_id * kClientWriteSize;
    write_wr.wr.rdma.rkey = srv_qp->rkey;
    write_wr.next = kReadAfterWrite ? &read_wr : nullptr;

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
          write_wr.wr.rdma.remote_addr + kClientWriteSize - sizeof(size_t);
      read_wr.wr.rdma.rkey = srv_qp->rkey;
      read_wr.next = nullptr;
    }

    int ret = ibv_post_send(cb->conn_qp[0], &write_wr, &bad_send_wr);
    rt_assert(ret == 0);
    hrd_poll_cq(cb->conn_cq[0], 1, &wc);  // Block till the read completes
    total_bytes_written += kClientWriteSize;

    double sec_elapsed = sec_since(start);
    if (sec_elapsed >= 1.0) {
      printf("Thread %zu: %.2f MB per write, %.2f GB/s\n", global_thread_id,
             kClientWriteSize * 1.0 / MB(1),
             total_bytes_written * 1.0 / (GB(1) * sec_elapsed));

      total_bytes_written = 0;
      clock_gettime(CLOCK_REALTIME, &start);
    }
  }
}

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (FLAGS_is_client == 1) {
    std::vector<std::thread> client_threads(FLAGS_num_threads_per_client);

    for (size_t i = 0; i < client_threads.size(); i++) {
      size_t global_thread_id =
          (FLAGS_machine_id * FLAGS_num_threads_per_client) + i;
      client_threads[i] = std::thread(client_func, global_thread_id);
    }

    for (size_t i = 0; i < client_threads.size(); i++) client_threads[i].join();
  } else {
    auto t = std::thread(server_func);
    t.join();
  }
}
