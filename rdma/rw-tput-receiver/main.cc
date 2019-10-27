#include <fcntl.h>
#include <gflags/gflags.h>
#include <libpmem.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <vector>
#include "../libhrd_cpp/hrd.h"

static constexpr size_t kServerBufSize = GB(8);
static constexpr size_t kAppMaxPostlist = 64;
static constexpr size_t kAppUnsigBatch = 64;
static constexpr size_t kBaseSHMKey = 3185;

// If true, we use a devdax-mapped buffer. If false, we use DRAM hugepages.
static constexpr bool kUsePmem = true;
static constexpr const char* kPmemFile = "/dev/dax0.0";

// If true, server zeroes out its buffer and reports write throughput
static constexpr bool kZeroServerBuf = false;

DEFINE_uint64(num_client_processes, 1, "Number of client processes");
DEFINE_uint64(num_threads_per_client, 1, "Threads per client process");
DEFINE_uint64(is_client, 0, "Is this process a client?");
DEFINE_uint64(use_uc, 0, "Use unreliable connected transport?");
DEFINE_uint64(do_read, 0, "Do RDMA reads?");
DEFINE_uint64(machine_id, 0, "Zero-based ID of this client machine");
DEFINE_uint64(size, 0, "RDMA size");
DEFINE_uint64(postlist, 0, "Postlist size");

// Parameters for a client thread
struct clt_thread_params_t {
  size_t global_thread_id;
  double* tput;
};

// Map the devdax buffer at the server
uint8_t* get_pmem_buf_server() {
  int fd = open(kPmemFile, O_RDWR);
  rt_assert(fd >= 0, "devdax open failed");

  size_t pmem_size = roundup<MB(2)>(kServerBufSize);  // Smaller sizes may fail
  void* buf =
      mmap(nullptr, pmem_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  rt_assert(buf != MAP_FAILED, "mmap failed for devdax");
  rt_assert(reinterpret_cast<size_t>(buf) % 256 == 0);

  return reinterpret_cast<uint8_t*>(buf);
}

void run_server() {
  size_t num_client_connections =
      FLAGS_num_client_processes * FLAGS_num_threads_per_client;

  uint8_t* pmem_buf = nullptr;
  if (kUsePmem) {
    pmem_buf = get_pmem_buf_server();

    // Fill in the persistent buffer, also sanity-check local write throughput
    if (kZeroServerBuf) {
      printf("main: Zero-ing pmem buffer\n");
      struct timespec start;
      clock_gettime(CLOCK_REALTIME, &start);
      pmem_memset_persist(pmem_buf, 0, kServerBufSize);
      printf("main: Zero-ed %f MB of pmem at %.1f GB/s\n",
             kServerBufSize * 1.0 / MB(1),
             kServerBufSize / (1000000000.0 * sec_since(start)));
    }
  }

  struct hrd_conn_config_t conn_config;
  conn_config.num_qps = num_client_connections;
  conn_config.use_uc = (FLAGS_use_uc == 1);
  conn_config.prealloc_buf = kUsePmem ? pmem_buf : nullptr;
  conn_config.buf_size = kServerBufSize;
  conn_config.buf_shm_key = kUsePmem ? -1 : 3185;

  auto* cb = hrd_ctrl_blk_init(0 /* id */, 0 /* port */, 0 /* numa */,
                               &conn_config, nullptr /* dgram config */);

  // Publish server QPs. Server i is for global connection ID i
  for (size_t i = 0; i < num_client_connections; i++) {
    auto srv_qp_name = std::string("server-") + std::to_string(i);
    hrd_publish_conn_qp(cb, i, srv_qp_name.c_str());
  }

  for (size_t i = 0; i < num_client_connections; i++) {
    auto conn_name = std::string("conn-") + std::to_string(i);
    hrd_qp_attr_t* conn_qp = nullptr;
    while (conn_qp == nullptr) {
      conn_qp = hrd_get_published_qp(conn_name.c_str());
      if (conn_qp == nullptr) {
        usleep(200000);
        continue;
      }

      printf("main: Server found client connection %zu! Connecting..\n", i);
      hrd_connect_qp(cb, i, conn_qp);
    }
  }

  hrd_publish_ready("server");
  printf("main: Server ready. Going to sleep.\n");

  while (true) sleep(1);
}

void run_client(clt_thread_params_t* params) {
  FastRand fast_rand;
  size_t clt_lid = params->global_thread_id % FLAGS_num_threads_per_client;

  hrd_conn_config_t conn_config;
  conn_config.num_qps = 1;
  conn_config.use_uc = (FLAGS_use_uc == 1);
  conn_config.prealloc_buf = nullptr;
  conn_config.buf_size = FLAGS_size;
  conn_config.buf_shm_key = kBaseSHMKey + clt_lid;

  auto* cb = hrd_ctrl_blk_init(params->global_thread_id, 0 /* port */,
                               0 /* numa */, &conn_config, nullptr);

  memset(const_cast<uint8_t*>(cb->conn_buf),
         static_cast<uint8_t>(params->global_thread_id) + 1,
         conn_config.buf_size);

  size_t global_conn_id = params->global_thread_id;
  auto conn_name = std::string("conn-") + std::to_string(global_conn_id);
  hrd_publish_conn_qp(cb, 0, conn_name.c_str());
  printf("main: Connection %s published. Waiting for server.\n",
         conn_name.c_str());

  auto srv_qp_name = std::string("server-") + std::to_string(global_conn_id);
  hrd_qp_attr_t* srv_qp = nullptr;
  while (srv_qp == nullptr) {
    srv_qp = hrd_get_published_qp(srv_qp_name.c_str());
    if (srv_qp == nullptr) usleep(2000);
  }

  rt_assert(srv_qp->buf_addr % FLAGS_size == 0,
            "Server buffer address not aligned to RDMA size");

  printf("main: Found server for connection %s. Connecting..\n",
         conn_name.c_str());
  hrd_connect_qp(cb, 0, srv_qp);
  printf("main: Client connected!\n");

  hrd_wait_till_ready("server");

  struct ibv_send_wr wr[kAppMaxPostlist], *bad_send_wr;
  struct ibv_sge sgl[kAppMaxPostlist];
  struct ibv_wc wc;
  size_t rolling_iter = 0;  // For performance measurement
  size_t nb_tx = 0;         // For selective signaling
  int ret;

  struct timespec start, end;
  clock_gettime(CLOCK_REALTIME, &start);

  while (true) {
    if (rolling_iter >= KB(512)) {
      clock_gettime(CLOCK_REALTIME, &end);
      double seconds = (end.tv_sec - start.tv_sec) +
                       (end.tv_nsec - start.tv_nsec) / 1000000000.0;
      double tput_mrps = rolling_iter / (seconds * 1000000);
      printf("main: Client %zu: %.2f M/s\n", params->global_thread_id,
             tput_mrps);
      rolling_iter = 0;

      // Per-machine stats
      params->tput[clt_lid] = tput_mrps;
      if (clt_lid == 0) {
        double tot = 0;
        for (size_t i = 0; i < FLAGS_num_threads_per_client; i++)
          tot += params->tput[i];
        hrd_red_printf("main: Machine: %.2f M/s\n", tot);
      }

      clock_gettime(CLOCK_REALTIME, &start);
    }

    // Post a batch
    for (size_t w_i = 0; w_i < FLAGS_postlist; w_i++) {
      wr[w_i].opcode =
          FLAGS_do_read == 0 ? IBV_WR_RDMA_WRITE : IBV_WR_RDMA_READ;
      wr[w_i].num_sge = 1;
      wr[w_i].next = (w_i == FLAGS_postlist - 1) ? nullptr : &wr[w_i + 1];
      wr[w_i].sg_list = &sgl[w_i];

      wr[w_i].send_flags = nb_tx % kAppUnsigBatch == 0 ? IBV_SEND_SIGNALED : 0;
      if (nb_tx % kAppUnsigBatch == 0 && nb_tx > 0) {
        hrd_poll_cq(cb->conn_cq[0], 1, &wc);
      }

      wr[w_i].send_flags |= FLAGS_do_read == 0 ? IBV_SEND_INLINE : 0;

      sgl[w_i].addr = reinterpret_cast<uint64_t>(&cb->conn_buf);
      sgl[w_i].length = FLAGS_size;
      sgl[w_i].lkey = cb->conn_buf_mr->lkey;

      size_t remote_offset =
          (fast_rand.next_u32() % (kServerBufSize / FLAGS_size)) * FLAGS_size;

      wr[w_i].wr.rdma.remote_addr = srv_qp->buf_addr + remote_offset;
      wr[w_i].wr.rdma.rkey = srv_qp->rkey;

      nb_tx++;
    }

    ret = ibv_post_send(cb->conn_qp[0], &wr[0], &bad_send_wr);
    rt_assert(ret == 0);

    rolling_iter += FLAGS_postlist;
  }
}

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_is_client == 1) {
    if (FLAGS_do_read == 0) {
      rt_assert(FLAGS_size <= kHrdMaxInline, "Inline size too small");
    }
    rt_assert(FLAGS_postlist <= kAppMaxPostlist, "Postlist too large");
    rt_assert(kAppUnsigBatch >= FLAGS_postlist, "Postlist check failed");
    rt_assert(kHrdSQDepth >= 2 * kAppUnsigBatch, "Queue capacity check failed");
  }

  // Launch a single server thread or multiple client threads

  if (FLAGS_is_client == 1) {
    std::vector<std::thread> thread_arr(FLAGS_num_threads_per_client);
    auto* tput = new double[FLAGS_num_threads_per_client];
    printf("main: Using %zu threads\n", FLAGS_num_threads_per_client);
    auto* param_arr = new clt_thread_params_t[FLAGS_num_threads_per_client];
    for (size_t i = 0; i < FLAGS_num_threads_per_client; i++) {
      param_arr[i].global_thread_id =
          (FLAGS_machine_id * FLAGS_num_threads_per_client) + i;
      param_arr[i].tput = tput;

      thread_arr[i] = std::thread(run_client, &param_arr[i]);
    }

    for (auto& thread : thread_arr) thread.join();
  } else {
    auto server_thread = std::thread(run_server);
    server_thread.join();
  }
}
