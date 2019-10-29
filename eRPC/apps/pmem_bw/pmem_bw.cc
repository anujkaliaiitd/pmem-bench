/** @file pmem_bw.cc
 *
 * @brief Benchmark to measure throughput of large writes to remote NVM. Each
 * client thread creates one session to the server thread.
 */

#include "pmem_bw.h"
#include <libpmem.h>
#include <signal.h>
#include <cstring>
#include "util/autorun_helpers.h"
#include "util/pmem.h"

static constexpr size_t kAppEvLoopMs = 1000;  // Duration of event loop
static constexpr bool kAppVerbose = false;

// Experiment control flags
static constexpr bool kAppClientMemsetReq = false;   // Fill entire request
static constexpr bool kAppServerMemsetResp = false;  // Fill entire response
static constexpr bool kAppClientCheckResp = false;   // Check entire response

static constexpr const char *kAppPmemFile = "/dev/dax0.0";
static constexpr size_t kAppPmemFileSize = GB(32);

void app_cont_func(void *, void *);  // Forward declaration

// Send a request using this MsgBuffer
void send_req(ClientContext *c, size_t msgbuf_idx) {
  erpc::MsgBuffer &req_msgbuf = c->req_msgbuf[msgbuf_idx];
  assert(req_msgbuf.get_data_size() == FLAGS_req_size);

  if (kAppVerbose) {
    printf("large_rpc_tput: Thread %zu sending request using msgbuf_idx %zu.\n",
           c->thread_id, msgbuf_idx);
  }

  c->req_ts[msgbuf_idx] = erpc::rdtsc();
  c->rpc->enqueue_request(c->session_num_vec[0], kAppReqType, &req_msgbuf,
                          &c->resp_msgbuf[msgbuf_idx], app_cont_func,
                          reinterpret_cast<void *>(msgbuf_idx));

  c->stat_tx_bytes_tot += FLAGS_req_size;
}

void req_handler(erpc::ReqHandle *req_handle, void *_context) {
  auto *c = static_cast<ServerContext *>(_context);

  const erpc::MsgBuffer *req_msgbuf = req_handle->get_req_msgbuf();
  const size_t copy_size = req_msgbuf->get_data_size();
  if (c->file_offset + copy_size >= kAppPmemFileSize) c->file_offset = 0;
  pmem_memcpy_persist(&c->pbuf[c->file_offset], req_msgbuf->buf, copy_size);

  c->file_offset += copy_size;

  erpc::Rpc<erpc::CTransport>::resize_msg_buffer(&req_handle->pre_resp_msgbuf,
                                                 FLAGS_resp_size);
  c->rpc->enqueue_response(req_handle, &req_handle->pre_resp_msgbuf);
}

void app_cont_func(void *_context, void *_tag) {
  auto *c = static_cast<ClientContext *>(_context);
  auto msgbuf_idx = reinterpret_cast<size_t>(_tag);

  const erpc::MsgBuffer &resp_msgbuf = c->resp_msgbuf[msgbuf_idx];
  if (kAppVerbose) {
    printf("large_rpc_tput: Received response for msgbuf %zu.\n", msgbuf_idx);
  }

  // Measure latency. 1 us granularity is sufficient for large RPC latency.
  double usec = erpc::to_usec(erpc::rdtsc() - c->req_ts[msgbuf_idx],
                              c->rpc->get_freq_ghz());
  c->lat_vec.push_back(usec);

  // Check the response
  erpc::rt_assert(resp_msgbuf.get_data_size() == FLAGS_resp_size,
                  "Invalid response size");

  if (kAppClientCheckResp) {
    bool match = true;
    // Check all response cachelines (checking every byte is slow)
    for (size_t i = 0; i < FLAGS_resp_size; i += 64) {
      if (resp_msgbuf.buf[i] != kAppDataByte) match = false;
    }
    erpc::rt_assert(match, "Invalid resp data");
  } else {
    erpc::rt_assert(resp_msgbuf.buf[0] == kAppDataByte, "Invalid resp data");
  }

  c->stat_rx_bytes_tot += FLAGS_resp_size;

  // Create a new request clocking this response, and put in request queue
  if (kAppClientMemsetReq) {
    memset(c->req_msgbuf[msgbuf_idx].buf, kAppDataByte, FLAGS_req_size);
  } else {
    c->req_msgbuf[msgbuf_idx].buf[0] = kAppDataByte;
  }

  send_req(c, msgbuf_idx);
}

void client_connect_sessions(BasicAppContext *c) {
  // All non-zero processes create one session to process #0
  if (FLAGS_process_id == 0) return;

  size_t global_thread_id =
      FLAGS_process_id * FLAGS_num_proc_other_threads + c->thread_id;
  size_t rem_tid = global_thread_id % FLAGS_num_proc_0_threads;

  c->session_num_vec.resize(1);

  printf(
      "large_rpc_tput: Thread %zu: Creating 1 session to proc 0, thread %zu.\n",
      c->thread_id, rem_tid);

  c->session_num_vec[0] =
      c->rpc->create_session(erpc::get_uri_for_process(0), rem_tid);
  erpc::rt_assert(c->session_num_vec[0] >= 0, "create_session() failed");

  while (c->num_sm_resps != 1) {
    c->rpc->run_event_loop(200);  // 200 milliseconds
    if (ctrl_c_pressed == 1) return;
  }
}

// The function executed by each client thread in the cluster
void server_func(size_t thread_id, erpc::Nexus *nexus) {
  _unused(thread_id);
  std::vector<size_t> port_vec = flags_get_numa_ports(FLAGS_numa_node);
  uint8_t phy_port = port_vec.at(0);

  ServerContext c;
  erpc::Rpc<erpc::CTransport> rpc(nexus, static_cast<void *>(&c), 0 /* tid */,
                                  basic_sm_handler, phy_port);
  c.rpc = &rpc;

  printf("Mapping pmem file...");
  c.pbuf = erpc::map_devdax_file(kAppPmemFile, kAppPmemFileSize);
  printf("done.\n");

  while (true) {
    rpc.run_event_loop(1000);
    if (ctrl_c_pressed == 1) break;
  }
}

// The function executed by each client thread in the cluster
void client_func(size_t thread_id, app_stats_t *app_stats, erpc::Nexus *nexus) {
  ClientContext c;
  c.thread_id = thread_id;
  c.app_stats = app_stats;
  if (thread_id == 0) c.tmp_stat = new TmpStat(app_stats_t::get_template_str());

  std::vector<size_t> port_vec = flags_get_numa_ports(FLAGS_numa_node);
  erpc::rt_assert(port_vec.size() > 0);
  uint8_t phy_port = port_vec.at(thread_id % port_vec.size());

  erpc::Rpc<erpc::CTransport> rpc(nexus, static_cast<void *>(&c),
                                  static_cast<uint8_t>(thread_id),
                                  basic_sm_handler, phy_port);
  rpc.retry_connect_on_invalid_rpc_id = true;
  c.rpc = &rpc;

  client_connect_sessions(&c);

  if (c.session_num_vec.size() > 0) {
    printf("large_rpc_tput: Thread %zu: All sessions connected.\n", thread_id);
  } else {
    printf("large_rpc_tput: Thread %zu: No sessions created.\n", thread_id);
  }

  // All threads allocate MsgBuffers, but they may not send requests
  alloc_req_resp_msg_buffers(&c);

  clock_gettime(CLOCK_REALTIME, &c.tput_t0);

  // Any thread that creates a session sends requests
  if (c.session_num_vec.size() > 0) {
    for (size_t msgbuf_idx = 0; msgbuf_idx < FLAGS_concurrency; msgbuf_idx++) {
      send_req(&c, msgbuf_idx);
    }
  }

  clock_gettime(CLOCK_REALTIME, &c.tput_t0);
  for (size_t i = 0; i < FLAGS_test_ms; i += kAppEvLoopMs) {
    rpc.run_event_loop(kAppEvLoopMs);
    if (unlikely(ctrl_c_pressed == 1)) break;
    if (c.session_num_vec.size() == 0) continue;  // No stats to print

    double ns = erpc::ns_since(c.tput_t0);  // Don't rely on kAppEvLoopMs

    // Publish stats
    auto &stats = c.app_stats[c.thread_id];
    stats.rx_gbps = c.stat_rx_bytes_tot * 8 / ns;
    stats.tx_gbps = c.stat_tx_bytes_tot * 8 / ns;

    if (c.lat_vec.size() > 0) {
      std::sort(c.lat_vec.begin(), c.lat_vec.end());
      stats.rpc_50_us = c.lat_vec[c.lat_vec.size() * 0.50];
      stats.rpc_99_us = c.lat_vec[c.lat_vec.size() * 0.99];
    } else {
      // Even if no RPCs completed, we need retransmission counter
      stats.rpc_50_us = kAppEvLoopMs * 1000;
      stats.rpc_99_us = kAppEvLoopMs * 1000;
    }

    // Reset stats for next iteration
    c.stat_rx_bytes_tot = 0;
    c.stat_tx_bytes_tot = 0;
    c.rpc->reset_num_re_tx(c.session_num_vec[0]);
    c.lat_vec.clear();

    printf(
        "large_rpc_tput: Thread %zu: Tput {RX %.2f, TX %.2f} Gbps. "
        "RPC latency {%.1f, %.1f}. Credits %zu (best = 32).\n",
        c.thread_id, stats.rx_gbps, stats.tx_gbps, stats.rpc_50_us,
        stats.rpc_99_us, erpc::kSessionCredits);

    if (c.thread_id == 0) {
      app_stats_t accum_stats;
      for (size_t i = 0; i < FLAGS_num_proc_other_threads; i++) {
        accum_stats += c.app_stats[i];
      }

      // Compute averages for non-additive stats
      accum_stats.rpc_50_us /= FLAGS_num_proc_other_threads;
      accum_stats.rpc_99_us /= FLAGS_num_proc_other_threads;
      c.tmp_stat->write(accum_stats.to_string());
    }

    clock_gettime(CLOCK_REALTIME, &c.tput_t0);
  }

  // We don't disconnect sessions
}

int main(int argc, char **argv) {
  signal(SIGINT, ctrl_c_handler);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  erpc::rt_assert(FLAGS_concurrency <= kAppMaxConcurrency, "Invalid conc");
  erpc::rt_assert(FLAGS_process_id < FLAGS_num_processes, "Invalid process ID");

  erpc::Nexus nexus(erpc::get_uri_for_process(FLAGS_process_id),
                    FLAGS_numa_node, 0);
  nexus.register_req_func(kAppReqType, req_handler);

  size_t num_threads = FLAGS_process_id == 0 ? FLAGS_num_proc_0_threads
                                             : FLAGS_num_proc_other_threads;
  std::vector<std::thread> threads(num_threads);

  if (FLAGS_process_id == 0) {
    for (size_t i = 0; i < num_threads; i++) {
      threads[i] = std::thread(server_func, &nexus);
      erpc::bind_to_core(threads[i], FLAGS_numa_node, i);
    }
  }

  else {
    auto *app_stats = new app_stats_t[num_threads];  // Leaked

    for (size_t i = 0; i < num_threads; i++) {
      threads[i] = std::thread(client_func, i, app_stats, &nexus);
      erpc::bind_to_core(threads[i], FLAGS_numa_node, i);
    }
  }

  for (auto &thread : threads) thread.join();
}
