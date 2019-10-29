#pragma once

#include <gflags/gflags.h>
#include <signal.h>
#include "../apps_common.h"
#include "rpc.h"
#include "util/numautils.h"

static constexpr size_t kAppReqType = 1;
static constexpr uint8_t kAppDataByte = 3;  // Data transferred in req & resp
static constexpr size_t kAppMaxConcurrency = 32;  // Outstanding reqs per thread

// Globals
volatile sig_atomic_t ctrl_c_pressed = 0;
void ctrl_c_handler(int) { ctrl_c_pressed = 1; }

// Flags
DEFINE_uint64(num_proc_0_threads, 0, "Threads in process 0");
DEFINE_uint64(num_proc_other_threads, 0, "Threads in process with ID != 0");
DEFINE_uint64(req_size, 0, "Request data size");
DEFINE_uint64(resp_size, 0, "Response data size");
DEFINE_uint64(concurrency, 0, "Concurrent requests per thread");

struct app_stats_t {
  double rx_gbps;
  double tx_gbps;
  size_t pad[6];

  app_stats_t() { memset(this, 0, sizeof(app_stats_t)); }

  static std::string get_template_str() { return "rx_gbps tx_gbps"; }

  /// Return a space-separated string of all stats
  std::string to_string() {
    return std::to_string(rx_gbps) + " " + std::to_string(tx_gbps);
  }

  /// Accumulate stats
  app_stats_t& operator+=(const app_stats_t& rhs) {
    this->rx_gbps += rhs.rx_gbps;
    this->tx_gbps += rhs.tx_gbps;
    return *this;
  }
};
static_assert(sizeof(app_stats_t) == 64, "");

class ServerContext : public BasicAppContext {
 public:
  size_t thread_id = 0;
  size_t pmem_write_bytes = 0;
  double pmem_write_cycles = 0;

  size_t cur_offset = 0;
  size_t offset_lo = 0;
  size_t offset_hi = 0;

  uint8_t* pbuf;
};

// Per-thread application context
class ClientContext : public BasicAppContext {
 public:
  struct timespec tput_t0;  // Start time for throughput measurement
  app_stats_t* app_stats;   // Common stats array for all threads

  size_t stat_rx_bytes_tot = 0;  // Total bytes received
  size_t stat_tx_bytes_tot = 0;  // Total bytes transmitted

  erpc::MsgBuffer req_msgbuf[kAppMaxConcurrency];
  erpc::MsgBuffer resp_msgbuf[kAppMaxConcurrency];
};

// Allocate request and response MsgBuffers
void alloc_req_resp_msg_buffers(ClientContext* c) {
  for (size_t i = 0; i < FLAGS_concurrency; i++) {
    c->req_msgbuf[i] = c->rpc->alloc_msg_buffer_or_die(FLAGS_req_size);
    c->resp_msgbuf[i] = c->rpc->alloc_msg_buffer_or_die(FLAGS_resp_size);

    // Fill the request regardless of kAppMemset. This is a one-time thing.
    memset(c->req_msgbuf[i].buf, i, FLAGS_req_size);
  }
}
