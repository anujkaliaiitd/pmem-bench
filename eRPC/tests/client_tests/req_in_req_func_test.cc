/**
 * @file test_nested_rpc.cc
 * @brief Test issuing requests from within request handlers. This uses a
 * primary-backup setup, where the client sends requests to the primary,
 * which completes an RPC with *one* of the backups before replying.
 */
#include "client_tests.h"

// Set to true if the request handler or continuation at the primary or backup
// should run in the background.
bool primary_bg, backup_bg;

static constexpr uint8_t kTestDataByte = 10;
static constexpr size_t kTestNumReqs = 33;
static_assert(kTestNumReqs > kSessionReqWindow, "");

/// Request type used for client to primary
static constexpr uint8_t kTestReqTypeCP = kTestReqType + 1;

/// Request type used for primary to backup
static constexpr uint8_t kTestReqTypePB = kTestReqType + 2;

/// Per-request info maintained at the primary
class PrimaryReqInfo {
 public:
  size_t req_size_cp;        ///< Size of client-to-primary request
  ReqHandle *req_handle_cp;  ///< Handle for client-to-primary request
  MsgBuffer req_msgbuf_pb;   ///< MsgBuffer for primary-to-backup request
  MsgBuffer resp_msgbuf_pb;  ///< MsgBuffer for primary-to-backup response
  size_t etid;               ///< eRPC thread ID in the request handler

  PrimaryReqInfo(size_t req_size_cp, ReqHandle *req_handle_cp, size_t etid)
      : req_size_cp(req_size_cp), req_handle_cp(req_handle_cp), etid(etid) {}
};

union client_tag_t {
  struct {
    uint16_t req_i;
    uint16_t msgbuf_i;
    uint32_t req_size;
  } s;
  void *_tag;

  client_tag_t(uint16_t req_i, uint16_t msgbuf_i, uint32_t req_size) {
    s.req_i = req_i;
    s.msgbuf_i = msgbuf_i;
    s.req_size = req_size;
  }

  client_tag_t(void *_tag) : _tag(_tag) {}
};
static_assert(sizeof(client_tag_t) == sizeof(void *), "");

/// Extended context for client
class AppContext : public BasicAppContext {
 public:
  FastRand fast_rand;
  size_t num_reqs_sent = 0;
};

///
/// Server-side code
///

// Forward declaration
void primary_cont_func(void *, void *);

/// The primary's request handler for client-to-primary requests. Forwards the
/// received request to one of the backup servers.
void req_handler_cp(ReqHandle *req_handle_cp, void *_c) {
  auto *c = static_cast<BasicAppContext *>(_c);
  assert(!c->is_client);
  ASSERT_EQ(c->rpc->in_background(), primary_bg);

  // This will be freed by eRPC when the request handler returns
  const MsgBuffer *req_msgbuf_cp = req_handle_cp->get_req_msgbuf();
  size_t req_size_cp = req_msgbuf_cp->get_data_size();

  test_printf("Primary [Rpc %u]: Received request of length %zu\n",
              c->rpc->get_rpc_id(), req_size_cp);

  // Record info for the request that we are now sending to the backup
  auto *srv_req_info =
      new PrimaryReqInfo(req_size_cp, req_handle_cp, c->rpc->get_etid());

  // Allocate request and response MsgBuffers for the request to the backup
  srv_req_info->req_msgbuf_pb = c->rpc->alloc_msg_buffer_or_die(req_size_cp);
  srv_req_info->resp_msgbuf_pb = c->rpc->alloc_msg_buffer_or_die(req_size_cp);

  // Request to backup = client-to-server request + 1
  for (size_t i = 0; i < req_size_cp; i++) {
    srv_req_info->req_msgbuf_pb.buf[i] = req_msgbuf_cp->buf[i] + 1;
  }

  // Backup is server thread #1
  c->rpc->enqueue_request(c->session_num_arr[1], kTestReqTypePB,
                          &srv_req_info->req_msgbuf_pb,
                          &srv_req_info->resp_msgbuf_pb, primary_cont_func,
                          reinterpret_cast<void *>(srv_req_info));
}

/// The backups' request handler for primary-to-backup to requests. Echoes the
/// received request back to the primary.
void req_handler_pb(ReqHandle *req_handle, void *_c) {
  auto *c = static_cast<BasicAppContext *>(_c);
  assert(!c->is_client);
  ASSERT_EQ(c->rpc->in_background(), backup_bg);

  const MsgBuffer *req_msgbuf_pb = req_handle->get_req_msgbuf();
  size_t req_size = req_msgbuf_pb->get_data_size();

  test_printf("Backup [Rpc %u]: Received request of length %zu.\n",
              c->rpc->get_rpc_id(), req_size);

  // eRPC will free dyn_resp_msgbuf
  req_handle->dyn_resp_msgbuf = c->rpc->alloc_msg_buffer_or_die(req_size);

  // Response to primary = request + 1
  for (size_t i = 0; i < req_size; i++) {
    req_handle->dyn_resp_msgbuf.buf[i] = req_msgbuf_pb->buf[i] + 1;
  }

  c->rpc->enqueue_response(req_handle, &req_handle->dyn_resp_msgbuf);
}

/// The primary's continuation function when it gets a response from a backup
void primary_cont_func(void *_c, void *_tag) {
  auto *c = static_cast<BasicAppContext *>(_c);
  assert(!c->is_client);
  ASSERT_EQ(c->rpc->in_background(), primary_bg);

  auto *srv_req_info = reinterpret_cast<PrimaryReqInfo *>(_tag);

  const MsgBuffer &resp_msgbuf_pb = srv_req_info->resp_msgbuf_pb;
  test_printf("Primary [Rpc %u]: Received response of length %zu\n",
              c->rpc->get_rpc_id(), resp_msgbuf_pb.get_data_size());

  // Check that we're still running in the same thread as for the
  // client-to-primary request
  assert(srv_req_info->etid == c->rpc->get_etid());

  // Extract the request info
  size_t req_size_cp = srv_req_info->req_size_cp;
  ReqHandle *req_handle_cp = srv_req_info->req_handle_cp;
  assert(resp_msgbuf_pb.get_data_size() == req_size_cp);

  // Check the response from server #1
  for (size_t i = 0; i < req_size_cp; i++) {
    assert(srv_req_info->req_msgbuf_pb.buf[i] + 1 == resp_msgbuf_pb.buf[i]);
  }

  // eRPC will free dyn_resp_msgbuf
  req_handle_cp->dyn_resp_msgbuf = c->rpc->alloc_msg_buffer_or_die(req_size_cp);

  // Response to client = server-to-server response + 1
  for (size_t i = 0; i < req_size_cp; i++) {
    req_handle_cp->dyn_resp_msgbuf.buf[i] = resp_msgbuf_pb.buf[i] + 1;
  }

  // Free resources of the server-to-server request
  c->rpc->free_msg_buffer(srv_req_info->req_msgbuf_pb);
  c->rpc->free_msg_buffer(srv_req_info->resp_msgbuf_pb);
  delete srv_req_info;

  // Send response to the client
  c->rpc->enqueue_response(req_handle_cp, &req_handle_cp->dyn_resp_msgbuf);
}

///
/// Client-side code
///
void client_cont_func(void *, void *);  // Forward declaration

/// Enqueue a request to server 0 using the request MsgBuffer index msgbuf_i
void client_request_helper(AppContext *c, size_t msgbuf_i) {
  assert(msgbuf_i < kSessionReqWindow);

  size_t req_size = get_rand_msg_size(&c->fast_rand, c->rpc);
  c->rpc->resize_msg_buffer(&c->req_msgbufs[msgbuf_i], req_size);

  // Fill in all the bytes of the request MsgBuffer with msgbuf_i
  for (size_t i = 0; i < req_size; i++) {
    c->req_msgbufs[msgbuf_i].buf[i] = kTestDataByte;
  }

  client_tag_t tag(static_cast<uint16_t>(c->num_reqs_sent),
                   static_cast<uint16_t>(msgbuf_i),
                   static_cast<uint32_t>(req_size));
  test_printf("Client [Rpc %u]: Sending request %zu of size %zu\n",
              c->rpc->get_rpc_id(), c->num_reqs_sent, req_size);

  c->rpc->enqueue_request(c->session_num_arr[0], kTestReqTypeCP,
                          &c->req_msgbufs[msgbuf_i], &c->resp_msgbufs[msgbuf_i],
                          client_cont_func, tag._tag);

  c->num_reqs_sent++;
}

void client_cont_func(void *_c, void *_tag) {
  // Extract info from tag
  auto tag = static_cast<client_tag_t>(_tag);
  size_t req_size = tag.s.req_size;
  size_t msgbuf_i = tag.s.msgbuf_i;

  auto *c = static_cast<AppContext *>(_c);
  assert(c->is_client);

  const MsgBuffer &resp_msgbuf = c->resp_msgbufs[msgbuf_i];

  test_printf("Client [Rpc %u]: Received response for req %u, length = %zu.\n",
              c->rpc->get_rpc_id(), tag.s.req_i, resp_msgbuf.get_data_size());

  // Check the response
  ASSERT_EQ(resp_msgbuf.get_data_size(), req_size);
  for (size_t i = 0; i < req_size; i++) {
    ASSERT_EQ(resp_msgbuf.buf[i], kTestDataByte + 3);
  }

  c->num_rpc_resps++;

  if (c->num_reqs_sent < kTestNumReqs) {
    client_request_helper(c, msgbuf_i);
  }
}

void client_thread(Nexus *nexus, size_t num_sessions) {
  // Create the Rpc and connect the sessions
  AppContext c;
  client_connect_sessions(nexus, c, num_sessions, basic_sm_handler);

  Rpc<CTransport> *rpc = c.rpc;

  // Start by filling the request window
  c.req_msgbufs.resize(erpc::kSessionReqWindow);
  c.resp_msgbufs.resize(erpc::kSessionReqWindow);
  for (size_t i = 0; i < erpc::kSessionReqWindow; i++) {
    const size_t sz = rpc->get_max_msg_size();
    c.req_msgbufs[i] = rpc->alloc_msg_buffer_or_die(sz);
    c.resp_msgbufs[i] = rpc->alloc_msg_buffer_or_die(sz);

    client_request_helper(&c, i);
  }

  wait_for_rpc_resps_or_timeout(c, kTestNumReqs);
  assert(c.num_rpc_resps == kTestNumReqs);

  for (auto &mb : c.req_msgbufs) rpc->free_msg_buffer(mb);
  for (auto &mb : c.resp_msgbufs) rpc->free_msg_buffer(mb);

  // Disconnect the sessions
  c.num_sm_resps = 0;
  for (size_t i = 0; i < num_sessions; i++) {
    rpc->destroy_session(c.session_num_arr[i]);
  }
  wait_for_sm_resps_or_timeout(c, num_sessions);
  assert(rpc->num_active_sessions() == 0);

  // Free resources
  delete rpc;
  client_done = true;
}

/// 1 primary, 1 backup, both in foreground
TEST(Base, BothInForeground) {
  primary_bg = false;
  backup_bg = false;

  auto reg_info_vec = {
      ReqFuncRegInfo(kTestReqTypeCP, req_handler_cp, ReqFuncType::kForeground),
      ReqFuncRegInfo(kTestReqTypePB, req_handler_pb, ReqFuncType::kForeground)};

  // 2 client sessions (=> 2 server threads), 0 background threads
  launch_server_client_threads(2, 0, client_thread, reg_info_vec,
                               ConnectServers::kTrue, 0.0);
}

/// 1 primary, 1 backup, primary in background
TEST(Base, PrimaryInBackground) {
  primary_bg = true;
  backup_bg = false;

  auto reg_info_vec = {
      ReqFuncRegInfo(kTestReqTypeCP, req_handler_cp, ReqFuncType::kBackground),
      ReqFuncRegInfo(kTestReqTypePB, req_handler_pb, ReqFuncType::kForeground)};

  // 2 client sessions (=> 2 server threads), 1 background threads
  launch_server_client_threads(2, 8, client_thread, reg_info_vec,
                               ConnectServers::kTrue, 0.0);
}

/// 1 primary, 1 backup, both in background
TEST(Base, BothInBackground) {
  primary_bg = true;
  backup_bg = true;

  auto reg_info_vec = {
      ReqFuncRegInfo(kTestReqTypeCP, req_handler_cp, ReqFuncType::kBackground),
      ReqFuncRegInfo(kTestReqTypePB, req_handler_pb, ReqFuncType::kBackground)};

  // 2 client sessions (=> 2 server threads), 3 background threads
  launch_server_client_threads(2, 3, client_thread, reg_info_vec,
                               ConnectServers::kTrue, 0.0);
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
