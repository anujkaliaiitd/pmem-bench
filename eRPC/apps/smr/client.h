/**
 * @file client.h
 * @brief Client code and RPC handlers for client-issued RPCs
 */

#pragma once
#include "smr.h"

// Change the leader to a different Raft server that we are connected to
void change_leader_to_any(AppContext *c) {
  size_t cur_leader_idx = c->client.leader_idx;
  printf("smr: Client change_leader_to_any() from current leader %zu.\n",
         c->client.leader_idx);

  // Pick the next session to a Raft server that is not disconnected
  for (size_t i = 1; i < FLAGS_num_raft_servers; i++) {
    size_t next_leader_idx = (cur_leader_idx + i) % FLAGS_num_raft_servers;
    if (!c->conn_vec[next_leader_idx].disconnected) {
      c->client.leader_idx = next_leader_idx;

      printf("smr: Client changed leader view to %zu.\n", c->client.leader_idx);
      return;
    }
  }

  printf("smr: Client failed to change leader to any Raft server. Exiting.\n");
  exit(0);
}

// Change the leader to a server with the given Raft node ID
bool change_leader_to_node(AppContext *c, int raft_node_id) {
  // Pick the next session to a Raft server that is not disconnected
  for (size_t i = 0; i < FLAGS_num_raft_servers; i++) {
    if (raft_node_id == get_raft_node_id_for_process(i)) {
      // Ignore if we're being redirected to a failed Raft server
      if (c->conn_vec[i].disconnected) return false;

      c->client.leader_idx = i;
      return true;
    }
  }

  printf("smr: Client could not find Raft node %d. Exiting.\n", raft_node_id);
  exit(0);
}

void client_cont(void *, void *);  // Forward declaration

void send_req_one(AppContext *c) {
  c->client.req_start_tsc = erpc::rdtsc();

  // Format the client's PUT request. Key and value are identical.
  auto *req = reinterpret_cast<client_req_t *>(c->client.req_msgbuf.buf);
  size_t rand_key = c->fast_rand.next_u32() & (kAppNumKeys - 1);
  req->key[0] = rand_key;
  req->value[0] = rand_key;

  if (kAppVerbose) {
    printf("smr: Client sending request %s to leader index %zu [%s].\n",
           req->to_string().c_str(), c->client.leader_idx,
           erpc::get_formatted_time().c_str());
  }

  connection_t &conn = c->conn_vec[c->client.leader_idx];
  c->rpc->enqueue_request(
      conn.session_num, static_cast<uint8_t>(ReqType::kClientReq),
      &c->client.req_msgbuf, &c->client.resp_msgbuf, client_cont, nullptr);
}

void client_cont(void *_context, void *) {
  auto *c = static_cast<AppContext *>(_context);
  double latency_us = erpc::to_usec(erpc::rdtsc() - c->client.req_start_tsc,
                                    c->rpc->get_freq_ghz());
  c->client.req_us_vec.push_back(latency_us);
  c->client.num_resps++;

  if (c->client.num_resps == 100000) {
    // At this point, there is no request outstanding, so long compute is OK
    auto &lat_vec = c->client.req_us_vec;
    std::sort(lat_vec.begin(), lat_vec.end());

    double us_min = lat_vec.at(0);
    double us_median = lat_vec.at(lat_vec.size() / 2);
    double us_99 = lat_vec.at(lat_vec.size() * .99);
    double us_999 = lat_vec.at(lat_vec.size() * .999);
    double us_max = lat_vec.at(lat_vec.size() - 1);

    printf(
        "smr: Latency us = "
        "{%.2f min, %.2f 50, %.2f 99, %.2f 99.9, %.2f max}. "
        "Request window = %zu (best 1). Inline size = %zu (best 120).\n",
        us_min, us_median, us_99, us_999, us_max, erpc::kSessionReqWindow,
        erpc::CTransport::kMaxInline);
    c->client.num_resps = 0;
    c->client.req_us_vec.clear();
  }

  if (likely(c->client.resp_msgbuf.get_data_size() > 0)) {
    // The RPC was successful
    auto *client_resp =
        reinterpret_cast<client_resp_t *>(c->client.resp_msgbuf.buf);

    if (kAppVerbose) {
      printf("smr: Client received resp %s [%s].\n",
             client_resp->to_string().c_str(),
             erpc::get_formatted_time().c_str());
    }

    switch (client_resp->resp_type) {
      case ClientRespType::kSuccess: {
        break;
      }

      case ClientRespType::kFailRedirect: {
        printf(
            "smr: Client request to server %zu failed with code = "
            "redirect. Trying to change leader to %s.\n",
            c->client.leader_idx,
            node_id_to_name_map.at(client_resp->leader_node_id).c_str());

        bool success = change_leader_to_node(c, client_resp->leader_node_id);
        if (!success) {
          printf(
              "smr: Client failed to change leader to %s. "
              "Retrying to current leader %zu after 200 ms.\n",
              node_id_to_name_map.at(client_resp->leader_node_id).c_str(),
              c->client.leader_idx);
        }

        usleep(200000);
        break;
      }

      case ClientRespType::kFailTryAgain: {
        printf(
            "smr: Client request to server %zu failed with code = "
            "try again. Trying again after 200 ms.\n",
            c->client.leader_idx);
        usleep(200000);
        break;
      }
    }
  } else {
    // This is a continuation-with-failure
    printf("smr: Client RPC to server %zu failed to complete [%s].\n",
           c->client.leader_idx, erpc::get_formatted_time().c_str());
    change_leader_to_any(c);
  }

  send_req_one(c);
}

void client_func(erpc::Nexus *nexus, AppContext *c) {
  c->client.leader_idx = 0;  // Start with leader = 0

  c->rpc = new erpc::Rpc<erpc::CTransport>(
      nexus, static_cast<void *>(c), kAppClientRpcId, sm_handler, kAppPhyPort);
  c->rpc->retry_connect_on_invalid_rpc_id = true;

  // Pre-allocate MsgBuffers
  c->client.req_msgbuf = c->rpc->alloc_msg_buffer_or_die(sizeof(client_req_t));
  c->client.resp_msgbuf =
      c->rpc->alloc_msg_buffer_or_die(sizeof(client_resp_t));

  // Raft client: Create session to each Raft server
  for (size_t i = 0; i < FLAGS_num_raft_servers; i++) {
    std::string uri = erpc::get_uri_for_process(i);
    printf("smr: Creating session to %s, index = %zu.\n", uri.c_str(), i);

    c->conn_vec[i].session_idx = i;
    c->conn_vec[i].session_num = c->rpc->create_session(uri, kAppServerRpcId);
    assert(c->conn_vec[i].session_num >= 0);
  }

  while (c->num_sm_resps != FLAGS_num_raft_servers) {
    c->rpc->run_event_loop(200);  // 200 ms
    if (ctrl_c_pressed == 1) {
      delete c->rpc;
      exit(0);
    }
  }

  printf("smr: Client connected to all. Sending reqs.\n");

  send_req_one(c);
  while (ctrl_c_pressed == 0) c->rpc->run_event_loop(200);

  delete c->rpc;
}
