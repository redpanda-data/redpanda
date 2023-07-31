// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/heartbeat_manager.h"

#include "config/configuration.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "outcome_future_utils.h"
#include "raft/consensus_client_protocol.h"
#include "raft/errc.h"
#include "raft/group_configuration.h"
#include "raft/raftgen_service.h"
#include "raft/types.h"
#include "rpc/errc.h"
#include "rpc/reconnect_transport.h"
#include "rpc/types.h"
#include "vlog.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/with_timeout.hh>

#include <absl/container/flat_hash_set.h>
#include <absl/container/node_hash_map.h>
#include <bits/stdint-uintn.h>
#include <boost/range/iterator_range.hpp>

namespace raft {
ss::logger hbeatlog{"r/heartbeat"};
using consensus_ptr = heartbeat_manager::consensus_ptr;
using consensus_set = heartbeat_manager::consensus_set;

heartbeat_manager::follower_request_meta::follower_request_meta(
  consensus_ptr ptr,
  follower_req_seq seq,
  model::offset dirty_offset,
  vnode target)
  : c(std::move(ptr))
  , seq(seq)
  , dirty_offset(dirty_offset)
  , follower_vnode(target) {
    c->update_suppress_heartbeats(
      follower_vnode, seq, heartbeats_suppressed::yes);
}

heartbeat_manager::follower_request_meta::~follower_request_meta() noexcept {
    if (c) {
        c->update_suppress_heartbeats(
          follower_vnode, seq, heartbeats_suppressed::no);
    }
}

heartbeat_manager::heartbeat_requests heartbeat_manager::requests_for_range() {
    absl::node_hash_map<
      model::node_id,
      ss::chunked_fifo<std::pair<
        heartbeat_metadata,
        heartbeat_manager::follower_request_meta>>>
      pending_beats;

    if (_consensus_groups.empty()) {
        return {};
    }

    // Set of follower nodes whose heartbeat_failed status indicates
    // that we should tear down their TCP connection before next heartbeat
    absl::flat_hash_set<model::node_id> reconnect_nodes;

    const auto last_heartbeat = clock_type::now() - _heartbeat_interval();
    for (auto& r : _consensus_groups) {
        if (!r->is_elected_leader()) {
            continue;
        }

        for (auto& [id, follower_metadata] : r->_fstats) {
            if (follower_metadata.suppress_heartbeats) {
                vlog(r->_ctxlog.trace, "[{}] heartbeat suppressed", id);
                continue;
            }
            if (
              follower_metadata.last_sent_append_entries_req_timestamp
              > last_heartbeat) {
                vlog(r->_ctxlog.trace, "[{}] heartbeat skipped", id);
                continue;
            }
            auto const seq_id = follower_metadata.last_sent_seq++;
            auto hb_meta = r->meta();
            pending_beats[id.id()].emplace_back(
              heartbeat_metadata{hb_meta, r->self(), id},
              heartbeat_manager::follower_request_meta(
                r, seq_id, hb_meta.prev_log_index, id));
            if (r->should_reconnect_follower(follower_metadata)) {
                reconnect_nodes.insert(id.id());
            }
        }
    }

    std::vector<heartbeat_manager::node_heartbeat> reqs;
    reqs.reserve(pending_beats.size());
    for (auto& p : pending_beats) {
        std::vector<heartbeat_metadata> requests;
        absl::node_hash_map<
          raft::group_id,
          heartbeat_manager::follower_request_meta>
          meta_map;
        requests.reserve(p.second.size());
        for (auto& [hb, follower_meta] : p.second) {
            meta_map.emplace(hb.meta.group, std::move(follower_meta));
            requests.push_back(std::move(hb));
        }
        reqs.emplace_back(
          p.first, heartbeat_request{std::move(requests)}, std::move(meta_map));
    }

    return heartbeat_requests{
      .requests{std::move(reqs)}, .reconnect_nodes{reconnect_nodes}};
}

heartbeat_manager::heartbeat_manager(
  config::binding<std::chrono::milliseconds> interval,
  consensus_client_protocol proto,
  model::node_id self,
  config::binding<std::chrono::milliseconds> heartbeat_timeout)
  : _heartbeat_interval(std::move(interval))
  , _heartbeat_timeout(std::move(heartbeat_timeout))
  , _client_protocol(std::move(proto))
  , _self(self) {
    _heartbeat_timer.set_callback([this] { dispatch_heartbeats(); });
}

ss::future<>
heartbeat_manager::send_heartbeats(std::vector<node_heartbeat> reqs) {
    return ss::do_with(
      std::move(reqs), [this](std::vector<node_heartbeat>& reqs) mutable {
          std::vector<ss::future<>> futures;
          futures.reserve(reqs.size());
          for (auto& r : reqs) {
              futures.push_back(do_heartbeat(std::move(r)));
          }
          return ss::when_all_succeed(futures.begin(), futures.end());
      });
}

ss::future<> heartbeat_manager::do_dispatch_heartbeats() {
    auto reqs = requests_for_range();

    for (const auto& node_id : reqs.reconnect_nodes) {
        if (co_await _client_protocol.ensure_disconnect(node_id)) {
            vlog(
              hbeatlog.info, "Closed unresponsive connection to {}", node_id);
        };
    }

    co_await send_heartbeats(std::move(reqs.requests));
}

ss::future<> heartbeat_manager::do_heartbeat(node_heartbeat&& r) {
    auto gate = _bghbeats.hold();
    vlog(
      hbeatlog.trace,
      "Dispatching hearbeats for {} groups to node: {}",
      r.meta_map.size(),
      r.target);

    auto f = _client_protocol
               .heartbeat(
                 r.target,
                 std::move(r.request),
                 rpc::client_opts(
                   rpc::timeout_spec::from_now(_heartbeat_timeout()),
                   rpc::compression_type::zstd,
                   512))
               .then([node = r.target,
                      groups = std::move(r.meta_map),
                      gate = std::move(gate),
                      this](result<heartbeat_reply> ret) mutable {
                   // this will happen after RPC client will return and resume
                   // sending heartbeats to follower
                   process_reply(node, groups, std::move(ret));
               });
    // fail fast to make sure that not lagging nodes will be able to receive
    // hearteats
    return ss::with_timeout(next_heartbeat_timeout(), std::move(f))
      .handle_exception_type([n = r.target](const ss::timed_out_error&) {
          vlog(hbeatlog.trace, "Heartbeat timeout, node: {}", n);
          // we just ignore this exception since it is the timeout so we do not
          // have to update consensus instances with results
      })
      .handle_exception_type([](const ss::gate_closed_exception&) {})
      .handle_exception([n = r.target](const std::exception_ptr& e) {
          vlog(hbeatlog.trace, "Heartbeat exception, node: {} - {}", n, e);
      });
}

void heartbeat_manager::process_reply(
  model::node_id n,
  const absl::node_hash_map<raft::group_id, follower_request_meta>& groups,
  result<heartbeat_reply> r) {
    if (!r) {
        vlog(
          hbeatlog.debug,
          "Received error when sending heartbeats to node {} - {}",
          n,
          r.error().message());
        for (auto& [g, req_meta] : groups) {
            auto it = _consensus_groups.find(g);
            if (it == _consensus_groups.end()) {
                vlog(
                  hbeatlog.warn,
                  "cannot find consensus group:{}, may have been moved or "
                  "deleted",
                  g);
                continue;
            }
            auto consensus = *it;
            /**
             * We want to reset connection only the connection is not responsive
             * it is indicated by the timeout errors, otherwise we do not want
             * to terminate the connection as it may interrupt other raft groups
             */
            if (
              r.error() == rpc::errc::client_request_timeout
              || r.error() == errc::timeout) {
                consensus->update_heartbeat_status(
                  req_meta.follower_vnode, false);
            }

            // propagate error
            consensus->process_append_entries_reply(
              n,
              result<append_entries_reply>(r.error()),
              req_meta.seq,
              req_meta.dirty_offset);
            consensus->get_probe().heartbeat_request_error();
        }
        return;
    }
    for (auto& m : r.value().meta) {
        auto it = _consensus_groups.find(m.group);
        if (it == _consensus_groups.end()) {
            vlog(
              hbeatlog.debug,
              "Could not find consensus for group:{} (shutting down?)",
              m.group);
            continue;
        }
        auto consensus = *it;

        if (unlikely(m.result == reply_result::group_unavailable)) {
            // We may see these if the responding node is still starting up and
            // the replica has yet to bootstrap.
            vlog(
              hbeatlog.debug,
              "Heartbeat request for group {} was unavailable on node {}",
              m.group,
              n);
            continue;
        }

        if (unlikely(m.result == reply_result::timeout)) {
            vlog(
              hbeatlog.debug,
              "Heartbeat request for group {} timed out on the node {}",
              m.group,
              n);
            continue;
        }

        if (unlikely(m.target_node_id != consensus->self())) {
            vlog(
              hbeatlog.warn,
              "Heartbeat response addressed to different node: {}, current "
              "node: {}, response: {}",
              m.target_node_id,
              consensus->self(),
              m);
            continue;
        }

        auto meta_it = groups.find(m.group);
        if (unlikely(meta_it == groups.end())) {
            vlog(
              hbeatlog.warn,
              "Unexpected heartbeat reply for group {} from node {}, skipping: "
              "{}",
              m.group,
              n,
              m);
            continue;
        }

        consensus->update_heartbeat_status(
          meta_it->second.follower_vnode, true);

        consensus->process_append_entries_reply(
          n,
          result<append_entries_reply>(m),
          meta_it->second.seq,
          meta_it->second.dirty_offset);
    }
}

void heartbeat_manager::dispatch_heartbeats() {
    ssx::background = ssx::spawn_with_gate_then(_bghbeats, [this] {
                          return _lock.with([this] {
                              return do_dispatch_heartbeats().finally([this] {
                                  if (!_bghbeats.is_closed()) {
                                      _heartbeat_timer.arm(
                                        next_heartbeat_timeout());
                                  }
                              });
                          });
                      }).handle_exception([](const std::exception_ptr& e) {
        vlog(hbeatlog.warn, "Error dispatching heartbeats - {}", e);
    });
    // update last
    _hbeat = clock_type::now();
}

ss::future<> heartbeat_manager::deregister_group(group_id g) {
    return _lock.with([this, g] {
        auto it = _consensus_groups.find(g);
        vassert(it != _consensus_groups.end(), "group not found: {}", g);
        _consensus_groups.erase(it);
    });
}

ss::future<>
heartbeat_manager::register_group(ss::lw_shared_ptr<consensus> ptr) {
    return _lock.with([this, ptr = std::move(ptr)] {
        auto ret = _consensus_groups.insert(ptr);
        vassert(
          ret.second,
          "double registration of group: {}:{}",
          ptr->ntp(),
          ptr->group());
    });
}

ss::future<> heartbeat_manager::start() {
    dispatch_heartbeats();
    return ss::make_ready_future<>();
}
ss::future<> heartbeat_manager::stop() {
    _heartbeat_timer.cancel();
    return _bghbeats.close();
}

clock_type::time_point heartbeat_manager::next_heartbeat_timeout() {
    return clock_type::now() + _heartbeat_interval();
}

} // namespace raft
