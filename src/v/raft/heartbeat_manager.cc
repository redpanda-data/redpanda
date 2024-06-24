// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/heartbeat_manager.h"

#include "base/likely.h"
#include "base/vlog.h"
#include "config/configuration.h"
#include "features/feature_table.h"
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
  , follower_vnode(target)
  , hb_guard(c->suppress_heartbeats(follower_vnode)) {}

heartbeat_manager::heartbeat_requests_v2
heartbeat_manager::requests_for_range() {
    absl::node_hash_map<
      model::node_id,
      ss::chunked_fifo<
        std::pair<group_heartbeat, heartbeat_manager::follower_request_meta>>>
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
            if (follower_metadata.are_heartbeats_suppressed()) {
                vlog(r->_ctxlog.trace, "[{}] heartbeat suppressed", id);
                continue;
            }
            if (
              follower_metadata.last_sent_append_entries_req_timestamp
              > last_heartbeat) {
                vlog(r->_ctxlog.trace, "[{}] heartbeat skipped", id);
                continue;
            }
            auto [it, _] = pending_beats.try_emplace(id.id());
            group_heartbeat group_beat{
              .group = r->group(),
            };
            const auto raft_metadata = r->meta();
            if (
              _enable_lw_heartbeat()
              && !needs_full_heartbeat(
                follower_metadata, raft_metadata, r->flushed_offset())) {
                r->_probe->lw_heartbeat();
                // we do not fill the dirty offset and follower request sequence
                // here as those fields are not used to process lightweight
                // heartbeats
                it->second.emplace_back(
                  group_beat,
                  heartbeat_manager::follower_request_meta(
                    r, raft::follower_req_seq{}, model::offset{}, id));
                continue;
            }
            vlog(r->_ctxlog.trace, "[{}] full heartbeat", id);
            r->_probe->full_heartbeat();
            auto const seq_id = follower_metadata.next_follower_sequence();

            follower_metadata.last_sent_protocol_meta = raft_metadata;
            group_beat.data = heartbeat_request_data{
              .source_revision = r->_self.revision(),
              .target_revision = id.revision(),
              .commit_index = raft_metadata.commit_index,
              .term = raft_metadata.term,
              .prev_log_index = raft_metadata.prev_log_index,
              .prev_log_term = raft_metadata.prev_log_term,
              .last_visible_index = raft_metadata.last_visible_index,
            };
            it->second.emplace_back(
              group_beat,
              heartbeat_manager::follower_request_meta(
                r, seq_id, raft_metadata.prev_log_index, id));

            if (r->should_reconnect_follower(follower_metadata)) {
                reconnect_nodes.insert(id.id());
            }
        }
    }

    std::vector<heartbeat_manager::node_heartbeat_v2> reqs;
    reqs.reserve(pending_beats.size());
    for (auto& p : pending_beats) {
        ss::chunked_fifo<group_heartbeat> requests;
        absl::node_hash_map<
          raft::group_id,
          heartbeat_manager::follower_request_meta>
          meta_map;
        requests.reserve(p.second.size());
        heartbeat_request_v2 req(_self, p.first);
        for (auto& [hb, follower_meta] : p.second) {
            meta_map.emplace(hb.group, std::move(follower_meta));
            req.add(hb);
        }
        reqs.emplace_back(p.first, std::move(req), std::move(meta_map));
    }

    return heartbeat_requests_v2{
      .requests{std::move(reqs)}, .reconnect_nodes{reconnect_nodes}};
}

bool heartbeat_manager::needs_full_heartbeat(
  const follower_index_metadata& f_meta,
  const protocol_metadata& p_meta,
  model::offset leader_flushed_offset) const {
    /**
     * This condition makes sending lw_heartbeats not vulnerable for
     * requests/replies reordering.
     *
     * We only send lw_heartbeat if last received not reordered reply was
     * successful and follower acknowledged state is equal to current leader
     * state.
     *
     * Full heartbeat will be sent to the follower every time its responded with
     * error, requests were reordered or leader log was flushed.
     *
     * Last condition is necessary to progress committed index if nothing but
     * the leader flushed offset changed. Flushed offset isn't part of protocol
     * metadata hence it must be checked separately.
     */

    return f_meta.last_sent_seq != f_meta.last_successful_received_seq
           || f_meta.last_sent_protocol_meta != p_meta
           || leader_flushed_offset != f_meta.last_flushed_log_index;
}

heartbeat_manager::heartbeat_manager(
  config::binding<std::chrono::milliseconds> interval,
  consensus_client_protocol proto,
  model::node_id self,
  config::binding<std::chrono::milliseconds> heartbeat_timeout,
  config::binding<bool> enable_lw_heartbeat,
  features::feature_table& ft)
  : _heartbeat_interval(std::move(interval))
  , _heartbeat_timeout(std::move(heartbeat_timeout))
  , _client_protocol(std::move(proto))
  , _self(self)
  , _enable_lw_heartbeat(std::move(enable_lw_heartbeat))
  , _feature_table(ft) {
    _heartbeat_timer.set_callback([this] { dispatch_heartbeats(); });
}

ss::future<>
heartbeat_manager::send_heartbeats(std::vector<node_heartbeat_v2> reqs) {
    return ss::do_with(
      std::move(reqs), [this](std::vector<node_heartbeat_v2>& reqs) mutable {
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

ss::future<> heartbeat_manager::do_heartbeat(node_heartbeat_v2 r) {
    auto gate = _bghbeats.hold();
    vlog(
      hbeatlog.trace,
      "Dispatching heartbeats for {} groups to node: {}",
      r.meta_map.size(),
      r.target);

    auto f = _client_protocol
               .heartbeat_v2(
                 r.target,
                 std::move(r.request),
                 rpc::client_opts(
                   rpc::timeout_spec::from_now(_heartbeat_timeout()),
                   rpc::compression_type::zstd,
                   512))
               .then([node = r.target,
                      groups = std::move(r.meta_map),
                      gate = std::move(gate),
                      this](result<heartbeat_reply_v2> ret) mutable {
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
  result<heartbeat_reply_v2> r) {
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
            consensus->get_probe().heartbeat_request_error();
            if (req_meta.seq == follower_req_seq{}) {
                consensus->reset_last_sent_protocol_meta(
                  req_meta.follower_vnode);
                continue;
            }
            // propagate error
            consensus->process_append_entries_reply(
              n,
              result<append_entries_reply>(r.error()),
              req_meta.seq,
              req_meta.dirty_offset);
        }
        return;
    }
    auto& reply = r.value();
    reply.for_each_lw_reply([this, n, target = reply.target(), &groups](
                              group_id group, reply_result result) {
        auto it = _consensus_groups.find(group);
        if (it == _consensus_groups.end()) {
            vlog(
              hbeatlog.debug,
              "Could not find consensus for group:{} (shutting down?)",
              group);
            return;
        }
        auto consensus = *it;

        if (unlikely(result == reply_result::group_unavailable)) {
            // We may see these if the responding node is still starting up
            // and the replica has yet to bootstrap.
            vlog(
              hbeatlog.debug,
              "Heartbeat request for group {} was unavailable on node {}",
              group,
              n);
            return;
        }

        if (unlikely(result == reply_result::timeout)) {
            vlog(
              hbeatlog.debug,
              "Heartbeat request for group {} timed out on the node {}",
              group,
              n);
            return;
        }
        if (unlikely(target != consensus->self().id())) {
            vlog(
              hbeatlog.warn,
              "Heartbeat response addressed to different node: {}, current "
              "node: {}, source node: {}",
              target,
              consensus->self().id(),
              n);
            return;
        }

        auto meta_it = groups.find(group);

        if (unlikely(meta_it == groups.end())) {
            vlog(
              hbeatlog.warn,
              "Unexpected heartbeat reply for group {} from node {}",
              group,
              n);
            return;
        }

        /**
         * Failed lightweight heartbeat, fallback to full heartbeat
         */
        if (unlikely(result == reply_result::failure)) {
            consensus->reset_last_sent_protocol_meta(
              meta_it->second.follower_vnode);
            return;
        }

        consensus->update_heartbeat_status(
          meta_it->second.follower_vnode, true);
    });

    for (auto& m : reply.full_replies()) {
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

        if (unlikely(reply.target() != consensus->self().id())) {
            vlog(
              hbeatlog.warn,
              "Heartbeat response addressed to different node: {}, current "
              "node: {}, source node: {}",
              reply.target(),
              consensus->self().id(),
              reply.source());
            continue;
        }
        auto meta_it = groups.find(m.group);

        if (unlikely(meta_it == groups.end())) {
            vlog(
              hbeatlog.warn,
              "Unexpected heartbeat reply for group {} from node {}",
              m.group,
              n);
            continue;
        }
        consensus->update_heartbeat_status(
          meta_it->second.follower_vnode, true);

        consensus->process_append_entries_reply(
          n,
          result<append_entries_reply>(append_entries_reply{
            .target_node_id = raft::vnode(
              reply.target(), m.data.target_revision),
            .node_id = raft::vnode(reply.source(), m.data.source_revision),
            .group = m.group,
            .term = m.data.term,
            .last_flushed_log_index = m.data.last_flushed_log_index,
            .last_dirty_log_index = m.data.last_dirty_log_index,
            .last_term_base_offset = m.data.last_term_base_offset,
            .result = m.result,
            .may_recover = m.data.may_recover,
          }),
          meta_it->second.seq,
          meta_it->second.dirty_offset);
    }
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
