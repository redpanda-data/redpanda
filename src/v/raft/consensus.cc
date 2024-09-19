// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/consensus.h"

#include "base/likely.h"
#include "base/vlog.h"
#include "bytes/iostream.h"
#include "config/configuration.h"
#include "config/property.h"
#include "metrics/prometheus_sanitize.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "raft/consensus_client_protocol.h"
#include "raft/consensus_utils.h"
#include "raft/errc.h"
#include "raft/group_configuration.h"
#include "raft/logger.h"
#include "raft/recovery_stm.h"
#include "raft/replicate_entries_stm.h"
#include "raft/rpc_client_protocol.h"
#include "raft/state_machine_manager.h"
#include "raft/types.h"
#include "raft/vote_stm.h"
#include "reflection/adl.h"
#include "rpc/types.h"
#include "ssx/future-util.h"
#include "storage/api.h"
#include "storage/kvstore.h"
#include "storage/ntp_config.h"
#include "storage/snapshot.h"
#include "storage/types.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/util/defer.hh>

#include <fmt/ostream.h>

#include <algorithm>
#include <chrono>
#include <exception>
#include <iterator>
#include <optional>
#include <ranges>
#include <system_error>

template<>
struct fmt::formatter<raft::consensus::vote_state> final
  : fmt::formatter<std::string_view> {
    using vote_state = raft::consensus::vote_state;
    template<typename FormatContext>
    auto format(const vote_state& s, FormatContext& ctx) const {
        std::string_view str = "unknown";
        switch (s) {
        case vote_state::follower:
            str = "follower";
            break;
        case vote_state::candidate:
            str = "candidate";
            break;
        case vote_state::leader:
            str = "leader";
            break;
        }
        return formatter<string_view>::format(str, ctx);
    }
};

namespace raft {

std::vector<model::record_batch_type>
offset_translator_batch_types(const model::ntp& ntp) {
    if (ntp.ns == model::kafka_namespace) {
        return model::offset_translator_batch_types();
    } else {
        return {};
    }
}

consensus::consensus(
  model::node_id nid,
  group_id group,
  group_configuration initial_cfg,
  timeout_jitter jit,
  ss::shared_ptr<storage::log> l,
  scheduling_config scheduling_config,
  config::binding<std::chrono::milliseconds> disk_timeout,
  config::binding<bool> enable_longest_log_detection,
  consensus_client_protocol client,
  consensus::leader_cb_t cb,
  storage::api& storage,
  std::optional<std::reference_wrapper<coordinated_recovery_throttle>>
    recovery_throttle,
  recovery_memory_quota& recovery_mem_quota,
  recovery_scheduler& recovery_scheduler,
  features::feature_table& ft,
  std::optional<voter_priority> voter_priority_override,
  keep_snapshotted_log should_keep_snapshotted_log)
  : _self(nid, initial_cfg.revision_id())
  , _group(group)
  , _jit(std::move(jit))
  , _log(l)
  , _scheduling(scheduling_config)
  , _disk_timeout(std::move(disk_timeout))
  , _enable_longest_log_detection(std::move(enable_longest_log_detection))
  , _client_protocol(client)
  , _leader_notification(std::move(cb))
  , _fstats(
      _self,
      config::shard_local_cfg()
        .raft_max_concurrent_append_requests_per_follower())
  , _batcher(this, config::shard_local_cfg().raft_replicate_batch_window_size())
  , _event_manager(this)
  , _probe(std::make_unique<probe>())
  , _ctxlog(group, _log->config().ntp())
  , _replicate_append_timeout(
      config::shard_local_cfg().replicate_append_timeout_ms())
  , _recovery_append_timeout(
      config::shard_local_cfg().recovery_append_timeout_ms())
  , _heartbeat_disconnect_failures(
      config::shard_local_cfg().raft_heartbeat_disconnect_failures())
  , _storage(storage)
  , _recovery_throttle(recovery_throttle)
  , _recovery_mem_quota(recovery_mem_quota)
  , _recovery_scheduler(recovery_scheduler)
  , _features(ft)
  , _snapshot_mgr(
      std::filesystem::path(_log->config().work_directory()),
      storage::simple_snapshot_manager::default_snapshot_filename,
      _scheduling.default_iopc)
  , _configuration_manager(std::move(initial_cfg), _group, _storage, _ctxlog)
  , _node_priority_override(voter_priority_override)
  , _keep_snapshotted_log(should_keep_snapshotted_log)
  , _append_requests_buffer(*this, 256)
  , _write_caching_enabled(log_config().write_caching())
  , _max_pending_flush_bytes(log_config().flush_bytes())
  , _max_flush_delay(compute_max_flush_delay())
  , _replication_monitor(this) {
    setup_metrics();
    setup_public_metrics();
    update_follower_stats(_configuration_manager.get_latest());
    _vote_timeout.set_callback([this] {
        maybe_step_down();
        dispatch_vote(false);
    });
    _deferred_flusher.set_callback([this]() {
        ssx::spawn_with_gate(
          _bg, [this]() { return do_flush().discard_result(); });
    });
}

void consensus::setup_metrics() {
    namespace sm = ss::metrics;

    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    _probe->setup_metrics(_log->config().ntp());
    auto labels = probe::create_metric_labels(_log->config().ntp());

    _metrics.add_group(
      prometheus_sanitize::metrics_name("raft"),
      {
        sm::make_gauge(
          "leader_for",
          [this] { return is_elected_leader(); },
          sm::description("Number of groups for which node is a leader"),
          labels),
        sm::make_gauge(
          "configuration_change_in_progress",
          [this] {
              return is_elected_leader()
                     && _configuration_manager.get_latest().get_state()
                          != configuration_state::simple;
          },
          sm::description("Indicates if current raft group configuration is in "
                          "joint state i.e. configuration is being changed"),
          labels),
      },
      {},
      {sm::shard_label, sm::label("partition")});
}

void consensus::setup_public_metrics() {
    if (config::shard_local_cfg().disable_public_metrics()) {
        return;
    }

    _probe->setup_public_metrics(_log->config().ntp());
}

void consensus::do_step_down(std::string_view ctx) {
    _hbeat = clock_type::now();
    if (_vstate == vote_state::leader) {
        vlog(
          _ctxlog.info,
          "[{}] Stepping down as leader in term {}, dirty offset {}",
          ctx,
          _term,
          _log->offsets().dirty_offset);
    }
    _fstats.reset();
    _vstate = vote_state::follower;
}

void consensus::maybe_step_down() {
    // ignore stepdown if we are not the leader
    if (_vstate != vote_state::leader) {
        return;
    }

    ssx::spawn_with_gate(_bg, [this] {
        return _op_lock.with([this] {
            // check again while holding a lock
            if (_vstate == vote_state::leader) {
                auto majority_hbeat = majority_heartbeat();
                if (majority_hbeat < _became_leader_at) {
                    majority_hbeat = _became_leader_at;
                }

                if (majority_hbeat + _jit.base_duration() < clock_type::now()) {
                    do_step_down("heartbeats_majority");
                    if (_leader_id) {
                        _leader_id = std::nullopt;
                        trigger_leadership_notification();
                    }
                }
            }
        });
    });
}

clock_type::time_point consensus::majority_heartbeat() const {
    return config().quorum_match([this](vnode rni) {
        if (rni == _self) {
            return clock_type::now();
        }

        if (auto it = _fstats.find(rni); it != _fstats.end()) {
            return it->second.last_received_reply_timestamp;
        }

        // if we do not know the follower state yet i.e. we have
        // never received its heartbeat
        return clock_type::time_point::min();
    });
}

void consensus::shutdown_input() {
    if (likely(!_as.abort_requested())) {
        _vote_timeout.cancel();
        _as.request_abort();
        _commit_index_updated.broken();
        _follower_reply.broken();
    }
}

ss::future<xshard_transfer_state> consensus::stop() {
    vlog(_ctxlog.info, "Stopping");
    shutdown_input();
    for (auto& idx : _fstats) {
        idx.second.follower_state_change.broken();
    }
    co_await _replication_monitor.stop();
    co_await _event_manager.stop();
    if (_stm_manager) {
        co_await _stm_manager->stop();
    }
    co_await _append_requests_buffer.stop();
    co_await _batcher.stop();

    _election_lock.broken();
    _op_lock.broken();
    _deferred_flusher.cancel();
    co_await _bg.close();

    // close writer if we have to
    if (unlikely(_snapshot_writer)) {
        co_await _snapshot_writer->close();
        _snapshot_writer.reset();
    }
    /**
     * Clear metrics after consensus instance is stopped.
     */
    _metrics.clear();
    _probe->clear();

    std::optional<model::term_id> leader_term;
    if (is_elected_leader()) {
        leader_term = _term;
    }
    co_return xshard_transfer_state{.leader_term = leader_term};
}

consensus::success_reply consensus::update_follower_index(
  model::node_id physical_node,
  const result<append_entries_reply>& r,
  follower_req_seq seq,
  model::offset dirty_offset) {
    // do not process replies when stoping
    if (unlikely(_as.abort_requested())) {
        return success_reply::no;
    }

    if (!r) {
        vlog(
          _ctxlog.trace,
          "Error append entries response from {}, {}",
          physical_node,
          r.error().message());
        // add stats to group
        return success_reply::no;
    }
    auto node = r.value().node_id;
    if (node == _self) {
        // We use similar path for replicate entries for both the current node
        // and followers. We do not need to track state of self in follower
        // index metadata as the same state is already tracked in
        // consensus::_meta. However self node accounts to majority and after
        // successful append on current node (which is done in parallel to
        // append on remote nodes) we have to check what is the maximum offset
        // replicated by the majority of nodes, successful replication on
        // current node may change it.
        return success_reply::yes;
    }
    const auto& config = _configuration_manager.get_latest();
    if (!config.contains(node)) {
        // We might have sent an append_entries just before removing
        // a node from configuration: ignore its reply, to avoid
        // doing things like initiating recovery to this removed node.
        vlog(
          _ctxlog.debug,
          "Ignoring reply from node {}, it is not in members list",
          node);
        return success_reply::no;
    }

    auto it = _fstats.find(node);

    if (it == _fstats.end()) {
        return success_reply::no;
    }

    follower_index_metadata& idx = it->second;
    const append_entries_reply& reply = r.value();
    vlog(_ctxlog.trace, "Append entries response: {}", reply);

    /**
     * Do not update any of the follower state if a node that replied to
     * append_entries_request is not the one that the request was addressed to.
     */
    if (unlikely(reply.node_id.id() != physical_node)) {
        vlog(
          _ctxlog.warn,
          "Received append entries response node_id doesn't match expected "
          "node_id (received: {}, expected: {})",
          reply.node_id.id(),
          physical_node);
        return success_reply::no;
    }

    if (unlikely(reply.result == reply_result::timeout)) {
        // ignore this response, timed out on the receiver node
        vlog(_ctxlog.trace, "Append entries request timedout at node {}", node);
        return success_reply::no;
    }
    if (unlikely(reply.result == reply_result::group_unavailable)) {
        // ignore this response since group is not yet bootstrapped at the
        // follower
        vlog(_ctxlog.trace, "Raft group not yet initialized at node {}", node);
        return success_reply::no;
    }
    if (unlikely(reply.group != _group)) {
        // logic bug
        throw std::runtime_error(fmt::format(
          "Append entries response send to wrong group: {}, current group: {}",
          reply.group,
          _group));
    }

    // check preconditions for processing the reply
    if (unlikely(!is_elected_leader())) {
        vlog(_ctxlog.debug, "ignoring append entries reply, not leader");
        return success_reply::no;
    }

    idx.last_received_reply_timestamp = clock_type::now();

    if (
      seq < idx.last_received_seq
      && reply.last_dirty_log_index < _log->offsets().dirty_offset) {
        vlog(
          _ctxlog.trace,
          "ignoring reordered reply {} from node {} - last: {} current: {} ",
          reply,
          reply.node_id,
          idx.last_received_seq,
          seq);
        return success_reply::no;
    }
    /**
     * Even though we allow some of the reordered responsens to be proccessed we
     * do not want it to update last received response sequence. This may lead
     * to processing one of the response that were reordered and should be
     * discarded.
     *
     * example:
     *  assumptions:
     *
     *  - [ seq: ... , lo: ...] denotes a response with given sequence (seq)
     *    containing information about last log offset of a follower (lo)
     *
     *  - request are processed from left to right
     *
     *  [ seq: 100, lo: 10][ seq:97, lo: 11 ][ seq:98, lo: 9 ]
     *
     * In this case we want to accept request with seq 100, but not the one with
     * seq 98, updating the last_received_seq unconditionally would cause
     * accepting request with seq 98, which should be rejected
     */
    idx.last_received_seq = std::min(
      std::max(seq, idx.last_received_seq), idx.last_sent_seq);
    auto broadcast_state_change = ss::defer(
      [&idx] { idx.follower_state_change.broadcast(); });

    // If RPC request or response contains term T > currentTerm:
    // set currentTerm = T, convert to follower (Raft paper: §5.1)
    if (reply.term > _term) {
        ssx::spawn_with_gate(_bg, [this, term = reply.term] {
            return step_down(
              model::term_id(term),
              "append entries response with greater term");
        });
        return success_reply::no;
    }

    // If recovery is in progress the recovery STM will handle follower index
    // updates
    if (!idx.is_recovering) {
        vlog(
          _ctxlog.trace,
          "Updated node {} last committed log index: {}",
          idx.node_id,
          reply.last_flushed_log_index);
        idx.last_dirty_log_index = reply.last_dirty_log_index;
        idx.last_flushed_log_index = reply.last_flushed_log_index;
        idx.next_index = model::next_offset(idx.last_dirty_log_index);
    }

    if (reply.result == reply_result::success) {
        successfull_append_entries_reply(idx, std::move(reply));
        return success_reply::yes;
    } else {
        idx.expected_log_end_offset = model::offset{};
    }

    if (idx.is_recovering) {
        // we are already recovering, if follower dirty log index moved back
        // from some reason (i.e. truncation, data loss, trigger recovery)
        if (idx.last_dirty_log_index > reply.last_dirty_log_index) {
            // update follower state to allow recovery of follower with
            // missing entries
            idx.last_dirty_log_index = reply.last_dirty_log_index;
            idx.last_flushed_log_index = reply.last_flushed_log_index;
            idx.next_index = model::next_offset(idx.last_dirty_log_index);
        }
        return success_reply::no;
    }

    if (needs_recovery(idx, dirty_offset)) {
        if (
          reply.may_recover || ntp() == model::controller_ntp
          || !_features.is_active(
            features::feature::raft_coordinated_recovery)) {
            vlog(
              _ctxlog.trace,
              "Starting recovery process for {} - current reply: {}",
              idx.node_id,
              reply);
            dispatch_recovery(idx);
        } else {
            vlog(
              _ctxlog.trace,
              "Recovery required but not yet permitted by follower {} - "
              "current reply: {}",
              idx.node_id,
              reply);
        }
        return success_reply::no;
    }
    return success_reply::no;
}

void consensus::maybe_promote_to_voter(vnode id) {
    ssx::spawn_with_gate(_bg, [this, id] {
        const auto& latest_cfg = _configuration_manager.get_latest();

        // node is no longer part of current configuration, skip promotion
        if (!latest_cfg.current_config().contains(id)) {
            return ss::now();
        }

        // is voter already
        if (latest_cfg.is_voter(id)) {
            return ss::now();
        }
        auto it = _fstats.find(id);

        // already removed
        if (it == _fstats.end()) {
            return ss::now();
        }

        // do not promote to voter, learner is not up to date
        if (it->second.match_index < _flushed_offset) {
            return ss::now();
        }

        // do not promote if the previous configuration is still uncommitted,
        // otherwise we may add several new voters in quick succession, that the
        // old voters will not know of, resulting in a possibility of
        // non-intersecting quorums.
        if (_configuration_manager.get_latest_offset() > _commit_index) {
            return ss::now();
        }

        return _op_lock.get_units().then([this,
                                          id](ssx::semaphore_units u) mutable {
            // check once more under _op_lock to protect against races with
            // concurrent voter promotions.
            if (_configuration_manager.get_latest_offset() > _commit_index) {
                return ss::now();
            }

            vlog(_ctxlog.trace, "promoting node {} to voter", id);
            auto latest_cfg = _configuration_manager.get_latest();
            latest_cfg.promote_to_voter(id);
            return replicate_configuration(std::move(u), std::move(latest_cfg))
              .then([this, id](std::error_code ec) {
                  vlog(
                    _ctxlog.trace,
                    "node {} promotion result {}",
                    id,
                    ec.message());
              });
        });
    });
}

void consensus::process_append_entries_reply(
  model::node_id physical_node,
  result<append_entries_reply> r,
  follower_req_seq seq_id,
  model::offset dirty_offset) {
    auto is_success = update_follower_index(
      physical_node, r, seq_id, dirty_offset);
    if (is_success) {
        maybe_promote_to_voter(r.value().node_id);
        maybe_update_majority_replicated_index();
        maybe_update_leader_commit_idx();
        _follower_reply.broadcast();
    }
}

void consensus::successfull_append_entries_reply(
  follower_index_metadata& idx, append_entries_reply reply) {
    // follower and leader logs matches
    idx.last_dirty_log_index = reply.last_dirty_log_index;
    idx.last_flushed_log_index = reply.last_flushed_log_index;
    idx.match_index = idx.last_dirty_log_index;
    idx.next_index = model::next_offset(idx.last_dirty_log_index);
    idx.last_successful_received_seq = idx.last_received_seq;
    /**
     * Update expected log end offset only if it is smaller than current value,
     * the check is needed here as there might be pending append entries
     * requests that were not yet replied by the follower.
     */
    idx.expected_log_end_offset = std::max(
      idx.last_dirty_log_index, idx.expected_log_end_offset);
    vlog(
      _ctxlog.trace,
      "Updated node {} match {} and next {} indices",
      idx.node_id,
      idx.match_index,
      idx.next_index);
}

size_t consensus::estimate_recovering_followers() const {
    return std::count_if(_fstats.begin(), _fstats.end(), [](const auto& idx) {
        return idx.second.is_recovering;
    });
}

bool consensus::needs_recovery(
  const follower_index_metadata& idx, model::offset dirty_offset) {
    // follower match_index is behind, we have to recover it

    return idx.match_index < dirty_offset
           || idx.match_index > idx.last_dirty_log_index;
}

void consensus::dispatch_recovery(follower_index_metadata& idx) {
    auto lstats = _log->offsets();
    auto log_max_offset = lstats.dirty_offset;
    if (idx.last_dirty_log_index >= log_max_offset) {
        // follower is ahead of current leader
        // try to send last batch that leader have
        vlog(
          _ctxlog.trace,
          "Follower {} is ahead of the leader setting next offset to {}",
          idx.next_index,
          log_max_offset);
        idx.next_index = log_max_offset;
        idx.expected_log_end_offset = model::offset{};
    }
    idx.is_recovering = true;
    // background
    ssx::background
      = ssx::spawn_with_gate_then(_bg, [this, node_id = idx.node_id] {
            auto recovery = std::make_unique<recovery_stm>(
              this, node_id, _scheduling, _recovery_mem_quota);
            auto ptr = recovery.get();
            return ptr->apply()
              .handle_exception_type(
                [this, node_id](const std::system_error& syserr) {
                    // Likely to contain an rpc::errc such as
                    // client_request_timeout
                    vlog(
                      _ctxlog.info,
                      "Node {} recovery cancelled ({})",
                      node_id,
                      syserr.code().message());
                })
              .handle_exception([this, node_id](const std::exception_ptr& e) {
                  vlog(
                    _ctxlog.warn, "Node {} recovery failed - {}", node_id, e);
              })
              .finally([r = std::move(recovery)] {});
        }).handle_exception([this](const std::exception_ptr& e) {
            vlog(_ctxlog.warn, "Recovery error - {}", e);
        });
}

ss::future<result<model::offset>>
consensus::linearizable_barrier(model::timeout_clock::time_point deadline) {
    using ret_t = result<model::offset>;
    ssx::semaphore_units u;
    try {
        u = co_await _op_lock.get_units();
    } catch (const ss::broken_semaphore&) {
        co_return make_error_code(errc::shutting_down);
    }

    if (!is_leader()) {
        // Callers may expect that our offsets (including commit offset) are
        // all correct after their barrier completes, so do not allow them
        // to barrier if we are in the state where we are elected but have
        // not yet finished replicating the first batch of the new term.
        co_return raft::errc::not_leader;
    }

    if (_vstate != vote_state::leader) {
        co_return result<model::offset>(make_error_code(errc::not_leader));
    }
    /**
     * Flush log on leader, to make sure the _commited_index will be updated
     */
    co_await flush_log();
    const auto cfg = config();
    const auto offsets = _log->offsets();

    vlog(
      _ctxlog.trace,
      "Linearizable barrier requested. Log state: {}, flushed offset: {}",
      offsets,
      _flushed_offset);

    /**
     * Dispatch round of heartbeats
     */

    absl::flat_hash_map<vnode, follower_req_seq> sequences;
    std::vector<ss::future<>> send_futures;
    send_futures.reserve(cfg.unique_voter_count());
    cfg.for_each_voter([this,
                        dirty_offset = offsets.dirty_offset,
                        &sequences,
                        &send_futures](vnode target) {
        // do not send request to self
        if (target == _self) {
            return;
        }
        // prepare empty request
        append_entries_request req(
          _self,
          target,
          meta(),
          model::make_memory_record_batch_reader(
            ss::circular_buffer<model::record_batch>{}),
          flush_after_append::yes);
        auto seq = next_follower_sequence(target);
        sequences.emplace(target, seq);

        update_node_append_timestamp(target);
        vlog(
          _ctxlog.trace, "Sending empty append entries request to {}", target);
        auto f = _client_protocol
                   .append_entries(
                     target.id(),
                     std::move(req),
                     rpc::client_opts(_replicate_append_timeout),
                     use_all_serde_append_entries())
                   .then([this, id = target.id(), seq, dirty_offset](
                           result<append_entries_reply> reply) {
                       process_append_entries_reply(
                         id, reply, seq, dirty_offset);
                   });

        send_futures.push_back(std::move(f));
    });
    // release semaphore
    // term snapshot taken under the semaphore
    auto term = _term;

    u.return_all();

    // wait for responsens in background
    ssx::spawn_with_gate(_bg, [futures = std::move(send_futures)]() mutable {
        return ss::when_all_succeed(futures.begin(), futures.end());
    });

    auto majority_sequences_updated = [&cfg, &sequences, this] {
        return cfg.majority([this, &sequences](vnode id) {
            if (id == _self) {
                return true;
            }
            if (auto it = _fstats.find(id); it != _fstats.end()) {
                return it->second.last_successful_received_seq >= sequences[id];
            }
            return false;
        });
    };

    try {
        // we do not hold the lock while waiting
        co_await _follower_reply.wait(
          deadline, [this, term, &majority_sequences_updated] {
              return majority_sequences_updated() || _term != term;
          });
    } catch (const ss::broken_condition_variable& e) {
        co_return ret_t(make_error_code(errc::shutting_down));
    } catch (const ss::timed_out_error& e) {
        co_return errc::timeout;
    }
    // grab an oplock to serialize state updates i.e. wait for all updates in
    // the state that were caused by follower replies
    auto units = co_await _op_lock.get_units();
    // term have changed, not longer a leader
    if (term != _term) {
        co_return ret_t(make_error_code(errc::not_leader));
    }
    vlog(_ctxlog.trace, "Linearizable offset: {}", _commit_index);
    co_return ret_t(_commit_index);
}

ss::future<result<replicate_result>>
consensus::chain_stages(replicate_stages stages) {
    return stages.request_enqueued.then_wrapped(
      [this, f = std::move(stages.replicate_finished)](
        ss::future<> enqueued) mutable {
          if (enqueued.failed()) {
              return enqueued
                .handle_exception([](const std::exception_ptr& e) {
                    vlog(
                      raftlog.debug, "replicate first stage exception - {}", e);
                })
                .then([this, f = std::move(f)]() mutable {
                    return f.discard_result()
                      .handle_exception([](const std::exception_ptr& e) {
                          vlog(
                            raftlog.debug,
                            "ignoring replicate second stage exception - {}",
                            e);
                      })
                      .then([this] {
                          if (_as.abort_requested()) {
                              return result<replicate_result>(
                                make_error_code(errc::shutting_down));
                          } else {
                              return result<replicate_result>(make_error_code(
                                errc::replicate_first_stage_exception));
                          }
                      });
                });
          }

          return std::move(f);
      });
}

ss::future<result<replicate_result>>
consensus::replicate(model::record_batch_reader&& rdr, replicate_options opts) {
    return chain_stages(do_replicate({}, std::move(rdr), opts));
}

ss::future<result<replicate_result>> consensus::replicate(
  model::term_id expected_term,
  model::record_batch_reader&& rdr,
  replicate_options opts) {
    return chain_stages(do_replicate(expected_term, std::move(rdr), opts));
}

replicate_stages consensus::replicate_in_stages(
  model::record_batch_reader&& rdr, replicate_options opts) {
    return do_replicate({}, std::move(rdr), opts);
}

replicate_stages consensus::replicate_in_stages(
  model::term_id expected_term,
  model::record_batch_reader&& rdr,
  replicate_options opts) {
    return do_replicate(expected_term, std::move(rdr), opts);
}

replicate_stages
wrap_stages_with_gate(ss::gate& gate, replicate_stages stages) {
    return replicate_stages(
      ss::with_gate(
        gate,
        [f = std::move(stages.request_enqueued)]() mutable {
            return std::move(f);
        }),
      ss::with_gate(gate, [f = std::move(stages.replicate_finished)]() mutable {
          return std::move(f);
      }));
}
replicate_stages consensus::do_replicate(
  std::optional<model::term_id> expected_term,
  model::record_batch_reader&& rdr,
  replicate_options opts) {
    // if gate is closed return fast, after this check we are certain that
    // `ss::with_gate` will succeed
    if (unlikely(_bg.is_closed())) {
        return replicate_stages(errc::shutting_down);
    }

    if (!is_elected_leader() || unlikely(_transferring_leadership)) {
        return replicate_stages(errc::not_leader);
    }

    switch (opts.consistency) {
    case consistency_level::no_ack:
        _probe->replicate_requests_ack_none();
        break;
    case consistency_level::leader_ack:
        _probe->replicate_requests_ack_leader();
        break;
    case consistency_level::quorum_ack:
        auto flush = opts.force_flush() || !_write_caching_enabled;
        if (flush) {
            _probe->replicate_requests_ack_all_with_flush();
        } else {
            _probe->replicate_requests_ack_all_without_flush();
        }
        break;
    }

    return wrap_stages_with_gate(
      _bg, _batcher.replicate(expected_term, std::move(rdr), opts));
}

ss::future<model::record_batch_reader>
consensus::do_make_reader(storage::log_reader_config config) {
    // limit to last visible index
    config.max_offset = std::min(config.max_offset, last_visible_index());
    return _log->make_reader(config);
}

ss::future<model::record_batch_reader> consensus::make_reader(
  storage::log_reader_config config,
  std::optional<clock_type::time_point> debounce_timeout) {
    return ss::try_with_gate(_bg, [this, config, debounce_timeout] {
        if (!debounce_timeout) {
            // fast path, do not wait
            return do_make_reader(config);
        }

        return _consumable_offset_monitor
          .wait(
            model::next_offset(_majority_replicated_index),
            *debounce_timeout,
            _as)
          .then([this, config]() mutable { return do_make_reader(config); });
    });
}

bool consensus::should_skip_vote(bool ignore_heartbeat) {
    bool skip_vote = false;

    if (likely(!ignore_heartbeat)) {
        auto last_election = clock_type::now() - _jit.base_duration();
        skip_vote |= (_hbeat > last_election); // nothing to do.
    }

    skip_vote |= _vstate == vote_state::leader; // already a leader

    return skip_vote;
}

ss::future<election_success>
consensus::dispatch_prevote(bool leadership_transfer) {
    auto pv_stm = std::make_unique<vote_stm>(this, is_prevote::yes);

    election_success success = election_success::no;
    try {
        vlog(
          _ctxlog.info,
          "starting pre-vote leader election, current term: {}, leadership "
          "transfer: {}",
          _term,
          leadership_transfer);
        success = co_await pv_stm->vote(leadership_transfer);
    } catch (...) {
        vlog(
          _ctxlog.warn,
          "Error returned from prevoting process {}",
          std::current_exception());
    }

    // make sure we wait for all futures when gate is closed
    if (_bg.is_closed()) {
        co_await pv_stm->wait();
        co_return success;
    }
    // background

    ssx::spawn_with_gate(_bg, [pv_stm = std::move(pv_stm)]() mutable {
        auto stm = pv_stm.get();
        return stm->wait().finally([pv_stm = std::move(pv_stm)] {});
    });

    co_return success;
}

/// performs no raft-state mutation other than resetting the timer
void consensus::dispatch_vote(bool leadership_transfer) {
    // 5.2.1.4 - prepare next timeout
    if (should_skip_vote(leadership_transfer)) {
        arm_vote_timeout();
        return;
    }
    auto self_priority = get_node_priority(_self);
    // check if current node priority is high enough
    // update target priority
    auto cur_target_priority = _target_priority;
    bool current_priority_to_low = cur_target_priority > self_priority;
    // Update target priority: irrespective of vote outcome, we will
    // lower our required priority for next time.
    _target_priority = next_target_priority();

    const auto& latest_config = _configuration_manager.get_latest();
    // skip sending vote request if current node is not a voter in current
    // configuration
    if (!latest_config.is_allowed_to_request_votes(_self)) {
        arm_vote_timeout();
        return;
    }

    // if priority is to low, skip dispatching votes, do not take priority into
    // account when we transfer leadership
    if (current_priority_to_low && !leadership_transfer) {
        const bool is_only_voter = latest_config.unique_voter_count() == 1
                                   && latest_config.is_voter(_self);
        if (!is_only_voter) {
            vlog(
              _ctxlog.trace,
              "current node priority {} is lower than target {} (next vote {})",
              self_priority,
              cur_target_priority,
              _target_priority);
            arm_vote_timeout();
            return;
        }
        vlog(
          _ctxlog.info,
          "current node priority {} is lower than target {}, however the node "
          "is the only voter, continue with dispatching vote",
          self_priority,
          cur_target_priority);
    }
    // background, acquire lock, transition state
    ssx::background
      = ssx::spawn_with_gate_then(_bg, [this, leadership_transfer] {
            return dispatch_prevote(leadership_transfer)
              .then([this, leadership_transfer](
                      election_success prevote_success) mutable {
                  vlog(
                    _ctxlog.debug,
                    "pre-vote phase success: {}, current term: {}, "
                    "leadership transfer: {}",
                    prevote_success,
                    _term,
                    leadership_transfer);
                  // if a current node is not longer candidate we should skip
                  // proceeding to actual vote phase
                  if (!prevote_success || _vstate != vote_state::candidate) {
                      return ss::make_ready_future<>();
                  }
                  auto vstm = std::make_unique<vote_stm>(this);
                  auto p = vstm.get();

                  // CRITICAL: vote performs locking on behalf of consensus
                  return p->vote(leadership_transfer)
                    .then_wrapped(
                      [this, p, vstm = std::move(vstm)](
                        ss::future<election_success> vote_f) mutable {
                          try {
                              vote_f.get();
                          } catch (const ss::gate_closed_exception&) {
                              // Shutting down, don't log.
                          } catch (...) {
                              vlog(
                                _ctxlog.warn,
                                "Error returned from voting process {}",
                                std::current_exception());
                          }
                          auto f = p->wait().finally(
                            [vstm = std::move(vstm)] {});
                          // make sure we wait for all futures when gate is
                          // closed
                          if (_bg.is_closed()) {
                              return f;
                          }
                          // background
                          ssx::spawn_with_gate(
                            _bg, [f = std::move(f)]() mutable {
                                return std::move(f);
                            });

                          return ss::make_ready_future<>();
                      });
              })
              .finally([this] { arm_vote_timeout(); });
        }).handle_exception([this](const std::exception_ptr& e) {
            vlog(_ctxlog.warn, "Exception thrown while voting - {}", e);
        });
}

void consensus::arm_vote_timeout() {
    if (!_bg.is_closed()) {
        _vote_timeout.rearm(_jit());
    }
}

ss::future<std::error_code>
consensus::update_group_member(model::broker broker) {
    return _op_lock.get_units()
      .then([this, broker = std::move(broker)](ssx::semaphore_units u) mutable {
          auto cfg = _configuration_manager.get_latest();
          if (!cfg.contains_broker(broker.id())) {
              vlog(
                _ctxlog.warn,
                "Node with id {} does not exists in current configuration",
                broker.id());
              return ss::make_ready_future<std::error_code>(
                errc::node_does_not_exists);
          }
          // update broker information
          cfg.update(std::move(broker));

          return replicate_configuration(std::move(u), std::move(cfg));
      })
      .handle_exception_type([](const ss::broken_semaphore&) {
          return make_error_code(errc::shutting_down);
      });
}

template<typename Func>
ss::future<std::error_code> consensus::change_configuration(Func&& f) {
    return _op_lock.get_units()
      .then([this, f = std::forward<Func>(f)](ssx::semaphore_units u) mutable {
          // prevent updating configuration if last configuration wasn't
          // committed
          if (_configuration_manager.get_latest_offset() > _commit_index) {
              return ss::make_ready_future<std::error_code>(
                errc::configuration_change_in_progress);
          }
          auto latest_cfg = config();
          // can not change configuration if its type is different than simple
          if (latest_cfg.get_state() != configuration_state::simple) {
              return ss::make_ready_future<std::error_code>(
                errc::configuration_change_in_progress);
          }
          maybe_upgrade_configuration_to_v4(latest_cfg);
          result<group_configuration> res = f(std::move(latest_cfg));
          if (res) {
              if (res.value().revision_id() < config().revision_id()) {
                  return ss::make_ready_future<std::error_code>(
                    errc::invalid_configuration_update);
              }
              return replicate_configuration(
                std::move(u), std::move(res.value()));
          }
          return ss::make_ready_future<std::error_code>(res.error());
      })
      .handle_exception_type([](const ss::broken_semaphore&) {
          return make_error_code(errc::shutting_down);
      });
}

ss::future<std::error_code> consensus::add_group_member(
  model::broker node, model::revision_id new_revision) {
    vlog(_ctxlog.trace, "Adding member: {}", node);
    return change_configuration([node = std::move(node), new_revision](
                                  group_configuration current) mutable {
        using ret_t = result<group_configuration>;
        if (current.contains_broker(node.id())) {
            return ret_t{errc::node_already_exists};
        }
        current.add_broker(std::move(node), new_revision);
        current.set_revision(new_revision);

        return ret_t{std::move(current)};
    });
}

ss::future<std::error_code>
consensus::remove_member(model::node_id id, model::revision_id new_revision) {
    vlog(_ctxlog.trace, "Removing member: {}", id);
    return change_configuration(
      [id, new_revision](group_configuration current) {
          using ret_t = result<group_configuration>;
          if (!current.contains_broker(id)) {
              return ret_t{errc::node_does_not_exists};
          }
          current.set_revision(new_revision);
          current.remove_broker(id);

          if (current.current_config().voters.empty()) {
              return ret_t{errc::invalid_configuration_update};
          }
          return ret_t{std::move(current)};
      });
}

ss::future<std::error_code> consensus::replace_configuration(
  std::vector<raft::broker_revision> new_brokers,
  model::revision_id new_revision) {
    return change_configuration(
      [new_brokers = std::move(new_brokers),
       new_revision](group_configuration current) mutable {
          using ret_t = result<group_configuration>;
          current.replace_brokers(std::move(new_brokers), new_revision);
          current.set_revision(new_revision);
          return ret_t{std::move(current)};
      });
}

ss::future<std::error_code> consensus::add_group_member(
  vnode node,
  model::revision_id new_revision,
  std::optional<model::offset> learner_start_offset) {
    vlog(_ctxlog.trace, "Adding member: {}", node);
    return change_configuration([node, new_revision, learner_start_offset](
                                  group_configuration current) mutable {
        using ret_t = result<group_configuration>;
        if (current.contains(node)) {
            return ret_t{errc::node_already_exists};
        }
        current.set_version(raft::group_configuration::v_5);
        current.add(node, new_revision, learner_start_offset);

        return ret_t{std::move(current)};
    });
}

ss::future<std::error_code>
consensus::remove_member(vnode node, model::revision_id new_revision) {
    vlog(_ctxlog.trace, "Removing member: {}", node);
    return change_configuration(
      [node, new_revision](group_configuration current) {
          using ret_t = result<group_configuration>;
          if (!current.contains(node)) {
              return ret_t{errc::node_does_not_exists};
          }
          current.set_version(raft::group_configuration::v_5);
          current.remove(node, new_revision);

          if (current.current_config().voters.empty()) {
              return ret_t{errc::invalid_configuration_update};
          }
          return ret_t{std::move(current)};
      });
}

ss::future<std::error_code> consensus::replace_configuration(
  std::vector<vnode> nodes,
  model::revision_id new_revision,
  std::optional<model::offset> learner_start_offset) {
    return change_configuration(
      [this, nodes = std::move(nodes), new_revision, learner_start_offset](
        group_configuration current) mutable {
          auto old = current;
          current.set_version(raft::group_configuration::v_5);
          current.replace(nodes, new_revision, learner_start_offset);
          vlog(
            _ctxlog.debug,
            "Replacing current configuration: {} with new configuration: {}",
            old,
            current);

          return result<group_configuration>{std::move(current)};
      });
}

template<typename Func>
ss::future<std::error_code>
consensus::interrupt_configuration_change(model::revision_id revision, Func f) {
    ssx::semaphore_units u;
    try {
        u = co_await _op_lock.get_units();
    } catch (const ss::broken_semaphore&) {
        co_return errc::shutting_down;
    }
    auto latest_cfg = config();
    // latest configuration is of joint type
    if (latest_cfg.get_state() == configuration_state::simple) {
        vlog(_ctxlog.info, "no configuration changes in progress");
        co_return errc::invalid_configuration_update;
    }
    if (latest_cfg.revision_id() > revision) {
        co_return errc::invalid_configuration_update;
    }
    result<group_configuration> new_cfg = f(std::move(latest_cfg));
    if (new_cfg.has_error()) {
        co_return new_cfg.error();
    }

    co_return co_await replicate_configuration(
      std::move(u), std::move(new_cfg.value()));
}

ss::future<std::error_code>
consensus::cancel_configuration_change(model::revision_id revision) {
    vlog(
      _ctxlog.info,
      "requested cancellation of current configuration change - {}",
      config());
    return interrupt_configuration_change(
             revision,
             [this, revision](raft::group_configuration cfg) {
                 /**
                  * Optimization of cancelling configuration change
                  *
                  * When the raft group reconfiguration advanced beyond the
                  * point where nodes from the old configuration are demoted to
                  * learners there is no point of cancellation as the resulting
                  * configuration would require greater quorum then the one
                  * required to finish current configuration change.
                  *
                  */
                 if (cfg.get_state() == raft::configuration_state::joint) {
                     if (!cfg.old_config()->learners.empty()) {
                         vlog(
                           _ctxlog.info,
                           "not cancelling partition configuration as old "
                           "configuration voters are already demoted to "
                           "learners: {}",
                           cfg);
                         return result<group_configuration>(
                           errc::invalid_configuration_update);
                     }
                 }
                 cfg.cancel_configuration_change(revision);
                 return result<group_configuration>(cfg);
             })
      .then([this](std::error_code ec) {
          if (!ec) {
              // current leader is not a voter, step down
              if (!config().is_voter(_self)) {
                  return _op_lock
                    .with([this, ec] {
                        do_step_down("current leader is not voter");
                        return ec;
                    })
                    .handle_exception_type([](const ss::broken_semaphore&) {
                        return make_error_code(errc::shutting_down);
                    });
              }
          }
          return ss::make_ready_future<std::error_code>(ec);
      });
}

ss::future<std::error_code>
consensus::abort_configuration_change(model::revision_id revision) {
    vlog(
      _ctxlog.info,
      "requested abort of current configuration change - {}",
      config());
    ssx::semaphore_units u;
    try {
        u = co_await _op_lock.get_units();
    } catch (const ss::broken_semaphore&) {
        co_return errc::shutting_down;
    }
    auto latest_cfg = config();
    // latest configuration must be of joint type
    if (latest_cfg.get_state() == configuration_state::simple) {
        vlog(_ctxlog.info, "no configuration changes in progress");
        co_return errc::invalid_configuration_update;
    }
    if (latest_cfg.revision_id() > revision) {
        co_return errc::invalid_configuration_update;
    }

    auto new_cfg = latest_cfg;
    new_cfg.abort_configuration_change(revision);

    /**
     * Aborting configuration change is an operation that may lead to data loss.
     * It must be possible to abort configuration change even if there is no
     * leader elected. We simply append new configuration to each of the
     * replicas log. If new leader will be elected using new configuration it
     * will eventually propagate valid configuration to all the followers.
     */
    update_follower_stats(new_cfg);
    _configuration_manager.set_override(std::move(new_cfg));
    do_step_down("reconfiguration-aborted");

    co_return errc::success;
}

ss::future<std::error_code> consensus::force_replace_configuration_locally(
  std::vector<vnode> voters,
  std::vector<vnode> learners,
  model::revision_id new_revision) {
    try {
        auto units = co_await _op_lock.get_units();
        auto new_cfg = group_configuration(
          std::move(voters), std::move(learners), new_revision);
        if (
          new_cfg.version() == group_configuration::v_5
          && use_serde_configuration()) {
            vlog(
              _ctxlog.debug,
              "Upgrading configuration {} version to 6",
              new_cfg);
            new_cfg.set_version(group_configuration::v_6);
        }
        vlog(_ctxlog.info, "Force replacing configuration with: {}", new_cfg);

        update_follower_stats(new_cfg);
        _configuration_manager.set_override(std::move(new_cfg));
        do_step_down("forced-reconfiguration");

    } catch (const ss::broken_semaphore&) {
        co_return errc::shutting_down;
    }
    co_return errc::success;
}

ss::future<> consensus::start(
  std::optional<state_machine_manager_builder> stm_manager_builder,
  std::optional<xshard_transfer_state> xst_state) {
    if (stm_manager_builder) {
        _stm_manager = std::move(stm_manager_builder.value()).build(this);
    }
    return ss::try_with_gate(
      _bg, [this, xst_state = std::move(xst_state)]() mutable {
          return do_start(std::move(xst_state));
      });
}

ss::future<>
consensus::do_start(std::optional<xshard_transfer_state> xst_state) {
    try {
        auto u = co_await _op_lock.get_units();

        read_voted_for();
        bool initial_state = _log->is_new_log();

        vlog(
          _ctxlog.info,
          "Starting with voted_for {} term {} initial_state {}",
          _voted_for,
          _term,
          initial_state);

        co_await _configuration_manager.start(initial_state, _self.revision());
        vlog(
          _ctxlog.trace,
          "Configuration manager started: {}",
          _configuration_manager);

        std::optional<storage::truncate_prefix_config> start_truncate_cfg;
        auto snapshot_units = co_await _snapshot_lock.get_units();
        auto metadata = co_await read_snapshot_metadata();
        if (metadata.has_value()) {
            update_offset_from_snapshot(metadata.value());
            co_await _configuration_manager.add(
              _last_snapshot_index, std::move(metadata->latest_configuration));
            _probe->configuration_update();

            start_truncate_cfg = truncation_cfg_for_snapshot(metadata.value());
            if (start_truncate_cfg.has_value()) {
                _flushed_offset = std::max(
                  _last_snapshot_index, _flushed_offset);
                co_await _configuration_manager.prefix_truncate(
                  _last_snapshot_index);
            }
            _snapshot_size = co_await _snapshot_mgr.get_snapshot_size();
        }
        co_await _log->start(start_truncate_cfg);
        snapshot_units.return_all();

        vlog(
          _ctxlog.debug,
          "Starting raft bootstrap from {}",
          _configuration_manager.get_highest_known_offset());
        auto st = co_await details::read_bootstrap_state(
          _log,
          model::next_offset(_configuration_manager.get_highest_known_offset()),
          _as);

        const auto lstats = _log->offsets();

        vlog(
          _ctxlog.info,
          "Current log offsets: {}, read bootstrap state: {}",
          lstats,
          st);

        // if log term is newer than the one coming from voted_for
        // state, we reset voted_for state
        if (lstats.dirty_offset_term > _term) {
            _term = lstats.dirty_offset_term;
            _voted_for = {};
        }
        /**
         * since we are starting, there were no new writes to the log
         * before that point. It is safe to use dirty offset as a
         * initial flushed offset since it is equal to last offset that
         * exists on disk and was read in log recovery process.
         */
        _flushed_offset = lstats.dirty_offset;
        /**
         * The configuration manager state may be divereged from the log
         * state, as log is flushed lazily, we have to make sure that
         * the log and configuration manager has exactly the same
         * offsets range
         */
        vlog(
          _ctxlog.info, "Truncating configurations at {}", lstats.dirty_offset);

        co_await _configuration_manager.truncate(
          model::next_offset(lstats.dirty_offset));

        /**
         * We read some batches from the log and have to update the
         * configuration manager.
         */
        if (st.config_batches_seen() > 0) {
            co_await _configuration_manager.add(
              std::move(st).release_configurations());
        }

        update_follower_stats(_configuration_manager.get_latest());

        /**
         * fix for incorrectly persisted configuration index. In
         * previous version of redpanda due to the issue with
         * incorrectly assigned raft configuration indicies
         * (https://github.com/redpanda-data/redpanda/issues/2326) there
         * may be a persistent corruption in offset translation caused
         * by incorrectly persited configuration index. It may cause log
         * offset to be negative. Here we check if this problem exists
         * and if so apply necessary offset translation.
         */
        const auto so = start_offset();
        const auto delta = _configuration_manager.offset_delta(start_offset());
        // no prefix truncation was applied we do not need adjustment
        if (so > model::offset(0) || so < delta) {
            // if start offset is smaller than offset delta we need to apply
            // adjustment
            const configuration_manager::configuration_idx new_idx(so());
            vlog(
              _ctxlog.info,
              "adjusting configuration index, current start offset: {}, "
              "delta: {}, new initial index: {}",
              so,
              delta,
              new_idx);
            co_await _configuration_manager.adjust_configuration_idx(new_idx);
        }

        // set last heartbeat timestamp to prevent skipping first
        // election
        _hbeat = clock_type::time_point::min();

        if (xst_state && xst_state->leader_term == _term) {
            // we were the leader before the x-shard transfer, try re-electing
            // immediately.
            dispatch_vote(true);
        } else {
            auto next_election = clock_type::now();
            auto conf = _configuration_manager.get_latest().current_config();
            if (!conf.voters.empty() && _self == conf.voters.front()) {
                // Arm immediate election for single node scenarios
                // or for the very first start of the preferred leader
                // in a multi-node group.  Otherwise use standard election
                // timeout.
                if (conf.voters.size() > 1 && _term > model::term_id{0}) {
                    next_election += _jit.next_duration();
                }
            } else {
                // current node is not a preselected leader, add 2x jitter
                // to give opportunity to the preselected leader to win
                // the first round
                next_election += _jit.base_duration()
                                 + 2 * _jit.next_jitter_duration();
            }

            if (!_bg.is_closed()) {
                _vote_timeout.rearm(next_election);
            }
        }

        const auto last_applied = read_last_applied();
        if (last_applied > lstats.dirty_offset) {
            vlog(
              _ctxlog.error,
              "Inconsistency detected between KVStore last_applied offset({}) "
              "and log end offset ({}).  If the storage directory was not "
              "modified intentionally, this is a bug. Raft in initial state: "
              "{}",
              last_applied,
              lstats.dirty_offset,
              initial_state);
        }

        if (initial_state) {
            co_await _storage.kvs().remove(
              storage::kvstore::key_space::consensus, last_applied_key());
        } else {
            if (last_applied > _commit_index) {
                _commit_index = last_applied;
                maybe_update_last_visible_index(_commit_index);
                vlog(
                  _ctxlog.trace, "Recovered commit_index: {}", _commit_index);
            }
        }

        co_await _event_manager.start();
        _append_requests_buffer.start();
        if (_stm_manager) {
            co_await _stm_manager->start();
        }

        vlog(
          _ctxlog.info,
          "started raft, log offsets: {}, term: {}, configuration: {}",
          lstats,
          _term,
          _configuration_manager.get_latest());

    } catch (const ss::broken_semaphore&) {
    }
}

ss::future<>
consensus::write_voted_for(consensus::voted_for_configuration config) {
    auto key = voted_for_key();
    iobuf val = reflection::to_iobuf(config);
    return _storage.kvs().put(
      storage::kvstore::key_space::consensus, std::move(key), std::move(val));
}

ss::future<> consensus::write_last_applied(model::offset o) {
    /**
     * it is possible that the offset is applied to the state machine before it
     * is flushed on the leader disk. This may lead to situations in which last
     * applied offset stored by a state machine is not readable.
     * In order to keep an invariant that: 'last applied offset MUST be
     * readable' we limit it here to committed (leader flushed) offset.
     */
    const auto limited_offset = std::min(o, _flushed_offset);
    auto key = last_applied_key();
    iobuf val = reflection::to_iobuf(limited_offset);
    return _storage.kvs().put(
      storage::kvstore::key_space::consensus, std::move(key), std::move(val));
}

model::offset consensus::read_last_applied() const {
    const auto key = last_applied_key();
    auto value = _storage.kvs().get(
      storage::kvstore::key_space::consensus, key);

    if (value) {
        return reflection::adl<model::offset>{}.from(std::move(*value));
    }

    return model::offset{};
}

void consensus::read_voted_for() {
    /*
     * Initial values
     */
    _voted_for = vnode{};
    _term = model::term_id(0);

    /*
     * decode the metadata from the key-value store, and delete the old
     * voted_for file, if it exists.
     */
    const auto key = voted_for_key();
    auto value = _storage.kvs().get(
      storage::kvstore::key_space::consensus, key);
    if (value) {
        try {
            auto config = reflection::adl<consensus::voted_for_configuration>{}
                            .from(std::move(*value));
            _voted_for = config.voted_for;
            _term = config.term;
        } catch (...) {
            /**
             * If first attempt to read voted_for failed, read buffer once again
             * and deserialize it with old version i.e. without vnode.
             *
             * NOTE: will be removed in future versions.
             */
            auto value = _storage.kvs().get(
              storage::kvstore::key_space::consensus, key);
            vlog(
              _ctxlog.info,
              "triggerred voter for read fallback, reading previous version of "
              "voted for configuration");
            // fallback to old version
            auto config
              = reflection::adl<consensus::voted_for_configuration_old>{}.from(
                std::move(*value));
            _voted_for = vnode(config.voted_for, model::revision_id(0));
            _term = config.term;
        }

        vlog(
          _ctxlog.info,
          "Recovered persistent state from kvstore: voted for: {}, term: "
          "{}",
          _voted_for,
          _term);
    }
}

ss::future<vote_reply> consensus::vote(vote_request&& r) {
    return with_gate(_bg, [this, r = std::move(r)]() mutable {
        auto target_node_id = r.node_id;
        return _op_lock
          .with(
            _jit.base_duration(),
            [this, r = std::move(r)]() mutable {
                return do_vote(std::move(r)).then([this](vote_reply reply) {
                    vlog(_ctxlog.trace, "vote reply: {}", reply);
                    return reply;
                });
            })
          .handle_exception_type(
            [this, target_node_id](const ss::semaphore_timed_out&) {
                return vote_reply{
                  .target_node_id = target_node_id,
                  .term = _term,
                  .granted = false,
                  .log_ok = false};
            })
          .handle_exception_type(
            [this, target_node_id](const ss::broken_semaphore&) {
                return vote_reply{
                  .target_node_id = target_node_id,
                  .term = _term,
                  .granted = false,
                  .log_ok = false};
            });
    });
}

model::term_id
consensus::get_last_entry_term(const storage::offset_stats& lstats) const {
    if (lstats.dirty_offset >= lstats.start_offset) {
        return lstats.dirty_offset_term;
    }
    // prefix truncated whole log, last term must come from snapshot, as last
    // entry is included into the snapshot
    vassert(
      _last_snapshot_index == lstats.dirty_offset,
      "Last log offset is smaller than its start offset, snapshot is "
      "required to have last included offset that is equal to log dirty "
      "offset. Log offsets: {}. Last snapshot index: {}. {}",
      lstats,
      _last_snapshot_index,
      _log->config().ntp());

    return _last_snapshot_term;
}

ss::future<vote_reply> consensus::do_vote(vote_request r) {
    vote_reply reply;
    reply.term = _term;
    reply.target_node_id = r.node_id;
    reply.node_id = _self;
    auto lstats = _log->offsets();
    auto last_log_index = lstats.dirty_offset;
    _probe->vote_request();
    auto last_entry_term = get_last_entry_term(lstats);
    bool term_changed = false;
    vlog(_ctxlog.trace, "Received vote request: {}", r);

    if (unlikely(is_request_target_node_invalid("vote", r))) {
        reply.log_ok = false;
        reply.granted = false;
        co_return reply;
    }

    // Optimization: for vote requests from nodes that are likely
    // to have been recently restarted (have failed heartbeats
    // and an <= present term), reset their RPC backoff to get
    // a heartbeat sent out sooner.
    //
    // TODO: with the 'hello' RPC this optimization should not be
    // necessary. however, leaving it in (1) should not conflict
    // with the 'hello' RPC based version and (2) leaving this
    // optimization in place for a release cycle means we can
    // simplify a rolling upgrade scenario where nodes are mixed
    // w.r.t. supporting the 'hello' RPC.
    if (is_elected_leader() and r.term <= _term) {
        // Look up follower stats for the requester
        if (auto it = _fstats.find(r.node_id); it != _fstats.end()) {
            auto& fstats = it->second;
            if (fstats.heartbeats_failed) {
                vlog(
                  _ctxlog.debug,
                  "Vote request from peer {} with heartbeat failures, "
                  "resetting backoff",
                  r.node_id);
                co_await _client_protocol.reset_backoff(r.node_id.id());
                co_return reply;
            }
        }
    }

    /// Stable leadership optimization
    ///
    /// When current node is a leader (we set _hbeat to max after
    /// successfull election) or already processed request from active
    /// leader  do not grant a vote to follower. This will prevent restarted
    /// nodes to disturb all groups leadership
    // Check if we updated the heartbeat timepoint in the last election
    // timeout duration When the vote was requested because of leadership
    // transfer grant the vote immediately.
    auto prev_election = clock_type::now() - _jit.base_duration();
    if (
      _hbeat > prev_election && !r.leadership_transfer
      && r.node_id != _voted_for) {
        vlog(
          _ctxlog.trace,
          "Already heard from the leader, not granting vote to node {}",
          r.node_id);
        reply.granted = false;
        co_return reply;
    }

    /// set to true if the caller's log is as up to date as the recipient's
    /// - extension on raft. see Diego's phd dissertation, section 9.6
    /// - "Preventing disruptions when a server rejoins the cluster"
    reply.log_ok
      = r.prev_log_term > last_entry_term
        || (r.prev_log_term == last_entry_term && r.prev_log_index >= last_log_index);

    vlog(
      _ctxlog.trace,
      "vote log_ok response: {}, last_entry_term: {}, last_log_index: {}, log "
      "offsets: {}",
      reply.log_ok,
      last_entry_term,
      last_log_index,
      lstats);

    // raft.pdf: reply false if term < currentTerm (§5.1)
    if (r.term < _term) {
        co_return reply;
    }
    auto n_priority = get_node_priority(r.node_id);
    // do not grant vote if voter priority is lower than current target
    // priority
    if (n_priority < _target_priority && !r.leadership_transfer) {
        vlog(
          _ctxlog.info,
          "not granting vote to node {}, it has priority {} which is lower "
          "than current target priority {}",
          r.node_id,
          n_priority,
          _target_priority);
        reply.granted = false;
        co_return reply;
    }

    if (r.term > _term) {
        vlog(
          _ctxlog.info,
          "Received vote request with larger term from node {}, received {}, "
          "current {}",
          r.node_id,
          r.term,
          _term);
        reply.term = r.term;
        _term = r.term;
        _voted_for = {};
        term_changed = true;
        do_step_down("candidate_term_greater");
        if (_leader_id) {
            _leader_id = std::nullopt;
            trigger_leadership_notification();
        }

        // do not grant vote if log isn't ok
        if (!reply.log_ok) {
            // even though we step down we do not want to update the hbeat as it
            // would cause subsequent votes to fail (_hbeat is updated by the
            // leader)
            _hbeat = clock_type::time_point::min();
            co_return reply;
        }
    }

    // do not grant vote if log isn't ok
    if (!reply.log_ok) {
        co_return reply;
    }

    if (_voted_for.id()() < 0) {
        try {
            co_await write_voted_for({r.node_id, _term});
        } catch (...) {
            vlog(
              _ctxlog.warn,
              "Unable to persist raft group state, vote not granted "
              "- {}",
              std::current_exception());
            reply.granted = false;
            co_return reply;
        }
        _voted_for = r.node_id;
        reply.granted = true;

    } else {
        reply.granted = (r.node_id == _voted_for);
    }
    /**
     * Only update last heartbeat value, indicating existence of a leader if a
     * term change i.e. a node is not processing prevote_request.
     */
    if (reply.granted && term_changed) {
        _hbeat = clock_type::now();
    }

    co_return reply;
}

ss::future<append_entries_reply>
consensus::append_entries(append_entries_request&& r) {
    return with_gate(_bg, [this, r = std::move(r)]() mutable {
        return _append_requests_buffer.enqueue(std::move(r));
    });
}

ss::future<append_entries_reply>
consensus::do_append_entries(append_entries_request&& r) {
    auto lstats = _log->offsets();
    append_entries_reply reply;
    const auto request_metadata = r.metadata();
    reply.node_id = _self;
    reply.target_node_id = r.source_node();
    reply.group = request_metadata.group;
    reply.term = _term;
    reply.last_dirty_log_index = lstats.dirty_offset;
    reply.last_flushed_log_index = _flushed_offset;
    reply.result = reply_result::failure;
    reply.may_recover = _follower_recovery_state
                        && _follower_recovery_state->is_active();

    _probe->append_request();

    if (unlikely(is_request_target_node_invalid("append_entries", r))) {
        co_return reply;
    }
    // no need to trigger timeout
    vlog(_ctxlog.trace, "Received append entries request: {}", r);

    // raft.pdf: Reply false if term < currentTerm (§5.1)
    if (request_metadata.term < _term) {
        reply.result = reply_result::failure;
        co_return reply;
    }
    /**
     * When the current leader is alive, whenever a follower receives heartbeat,
     * it updates its target priority to the initial value
     */
    _target_priority = voter_priority::max();
    do_step_down("append_entries_term_greater");
    if (request_metadata.term > _term) {
        vlog(
          _ctxlog.debug,
          "Append entries request term: {} is greater than current: {}. "
          "Setting new term",
          request_metadata.term,
          _term);
        _term = request_metadata.term;
        _voted_for = {};
        maybe_update_leader(r.source_node());

        co_return co_await do_append_entries(std::move(r));
    }
    // raft.pdf:If AppendEntries RPC received from new leader: convert to
    // follower (§5.2)
    maybe_update_leader(r.source_node());

    // raft.pdf: Reply false if log doesn’t contain an entry at
    // prevLogIndex whose term matches prevLogTerm (§5.3)
    // broken into 3 sections

    auto last_log_offset = lstats.dirty_offset;
    // section 1
    // For an entry to fit into our log, it must not leave a gap.
    if (request_metadata.prev_log_index > last_log_offset) {
        if (!r.batches().is_end_of_stream()) {
            vlog(
              _ctxlog.debug,
              "Rejecting append entries. Would leave gap in log, last log "
              "offset: {} request previous log offset: {}",
              last_log_offset,
              request_metadata.prev_log_index);
        }

        upsert_recovery_state(
          last_log_offset,
          request_metadata.dirty_offset,
          request_metadata.dirty_offset > request_metadata.prev_log_index);
        reply.may_recover = _follower_recovery_state->is_active();

        co_return reply;
    }

    // section 2
    // must come from the same term
    // if prev log index from request is the same as current we can use
    // prev_log_term as an optimization
    auto last_log_term
      = lstats.dirty_offset == request_metadata.prev_log_index
          ? lstats.dirty_offset_term // use term from lstats
          : get_term(model::offset(
              request_metadata
                .prev_log_index)); // lookup for request term in log
    // We can only check prev_log_term for entries that are present in the
    // log. When leader installed snapshot on the follower we may require to
    // skip the term check as term of prev_log_idx may not be available.
    if (
      request_metadata.prev_log_index >= lstats.start_offset
      && request_metadata.prev_log_term != last_log_term) {
        vlog(
          _ctxlog.debug,
          "Rejecting append entries. mismatching entry term at offset: "
          "{}, current term: {} request term: {}",
          request_metadata.prev_log_index,
          last_log_term,
          request_metadata.prev_log_term);
        reply.result = reply_result::failure;

        upsert_recovery_state(
          last_log_offset,
          request_metadata.dirty_offset,
          request_metadata.dirty_offset > request_metadata.prev_log_index);
        reply.may_recover = _follower_recovery_state->is_active();

        co_return reply;
    }

    model::offset adjusted_prev_log_index = request_metadata.prev_log_index;
    if (adjusted_prev_log_index < last_log_offset) {
        // The append point is before the end of our log. We need to skip
        // over batches that we already have (they will have the matching
        // term) to find the true truncation point. This is important for the
        // case when we already have _all_ batches locally (possible if e.g.
        // the request was delayed/duplicated). In this case we don't want to
        // truncate, otherwise we might lose already committed data.

        struct find_mismatch_consumer {
            const consensus& parent;
            model::offset last_log_offset;
            model::offset last_matched;

            ss::future<ss::stop_iteration>
            operator()(const model::record_batch& b) {
                model::offset last_batch_offset
                  = last_matched
                    + model::offset(b.header().last_offset_delta + 1);
                if (
                  last_batch_offset > last_log_offset
                  || parent.get_term(last_batch_offset) != b.term()) {
                    co_return ss::stop_iteration::yes;
                }
                last_matched = last_batch_offset;
                co_return ss::stop_iteration::no;
            }

            model::offset end_of_stream() { return last_matched; }
        };

        model::offset last_matched = co_await r.batches().peek_each_ref(
          find_mismatch_consumer{
            .parent = *this,
            .last_log_offset = last_log_offset,
            .last_matched = adjusted_prev_log_index},
          model::no_timeout); // no_timeout as the batches are already in memory
        if (last_matched != adjusted_prev_log_index) {
            vlog(
              _ctxlog.info,
              "skipped matching records in append_entries batch from {} to {}, "
              "current state: {}",
              adjusted_prev_log_index,
              last_matched,
              meta());
            adjusted_prev_log_index = last_matched;
        }
    }

    // special case for heartbeats and batches without new records.
    // we need to handle it early (before executing truncation)
    // as timeouts are asynchronous to append calls and can have stall data
    if (r.batches().is_end_of_stream()) {
        if (adjusted_prev_log_index < last_log_offset) {
            // do not tuncate on heartbeat just response with false
            reply.result = reply_result::failure;
            co_return reply;
        }
        auto f = ss::now();
        if (r.is_flush_required() && lstats.dirty_offset > _flushed_offset) {
            f = flush_log().discard_result();
        }
        auto last_visible = std::min(
          lstats.dirty_offset, request_metadata.last_visible_index);
        // on the follower leader control visibility of entries in the log
        maybe_update_last_visible_index(last_visible);
        _last_leader_visible_offset = std::max(
          request_metadata.last_visible_index, _last_leader_visible_offset);
        _confirmed_term = _term;
        if (_follower_recovery_state) {
            vlog(
              _ctxlog.debug,
              "exiting follower_recovery_state (heartbeat), "
              "leader meta: {} (our offset: {})",
              request_metadata,
              last_log_offset);
            _follower_recovery_state.reset();
        }

        co_await std::move(f);
        maybe_update_follower_commit_idx(
          model::offset(request_metadata.commit_index));
        reply.last_flushed_log_index = _flushed_offset;
        reply.result = reply_result::success;
        co_return reply;
    }

    if (adjusted_prev_log_index < request_metadata.dirty_offset) {
        // This is a valid recovery request. In case we haven't allowed it,
        // defer to the leader and force-enter the recovery state.
        upsert_recovery_state(
          last_log_offset, request_metadata.dirty_offset, true);
        reply.may_recover = _follower_recovery_state->is_active();
    }

    // section 3
    if (adjusted_prev_log_index < last_log_offset) {
        if (unlikely(adjusted_prev_log_index < _commit_index)) {
            reply.result = reply_result::success;
            // clamp dirty offset to the current commit index not to allow
            // leader reasoning about follower log beyond that point
            reply.last_dirty_log_index = _commit_index;
            reply.last_flushed_log_index = _commit_index;
            vlog(
              _ctxlog.info,
              "Stale append entries request processed, entry is already "
              "present, request: {}, current state: {}",
              request_metadata,
              meta());
            co_return reply;
        }
        auto truncate_at = model::next_offset(
          model::offset(adjusted_prev_log_index));
        vlog(
          _ctxlog.info,
          "Truncating log in term: {}, Request previous log index: {} is "
          "earlier than log end offset: {}, last visible index: {}, leader "
          "last visible index: {}. Truncating to: {}",
          request_metadata.term,
          adjusted_prev_log_index,
          lstats.dirty_offset,
          last_visible_index(),
          _last_leader_visible_offset,
          truncate_at);
        _probe->log_truncated();

        _majority_replicated_index = std::min(
          model::prev_offset(truncate_at), _majority_replicated_index);
        _last_quorum_replicated_index_with_flush = std::min(
          model::prev_offset(truncate_at),
          _last_quorum_replicated_index_with_flush);
        // update flushed offset since truncation may happen to already
        // flushed entries
        _flushed_offset = std::min(
          model::prev_offset(truncate_at), _flushed_offset);

        try {
            co_await _log->truncate(
              storage::truncate_config(truncate_at, _scheduling.default_iopc));
            // update flushed offset once again after truncation as flush is
            // executed concurrently to append entries and it may race with
            // the truncation
            _flushed_offset = std::min(
              model::prev_offset(truncate_at), _flushed_offset);

            co_await _configuration_manager.truncate(truncate_at);
            _probe->configuration_update();
            update_follower_stats(_configuration_manager.get_latest());

            auto lstats = _log->offsets();
            if (unlikely(lstats.dirty_offset != adjusted_prev_log_index)) {
                vlog(
                  _ctxlog.warn,
                  "Log truncation error, expected offset: {}, log offsets: {}, "
                  "requested truncation at {}",
                  adjusted_prev_log_index,
                  lstats,
                  truncate_at);
                _flushed_offset = std::min(
                  model::prev_offset(lstats.dirty_offset), _flushed_offset);
            }
        } catch (...) {
            vlog(
              _ctxlog.warn,
              "Error occurred while truncating log - {}",
              std::current_exception());
            reply.result = reply_result::failure;
            co_return reply;
        }

        co_return co_await do_append_entries(std::move(r));
    }

    // success. copy entries for each subsystem

    try {
        auto deferred = ss::defer([this] {
            // we do not want to include our disk flush latency into
            // the leader vote timeout
            _hbeat = clock_type::now();
        });

        storage::append_result ofs = co_await disk_append(
          std::move(r).release_batches(), update_last_quorum_index::no);
        auto last_visible = std::min(
          ofs.last_offset, request_metadata.last_visible_index);
        maybe_update_last_visible_index(last_visible);

        _last_leader_visible_offset = std::max(
          request_metadata.last_visible_index, _last_leader_visible_offset);
        _confirmed_term = _term;

        maybe_update_follower_commit_idx(request_metadata.commit_index);

        if (_follower_recovery_state) {
            _follower_recovery_state->update_progress(
              ofs.last_offset,
              std::max(request_metadata.dirty_offset, ofs.last_offset));

            if (
              request_metadata.dirty_offset
              == request_metadata.prev_log_index) {
                // Normal (non-recovery, non-heartbeat) append_entries
                // request means that recovery is over.
                vlog(
                  _ctxlog.debug,
                  "exiting follower_recovery_state, leader meta: {} "
                  "(our offset: {})",
                  request_metadata,
                  ofs.last_offset);
                _follower_recovery_state.reset();
            }
            // m.dirty_offset can be bogus here if we are talking to
            // a pre-23.3 redpanda. In this case we can't reliably
            // distinguish between recovery and normal append_entries
            // and will exit recovery only via heartbeats (which is okay
            // but can inflate the number of recovering partitions
            // statistic a bit).
        }
        co_return make_append_entries_reply(reply.target_node_id, ofs);
    } catch (...) {
        vlog(
          _ctxlog.warn,
          "Error occurred while appending log entries - {}",
          std::current_exception());
        reply.result = reply_result::failure;
        co_return reply;
    }
}

void consensus::maybe_update_leader(vnode request_node) {
    if (unlikely(_leader_id != request_node)) {
        _leader_id = request_node;
        _follower_reply.broadcast();
        trigger_leadership_notification();
    }
}

ss::future<install_snapshot_reply>
consensus::install_snapshot(install_snapshot_request&& r) {
    return with_gate(_bg, [this, r = std::move(r)]() mutable {
        return _op_lock
          .with([this, r = std::move(r)]() mutable {
              return _snapshot_lock.with([this, r = std::move(r)]() mutable {
                  return do_install_snapshot(std::move(r));
              });
          })
          .handle_exception_type([this](const ss::broken_semaphore&) {
              return install_snapshot_reply{.term = _term, .success = false};
          });
    });
}

ss::future<> consensus::hydrate_snapshot() {
    // Read snapshot, reset state machine using snapshot contents (and load
    // snapshot’s cluster configuration) (§7.8)
    auto metadata = co_await read_snapshot_metadata();
    if (!metadata.has_value()) {
        co_return;
    }
    update_offset_from_snapshot(metadata.value());
    co_await _configuration_manager.add(
      _last_snapshot_index, std::move(metadata->latest_configuration));
    _probe->configuration_update();
    auto truncate_cfg = truncation_cfg_for_snapshot(metadata.value());
    if (truncate_cfg.has_value()) {
        co_await truncate_to_latest_snapshot(truncate_cfg.value());
    }
    _snapshot_size = co_await _snapshot_mgr.get_snapshot_size();
    update_follower_stats(_configuration_manager.get_latest());
}

std::optional<storage::truncate_prefix_config>
consensus::truncation_cfg_for_snapshot(const snapshot_metadata& metadata) {
    if (
      _keep_snapshotted_log
      && _log->offsets().dirty_offset >= _last_snapshot_index) {
        // skip prefix truncating if we want to preserve the log (e.g. for the
        // controller partition), but only if there is no gap between old end
        // offset and new start offset, otherwise we must still advance the log
        // start offset by prefix-truncating.
        return std::nullopt;
    }
    auto delta = metadata.log_start_delta;
    if (delta < offset_translator_delta(0)) {
        delta = offset_translator_delta(
          _configuration_manager.offset_delta(_last_snapshot_index));
        vlog(
          _ctxlog.warn,
          "received snapshot without delta field in metadata, "
          "falling back to delta obtained from configuration "
          "manager: {}",
          delta);
    }
    return storage::truncate_prefix_config(
      model::next_offset(_last_snapshot_index),
      _scheduling.default_iopc,
      model::offset_delta(delta));
}

ss::future<>
consensus::truncate_to_latest_snapshot(storage::truncate_prefix_config cfg) {
    // we have to prefix truncate config manage at exactly last offset included
    // in snapshot as this is the offset of configuration included in snapshot
    // metadata.
    return _log->truncate_prefix(cfg)
      .then([this] {
          return _configuration_manager.prefix_truncate(_last_snapshot_index);
      })
      .then([this] {
          // when log was prefix truncate flushed offset should be equal to at
          // least last snapshot index
          _flushed_offset = std::max(_last_snapshot_index, _flushed_offset);
      });
}

ss::future<std::optional<snapshot_metadata>>
consensus::read_snapshot_metadata() {
    std::optional<raft::snapshot_metadata> metadata;
    auto snapshot_reader = co_await _snapshot_mgr.open_snapshot();
    if (!snapshot_reader.has_value()) {
        co_return std::nullopt;
    }
    std::exception_ptr eptr;
    try {
        auto buf = co_await snapshot_reader->read_metadata();
        auto parser = iobuf_parser(std::move(buf));
        metadata = reflection::adl<raft::snapshot_metadata>{}.from(parser);
    } catch (...) {
        eptr = std::current_exception();
    }
    co_await snapshot_reader->close();
    if (eptr) {
        std::rethrow_exception(eptr);
    }
    co_return metadata;
}

void consensus::update_offset_from_snapshot(
  const raft::snapshot_metadata& metadata) {
    vassert(
      metadata.last_included_index >= _last_snapshot_index,
      "Tried to load stale snapshot. Loaded snapshot last "
      "index {}, current snapshot last index {}",
      metadata.last_included_index,
      _last_snapshot_index);
    vlog(
      _ctxlog.info,
      "hydrating snapshot with last included index: {}",
      metadata.last_included_index);

    _last_snapshot_index = metadata.last_included_index;
    _last_snapshot_term = metadata.last_included_term;

    auto prev_commit_index = _commit_index;
    _commit_index = std::max(_last_snapshot_index, _commit_index);
    maybe_update_last_visible_index(_commit_index);
    if (prev_commit_index != _commit_index) {
        _commit_index_updated.broadcast();
        _replication_monitor.notify_committed();
        _event_manager.notify_commit_index();
    }
}

ss::future<install_snapshot_reply>
consensus::do_install_snapshot(install_snapshot_request r) {
    vlog(_ctxlog.trace, "received install_snapshot request: {}", r);

    install_snapshot_reply reply{.term = _term, .success = false};
    reply.target_node_id = r.node_id;
    reply.node_id = _self;

    if (unlikely(is_request_target_node_invalid("install_snapshot", r))) {
        co_return reply;
    }

    bool is_done = r.done;
    // Raft paper: Reply immediately if term < currentTerm (§7.1)
    if (r.term < _term) {
        co_return reply;
    }

    // no need to trigger timeout
    _hbeat = clock_type::now();

    // request received from new leader
    do_step_down("install_snapshot_received");
    if (r.term > _term) {
        _term = r.term;
        _voted_for = {};
        maybe_update_leader(r.source_node());
        co_return co_await do_install_snapshot(std::move(r));
    }
    maybe_update_leader(r.source_node());

    // Create new snapshot file if first chunk (offset is 0) (§7.2)
    if (r.file_offset == 0) {
        // discard old chunks, previous snaphost wasn't finished
        if (_snapshot_writer) {
            co_await _snapshot_writer->close();
            co_await _snapshot_mgr.remove_partial_snapshots();
        }

        auto w = co_await _snapshot_mgr.start_snapshot();
        _snapshot_writer.emplace(std::move(w));
        _received_snapshot_index = r.last_included_index;
        _received_snapshot_bytes = 0;
    }

    if (
      r.last_included_index != _received_snapshot_index
      || r.file_offset != _received_snapshot_bytes) {
        // Out of order request? Ignore and answer with success=false.
        co_return reply;
    }

    upsert_recovery_state(r.last_included_index, r.dirty_offset, true);

    // Write data into snapshot file at given offset (§7.3)
    size_t chunk_size = r.chunk.size_bytes();
    co_await write_iobuf_to_output_stream(
      std::move(r.chunk), _snapshot_writer->output());

    _received_snapshot_bytes += chunk_size;
    reply.bytes_stored = _received_snapshot_bytes;

    // Reply and wait for more data chunks if done is false (§7.4)
    if (!is_done) {
        reply.success = true;
        co_return reply;
    }

    // Last chunk, finish storing snapshot
    co_return co_await finish_snapshot(std::move(r), reply);
}

ss::future<install_snapshot_reply> consensus::finish_snapshot(
  install_snapshot_request r, install_snapshot_reply reply) {
    if (!_snapshot_writer) {
        reply.bytes_stored = 0;
        return ss::make_ready_future<install_snapshot_reply>(reply);
    }

    auto f = _snapshot_writer->close();
    // discard any existing or partial snapshot with a smaller index (§7.5)
    if (r.last_included_index < _last_snapshot_index) {
        vlog(
          _ctxlog.warn,
          "Stale snapshot with last index {} received. Previously "
          "received snapshot last included index {}",
          r.last_included_index,
          _last_snapshot_index);

        return f
          .then([this] { return _snapshot_mgr.remove_partial_snapshots(); })
          .then([this, reply]() mutable {
              _snapshot_writer.reset();
              reply.bytes_stored = 0;
              reply.success = false;
              return reply;
          });
    }
    // success case
    return f
      .then([this] {
          return _snapshot_mgr.finish_snapshot(_snapshot_writer.value());
      })
      .then([this, reply]() mutable {
          _snapshot_writer.reset();
          return hydrate_snapshot().then([reply]() mutable {
              reply.success = true;
              return reply;
          });
      });
}

ss::future<> consensus::write_snapshot(write_snapshot_cfg cfg) {
    auto holder = _bg.hold();
    model::offset last_included_index = cfg.last_included_index;
    bool updated = co_await _snapshot_lock
                     .with([this, cfg = std::move(cfg)]() mutable {
                         // do nothing, we already have snapshot for this offset
                         // MUST be checked under the _op_lock
                         if (cfg.last_included_index <= _last_snapshot_index) {
                             return ss::make_ready_future<bool>(false);
                         }

                         auto max_offset = last_visible_index();
                         vassert(
                           cfg.last_included_index <= max_offset,
                           "Can not take snapshot, requested offset: {} is "
                           "greater than max "
                           "snapshot offset: {}",
                           cfg.last_included_index,
                           max_offset);

                         return do_write_snapshot(
                                  cfg.last_included_index, std::move(cfg.data))
                           .then([] { return true; });
                     })
                     .handle_exception_type(
                       [](const ss::broken_semaphore&) { return false; });

    if (!updated) {
        co_return;
    }

    if (_keep_snapshotted_log) {
        // Skip prefix truncating to preserve the log (we want this e.g. for the
        // controller partition in case we need to turn off snapshots).
        co_return;
    }

    // Release the lock when truncating the log because it can take some
    // time while we wait for readers to be evicted.
    co_await _log->truncate_prefix(storage::truncate_prefix_config(
      model::next_offset(last_included_index), _scheduling.default_iopc));

    /*
     * We do not need to keep an oplock when updating the flushed offset here as
     * it is only moved forward so there is no risk of moving the flushed offset
     * back, also there is no risk incorrectly marking dirty batches flushed as
     * the offset will be at most equal to log start offset
     */
    _flushed_offset = std::max(last_included_index, _flushed_offset);

    co_await _snapshot_lock.with([this, last_included_index]() mutable {
        return _configuration_manager.prefix_truncate(last_included_index);
    });
}

ss::future<>
consensus::do_write_snapshot(model::offset last_included_index, iobuf&& data) {
    vlog(
      _ctxlog.trace,
      "Persisting snapshot with last included offset {} of size {}",
      last_included_index,
      data.size_bytes());

    auto last_included_term = _log->get_term(last_included_index);
    vassert(
      last_included_term.has_value(),
      "Unable to get term for snapshot last included offset: {}, log: {}",
      last_included_index,
      _log);
    auto config = _configuration_manager.get(last_included_index);
    vassert(
      config.has_value(),
      "Configuration for offset {} must be available in configuration manager: "
      "{}",
      last_included_index,
      _configuration_manager);

    snapshot_metadata md{
      .last_included_index = last_included_index,
      .last_included_term = last_included_term.value(),
      .latest_configuration = *config,
      .cluster_time = clock_type::time_point::min(),
      .log_start_delta = offset_translator_delta(
        _log->offset_delta(model::next_offset(last_included_index))()),
    };

    return details::persist_snapshot(
             _snapshot_mgr, std::move(md), std::move(data))
      .then([this, last_included_index, term = *last_included_term]() mutable {
          // update consensus state
          _last_snapshot_index = last_included_index;
          _last_snapshot_term = term;
          return _snapshot_mgr.get_snapshot_size();
      })
      .then([this](uint64_t size) { _snapshot_size = size; });
}

ss::future<std::optional<consensus::opened_snapshot>>
consensus::open_snapshot() {
    auto reader = co_await _snapshot_mgr.open_snapshot();
    if (!reader) {
        co_return std::nullopt;
    }

    auto metadata
      = co_await reader->read_metadata()
          .then([](iobuf md_buf) {
              auto md_parser = iobuf_parser(std::move(md_buf));
              return reflection::adl<raft::snapshot_metadata>{}.from(md_parser);
          })
          .then_wrapped([&reader](ss::future<raft::snapshot_metadata> f) {
              if (!f.failed()) {
                  return f;
              }

              return reader->close().then(
                [f = std::move(f)]() mutable { return std::move(f); });
          });

    co_return opened_snapshot{
      .metadata = metadata,
      .reader = std::move(*reader),
    };
}

ss::future<std::optional<ss::file>> consensus::open_snapshot_file() const {
    return _snapshot_mgr.open_snapshot_file();
}

ss::future<std::error_code> consensus::replicate_configuration(
  ssx::semaphore_units u, group_configuration cfg) {
    // under the _op_sem lock
    if (!is_elected_leader()) {
        return ss::make_ready_future<std::error_code>(errc::not_leader);
    }
    vlog(_ctxlog.debug, "Replicating group configuration {}", cfg);
    return ss::with_gate(
      _bg, [this, u = std::move(u), cfg = std::move(cfg)]() mutable {
          maybe_upgrade_configuration_to_v4(cfg);
          if (
            cfg.version() == group_configuration::v_5
            && use_serde_configuration()) {
              vlog(
                _ctxlog.debug, "Upgrading configuration {} version to 6", cfg);
              cfg.set_version(group_configuration::v_6);
          }

          auto batches = details::serialize_configuration_as_batches(
            std::move(cfg));
          for (auto& b : batches) {
              b.set_term(model::term_id(_term));
          }
          auto seqs = next_followers_request_seq();
          append_entries_request req(
            _self,
            meta(),
            model::make_memory_record_batch_reader(std::move(batches)));
          /**
           * We use dispatch_replicate directly as we already hold the
           * _op_lock mutex when replicating configuration
           */
          std::vector<ssx::semaphore_units> units;
          units.push_back(std::move(u));
          return dispatch_replicate(
                   std::move(req), std::move(units), std::move(seqs))
            .then([](result<replicate_result> res) {
                if (res) {
                    return make_error_code(errc::success);
                }
                return res.error();
            });
      });
}

void consensus::maybe_upgrade_configuration_to_v4(group_configuration& cfg) {
    if (unlikely(cfg.version() < group_configuration::v_4)) {
        if (
          _features.is_active(features::feature::raft_improved_configuration)
          && cfg.get_state() == configuration_state::simple) {
            vlog(_ctxlog.debug, "Upgrading configuration version");
            cfg.set_version(raft::group_configuration::v_4);
        }
    }
}

void consensus::update_confirmed_term() {
    auto prev_confirmed = _confirmed_term;
    _confirmed_term = _term;

    if (prev_confirmed != _term && is_elected_leader()) {
        trigger_leadership_notification();
    }
}

ss::future<result<replicate_result>> consensus::dispatch_replicate(
  append_entries_request req,
  std::vector<ssx::semaphore_units> u,
  absl::flat_hash_map<vnode, follower_req_seq> seqs) {
    auto stm = ss::make_lw_shared<replicate_entries_stm>(
      this, std::move(req), std::move(seqs));

    return stm->apply(std::move(u))
      .then([stm](result<replicate_result> res) {
          if (!res) {
              return ss::make_ready_future<result<replicate_result>>(res);
          }
          return stm->wait_for_majority();
      })
      .finally([this, stm] {
          auto f = stm->wait_for_shutdown().finally([stm] {});
          // if gate is closed wait for all futures
          if (_bg.is_closed()) {
              _ctxlog.info(
                "gate-closed, waiting to finish background requests");
              return f;
          }
          // background
          ssx::background
            = ssx::spawn_with_gate_then(_bg, [f = std::move(f)]() mutable {
                  return std::move(f);
              }).handle_exception([this, stm](const std::exception_ptr& e) {
                  _ctxlog.debug(
                    "Error waiting for background acks to finish - {}", e);
              });
          return ss::now();
      });
}

append_entries_reply consensus::make_append_entries_reply(
  vnode target_node, storage::append_result disk_results) {
    append_entries_reply reply;
    reply.node_id = _self;
    reply.target_node_id = target_node;
    reply.group = _group;
    reply.term = _term;
    reply.last_dirty_log_index = disk_results.last_offset;
    reply.last_flushed_log_index = _flushed_offset;
    reply.result = reply_result::success;
    reply.may_recover = _follower_recovery_state
                        && _follower_recovery_state->is_active();
    return reply;
}

ss::future<consensus::flushed> consensus::flush_log() {
    _deferred_flusher.cancel();
    _in_flight_flush = false;
    if (!has_pending_flushes()) {
        _last_flush_time = clock_type::now();
        co_return flushed::no;
    }
    auto flushed_up_to = _log->offsets().dirty_offset;
    const auto prior_truncations = _log->get_log_truncation_counter();
    _probe->log_flushed();
    _pending_flush_bytes = 0;
    co_await _log->flush();
    _last_flush_time = clock_type::now();
    const auto lstats = _log->offsets();
    /**
     * log flush may be interleaved with trucation, hence we need to check
     * if log was truncated, if so we do nothing, flushed offset will be
     * updated in the truncation path.
     *
     * On a follower the log flush may be interleaved with truncation and
     * append. In this case the dirty_offset will not be moving back (e.g the
     * log can be truncated by -1 and then +1 message will be added making
     * dirty_offset equal to the old value and the committed_offset equal to
     * dirty_offset - 1). In this case we shouldn't do anything as well. The
     * truncation path should update the flushed offset.
     */
    if (
      flushed_up_to > lstats.dirty_offset
      || _log->get_log_truncation_counter() > prior_truncations) {
        co_return flushed::yes;
    }

    _flushed_offset = std::max(flushed_up_to, _flushed_offset);
    vlog(_ctxlog.trace, "flushed offset updated: {}", _flushed_offset);

    maybe_update_majority_replicated_index();
    maybe_update_leader_commit_idx();

    // TODO: remove this assertion when we will remove committed_offset
    // from storage.
    vassert(
      lstats.committed_offset >= _flushed_offset,
      "Raft incorrectly tracking flushed log offset. Expected offset: {}, "
      " current log offsets: {}, log: {}",
      _flushed_offset,
      lstats,
      _log);
    co_return flushed::yes;
}

void consensus::background_flush_log() {
    _in_flight_flush = true;
    // An imminent flush means that we do not need the
    // scheduled flush anymore as this guarantees that everything
    // up until this point is flushed.
    _deferred_flusher.cancel();
    ssx::spawn_with_gate(_bg, [this]() { return do_flush().discard_result(); });
}

void consensus::maybe_schedule_flush() {
    if (!has_pending_flushes() || _in_flight_flush) {
        // if a flush is already pending, nothing to do.
        return;
    }
    if (_pending_flush_bytes >= _max_pending_flush_bytes) {
        // max flush bytes exceeded, schedule a flush right away.
        background_flush_log();
        return;
    }
    if (!_deferred_flusher.armed()) {
        // No imminent flush and no deferred flush at this point,
        // arm the timer to ensure appended data honors flush.ms
        _deferred_flusher.arm(flush_ms());
    }
}

ss::future<storage::append_result> consensus::disk_append(
  model::record_batch_reader&& reader,
  update_last_quorum_index should_update_last_quorum_idx) {
    using ret_t = storage::append_result;
    auto cfg = storage::log_append_config{
      // no fsync explicit on a per write, we verify at the end to
      // batch fsync
      storage::log_append_config::fsync::no,
      _scheduling.default_iopc,
      model::timeout_clock::now() + _disk_timeout()};

    class consumer {
    public:
        consumer(storage::log_appender appender)
          : _appender(std::move(appender)) {}

        ss::future<ss::stop_iteration> operator()(model::record_batch& batch) {
            auto ret = co_await _appender(batch);
            co_return ret;
        }

        auto end_of_stream() { return _appender.end_of_stream(); }

    private:
        storage::log_appender _appender;
    };

    return details::for_each_ref_extract_configuration(
             _log->offsets().dirty_offset,
             std::move(reader),
             consumer(_log->make_appender(cfg)),
             cfg.timeout)
      .then([this, should_update_last_quorum_idx](
              std::tuple<ret_t, std::vector<offset_configuration>> t) {
          auto& [ret, configurations] = t;
          _pending_flush_bytes += ret.byte_size;
          if (should_update_last_quorum_idx) {
              /**
               * We have to update last quorum replicated index before we
               * trigger read for followers recovery as recovery_stm will have
               * to deceide if follower flush is required basing on last quorum
               * replicated index.
               */
              _last_quorum_replicated_index_with_flush = ret.last_offset;
          } else {
              // Here are are appending entries without a flush, signal the
              // flusher incase we hit the thresholds, particularly unflushed
              // bytes.
              maybe_schedule_flush();
          }
          // TODO
          // if we rolled a log segment. write current configuration
          // for speedy recovery in the background

          // leader never flush just after write
          // for quorum_ack it flush in parallel to dispatching RPCs
          // to followers for other consistency flushes are done
          // separately.
          auto f = ss::now();
          if (!configurations.empty()) {
              // we can use latest configuration to update follower stats
              f = _configuration_manager.add(std::move(configurations))
                    .then([this] {
                        update_follower_stats(
                          _configuration_manager.get_latest());
                    });
          }

          return f.then([this, ret = ret] {
              // if we are already shutting down, do nothing
              if (_bg.is_closed()) {
                  return ret;
              }

              _configuration_manager
                .maybe_store_highest_known_offset_in_background(
                  ret.last_offset, ret.byte_size, _bg);

              return ret;
          });
      });
}

model::term_id consensus::get_term(model::offset o) const {
    if (unlikely(o < model::offset(0))) {
        return model::term_id{};
    }
    auto lstat = _log->offsets();

    // if log is empty, return term from snapshot
    if (o == lstat.dirty_offset && lstat.start_offset > lstat.dirty_offset) {
        return _last_snapshot_term;
    }

    return _log->get_term(o).value_or(model::term_id{});
}

clock_type::time_point
consensus::last_sent_append_entries_req_timestamp(vnode id) {
    return _fstats.get(id).last_sent_append_entries_req_timestamp;
}

protocol_metadata consensus::meta() const {
    auto lstats = _log->offsets();
    const auto prev_log_term = lstats.dirty_offset >= lstats.start_offset
                                 ? lstats.dirty_offset_term
                                 : _last_snapshot_term;
    return protocol_metadata{
      .group = _group,
      .commit_index = _commit_index,
      .term = _term,
      .prev_log_index = lstats.dirty_offset,
      .prev_log_term = prev_log_term,
      .last_visible_index = last_visible_index(),
      .dirty_offset = lstats.dirty_offset};
}

void consensus::update_node_append_timestamp(vnode id) {
    if (auto it = _fstats.find(id); it != _fstats.end()) {
        it->second.last_sent_append_entries_req_timestamp = clock_type::now();
    }
}
void consensus::maybe_update_node_reply_timestamp(vnode id) {
    if (auto it = _fstats.find(id); it != _fstats.end()) {
        it->second.last_received_reply_timestamp = clock_type::now();
    }
}

follower_req_seq consensus::next_follower_sequence(vnode id) {
    if (auto it = _fstats.find(id); it != _fstats.end()) {
        return it->second.next_follower_sequence();
    }

    return follower_req_seq{};
}

absl::flat_hash_map<vnode, follower_req_seq>
consensus::next_followers_request_seq() {
    absl::flat_hash_map<vnode, follower_req_seq> ret;
    ret.reserve(_fstats.size());
    auto range = boost::make_iterator_range(_fstats.begin(), _fstats.end());
    for (const auto& [node_id, _] : range) {
        ret.emplace(node_id, next_follower_sequence(node_id));
    }
    return ret;
}

ss::future<> consensus::refresh_commit_index() {
    return _op_lock.get_units()
      .then([this](ssx::semaphore_units u) mutable {
          auto f = ss::now();
          if (has_pending_flushes()) {
              f = flush_log().discard_result();
          }

          if (!is_elected_leader()) {
              return f;
          }
          return f.then([this, u = std::move(u)]() mutable {
              return do_maybe_update_leader_commit_idx(std::move(u));
          });
      })
      .handle_exception_type([](const ss::broken_semaphore&) {
          // ignore exception, shutting down
      });
}

void consensus::maybe_update_leader_commit_idx() {
    ssx::background = ssx::spawn_with_gate_then(_bg, [this] {
                          return _op_lock.get_units().then(
                            [this](ssx::semaphore_units u) mutable {
                                // do not update committed index if not the
                                // leader, this check has to be done under the
                                // semaphore
                                if (!is_elected_leader()) {
                                    return ss::now();
                                }
                                return do_maybe_update_leader_commit_idx(
                                  std::move(u));
                            });
                      }).handle_exception([this](const std::exception_ptr& e) {
        vlog(_ctxlog.warn, "Error updating leader commit index", e);
    });
}
/**
 * The `maybe_commit_configuration` method is the place where configuration
 * state transition is decided and executed
 */
ss::future<> consensus::maybe_commit_configuration(ssx::semaphore_units u) {
    // we are not a leader, do nothing
    if (_vstate != vote_state::leader) {
        co_return;
    }

    auto latest_offset = _configuration_manager.get_latest_offset();
    // no configurations were committed
    if (latest_offset > _commit_index) {
        co_return;
    }

    auto latest_cfg = _configuration_manager.get_latest();

    /**
     * current config still contains learners, do nothing
     */
    if (unlikely(!latest_cfg.current_config().learners.empty())) {
        co_return;
    }
    switch (latest_cfg.get_state()) {
    case configuration_state::simple:
        co_return;
    case configuration_state::transitional:
        vlog(
          _ctxlog.info, "finishing transition of configuration {}", latest_cfg);
        latest_cfg.finish_configuration_transition();
        break;
    case configuration_state::joint:
        if (latest_cfg.maybe_demote_removed_voters()) {
            vlog(
              _ctxlog.info,
              "demoting removed voters, new configuration {}",
              latest_cfg);
        } else {
            /**
             * When all old voters were demoted, as a leader, we replicate
             * new configuration
             */
            latest_cfg.discard_old_config();
            vlog(
              _ctxlog.info,
              "leaving joint consensus, new simple configuration {}",
              latest_cfg);
        }
    }

    auto contains_current = latest_cfg.contains(_self);
    auto ec = co_await replicate_configuration(
      std::move(u), std::move(latest_cfg));

    if (ec) {
        if (ec != errc::shutting_down) {
            vlog(
              _ctxlog.info,
              "unable to replicate updated configuration: {}",
              ec.message());
        }
        co_return;
    }
    // leader was removed, step down.
    if (!contains_current) {
        vlog(
          _ctxlog.trace,
          "current node is not longer group member, stepping down");
        co_await transfer_and_stepdown("no_longer_member");
        if (_leader_id) {
            _leader_id = std::nullopt;
            trigger_leadership_notification();
        }
    }
}

ss::future<>
consensus::do_maybe_update_leader_commit_idx(ssx::semaphore_units u) {
    // Raft paper:
    //
    // If there exists an N such that N > commitIndex, a majority
    // of matchIndex[i] ≥ N, and log[N].term == currentTerm:
    // set commitIndex = N (§5.3, §5.4).
    auto majority_match = config().quorum_match([this](vnode id) {
        // current node - we just return commited offset
        if (id == _self) {
            return _flushed_offset;
        }
        if (auto it = _fstats.find(id); it != _fstats.end()) {
            return it->second.match_committed_index();
        }

        return model::offset{};
    });
    if (get_term(majority_match) == _term) {
        update_confirmed_term();
    }
    /**
     * we have to make sure that we do not advance committed_index beyond the
     * point which is readable in log. Since we are not waiting for flush to
     * happen before updating leader commited index we have to limit committed
     * index to the log committed offset. This way we make sure that when read
     * is handled all batches up to committed offset will be visible. Allowing
     * committed offset to be greater than leader flushed offset may result in
     * stale read i.e. even though the committed_index was updated on the leader
     * batcher aren't readable since some of the writes are still in flight in
     * segment appender.
     */
    majority_match = std::min(majority_match, _flushed_offset);

    if (majority_match > _commit_index && get_term(majority_match) == _term) {
        update_confirmed_term();
        _commit_index = majority_match;
        vlog(_ctxlog.trace, "Leader commit index updated {}", _commit_index);

        _replication_monitor.notify_committed();
        _commit_index_updated.broadcast();
        _event_manager.notify_commit_index();
        // if we successfully acknowledged all quorum writes we can make pending
        // relaxed consistency requests visible
        if (_commit_index >= _last_quorum_replicated_index_with_flush) {
            maybe_update_last_visible_index(_majority_replicated_index);
        } else {
            // still have to wait for some quorum consistency requests to be
            // committed
            maybe_update_last_visible_index(_commit_index);
        }
        maybe_update_last_visible_index(_commit_index);
        return maybe_commit_configuration(std::move(u));
    }
    return ss::now();
}

void consensus::maybe_update_follower_commit_idx(
  model::offset request_commit_idx) {
    // Raft paper:
    //
    // If leaderCommit > commitIndex, set commitIndex =
    // min(leaderCommit, index of last new entry)
    if (request_commit_idx > _commit_index) {
        auto new_commit_idx = std::min(request_commit_idx, _flushed_offset);
        if (new_commit_idx != _commit_index) {
            _commit_index = new_commit_idx;
            vlog(
              _ctxlog.trace, "Follower commit index updated {}", _commit_index);
            _replication_monitor.notify_committed();
            _commit_index_updated.broadcast();
            _event_manager.notify_commit_index();
        }
    }
}

void consensus::update_follower_stats(const group_configuration& cfg) {
    vlog(_ctxlog.trace, "Updating follower stats with config {}", cfg);
    _fstats.update_with_configuration(cfg);
}

void consensus::trigger_leadership_notification() {
    if (_leader_id == _self) {
        _probe->leadership_changed();
    }
    vlog(
      _ctxlog.debug,
      "triggering leadership notification with term: {}, new leader: {}",
      _term,
      _leader_id);
    _leader_notification(leadership_status{
      .term = _term, .group = _group, .current_leader = _leader_id});

    if (_follower_recovery_state && !_leader_id) {
        // If we are recovering and the group has lost leadership, it is unclear
        // when it will regain it. Yield the recovery slot so that other groups
        // can make progress.
        _follower_recovery_state->yield();
    }
}

std::ostream& operator<<(std::ostream& o, const consensus& c) {
    fmt::print(
      o,
      "{{log:{}, group_id:{}, term: {}, commit_index: {}, voted_for:{}}}",
      c._log,
      c._group,
      c._term,
      c._commit_index,
      c._voted_for);
    return o;
}

group_configuration consensus::config() const {
    return _configuration_manager.get_latest();
}

ss::future<timeout_now_reply> consensus::timeout_now(timeout_now_request&& r) {
    if (unlikely(is_request_target_node_invalid("timeout_now", r))) {
        return ss::make_ready_future<timeout_now_reply>(timeout_now_reply{
          .term = _term,
          .result = timeout_now_reply::status::failure,
        });
    }

    if (r.term != _term) {
        vlog(
          _ctxlog.debug,
          "Ignoring timeout request from node {} at term {} != {}",
          r.node_id,
          r.term,
          _term);

        auto f = ss::now();
        if (r.term > _term) {
            f = step_down(r.term, "timeout_now");
        }

        return f.then([this] {
            return ss::make_ready_future<timeout_now_reply>(timeout_now_reply{
              .term = _term,
              .result = timeout_now_reply::status::failure,
            });
        });
    }

    if (_vstate != vote_state::follower) {
        vlog(
          _ctxlog.debug,
          "Ignoring timeout request in non-follower state {} from node {} at "
          "term {}",
          _vstate,
          r.node_id,
          r.term);

        return ss::make_ready_future<timeout_now_reply>(timeout_now_reply{
          .term = _term,
          .result = timeout_now_reply::status::failure,
        });
    }

    if (_node_priority_override == zero_voter_priority) {
        vlog(
          _ctxlog.debug,
          "Ignoring timeout request in state {} with node voter priority zero "
          "from node {} at term {}",
          _vstate,
          r.node_id,
          r.term);

        return ss::make_ready_future<timeout_now_reply>(timeout_now_reply{
          .term = _term,
          .result = timeout_now_reply::status::failure,
        });
    }

    // start an election immediately
    dispatch_vote(true);

    vlog(
      _ctxlog.debug,
      "Timeout request election triggered from node {} at term {}",
      r.node_id,
      r.term);

    /*
     * One optimization that we can investigate is returning _term+1 (despite
     * the election having not yet started) and allowing the receiver to step
     * down even before it receives a request vote rpc.
     */
    return ss::make_ready_future<timeout_now_reply>(timeout_now_reply{
      .target_node_id = r.node_id,
      .term = _term,
      .result = timeout_now_reply::status::success,
    });
}

ss::future<transfer_leadership_reply>
consensus::transfer_leadership(transfer_leadership_request req) {
    transfer_leadership_reply reply;
    try {
        auto err = co_await do_transfer_leadership(req);
        if (err) {
            vlog(
              _ctxlog.warn,
              "Unable to transfer leadership to {}: {}",
              req.target,
              err.message());

            reply.success = false;
            if (err.category() == raft::error_category()) {
                reply.result = static_cast<errc>(err.value());
            }
            co_return reply;
        }

        reply.success = true;
        reply.result = errc::success;
        co_return reply;

    } catch (const ss::condition_variable_timed_out& e) {
        vlog(
          _ctxlog.warn,
          "Unable to transfer leadership to {}: {}",
          req.target,
          e);
        reply.success = false;
        reply.result = errc::timeout;
        co_return reply;
    }
}

/**
 * After we have accepted the request to transfer leadership, carry out
 * preparatory phase before we actually send a timeout_now to the new
 * leader.  During this phase we endeavor to make sure the new leader
 * is sufficiently up to date that they will win the election when
 * they start it.
 */
ss::future<std::error_code> consensus::prepare_transfer_leadership(
  vnode target_rni, transfer_leadership_options opts) {
    /*
     * the follower's log needs to be up-to-date so that it will
     * receive votes when we ask it to trigger an immediate
     * election. so check if the followers needs some recovery, and
     * then wait on that process to complete before sending the
     * election request.
     */

    vlog(
      _ctxlog.trace,
      "transfer leadership: preparing target={}, dirty_offset={}",
      target_rni,
      _log->offsets().dirty_offset);

    // Enforce ordering wrt anyone currently doing an append under op_lock
    {
        try {
            auto units = co_await _op_lock.get_units();
            vlog(_ctxlog.trace, "transfer leadership: cleared oplock");
        } catch (const ss::broken_semaphore&) {
            co_return make_error_code(errc::shutting_down);
        }
    }

    // Allow any buffered batches to complete, to avoid racing
    // advances of dirty offset against new leader's recovery
    co_await _batcher.flush({}, true);

    // After we have (maybe) waited for op_lock and batcher,
    // proceed to (maybe) wait for recovery to complete
    if (!_fstats.contains(target_rni)) {
        // Gone?  Nothing to wait for, proceed immediately.
        co_return make_error_code(errc::node_does_not_exists);
    }
    auto& meta = _fstats.get(target_rni);
    if (
      !meta.is_recovering
      && needs_recovery(meta, _log->offsets().dirty_offset)) {
        vlog(
          _ctxlog.debug,
          "transfer leadership: starting node {} recovery",
          target_rni);
        dispatch_recovery(meta); // sets is_recovering flag
    } else {
        vlog(
          _ctxlog.debug,
          "transfer leadership: node {} doesn't need recovery or "
          "is already recovering (is_recovering {} dirty offset {})",
          target_rni,
          meta.is_recovering,
          _log->offsets().dirty_offset);
    }

    auto timeout = ss::semaphore::clock::duration(opts.recovery_timeout);

    if (meta.is_recovering) {
        vlog(
          _ctxlog.info,
          "transfer leadership: waiting for node {} to catch up",
          target_rni);
        meta.follower_state_change.broadcast();
        try {
            co_await meta.recovery_finished.wait(timeout);
        } catch (const ss::condition_variable_timed_out&) {
            vlog(
              _ctxlog.warn,
              "transfer leadership: timed out waiting on node {} "
              "recovery",
              target_rni);
            co_return make_error_code(errc::timeout);
        }
        vlog(
          _ctxlog.info,
          "transfer leadership: finished waiting on node {} "
          "recovery",
          target_rni);
    } else {
        vlog(
          _ctxlog.debug,
          "transfer leadership: node {} is not recovering, proceeding "
          "(dirty offset {})",
          target_rni,
          _log->offsets().dirty_offset);
    }

    co_return make_error_code(errc::success);
}

ss::future<std::error_code>
consensus::do_transfer_leadership(transfer_leadership_request req) {
    auto target = req.target;
    transfer_leadership_options opts{
      .recovery_timeout = req.timeout.value_or(
        config::shard_local_cfg().raft_transfer_leader_recovery_timeout_ms()),
    };

    if (!is_elected_leader()) {
        vlog(_ctxlog.warn, "Cannot transfer leadership from non-leader");
        return seastar::make_ready_future<std::error_code>(
          make_error_code(errc::not_leader));
    }

    // no explicit node was requested. choose the most up to date follower
    if (!target) {
        auto it = std::max_element(
          _fstats.begin(), _fstats.end(), [](const auto& a, const auto& b) {
              return a.second.last_dirty_log_index
                     < b.second.last_dirty_log_index;
          });

        if (unlikely(it == _fstats.end())) {
            vlog(
              _ctxlog.warn,
              "Cannot transfer leadership. No suitable target node found");
            return seastar::make_ready_future<std::error_code>(
              make_error_code(errc::node_does_not_exists));
        }

        target = it->first.id();
    }

    if (*target == _self.id()) {
        vlog(_ctxlog.warn, "Cannot transfer leadership to self");
        return seastar::make_ready_future<std::error_code>(
          make_error_code(errc::transfer_to_current_leader));
    }

    auto conf = _configuration_manager.get_latest();
    if (
      _configuration_manager.get_latest_offset() > last_visible_index()
      || conf.get_state() == configuration_state::joint) {
        vlog(
          _ctxlog.warn,
          "Cannot transfer leadership during configuration change");
        return ss::make_ready_future<std::error_code>(
          make_error_code(errc::configuration_change_in_progress));
    }
    auto target_rni = conf.current_config().find(*target);

    if (!target_rni) {
        vlog(
          _ctxlog.warn,
          "Cannot transfer leadership to node {} not found in configuration",
          *target);
        return seastar::make_ready_future<std::error_code>(
          make_error_code(errc::node_does_not_exists));
    }

    if (!conf.is_voter(*target_rni)) {
        vlog(
          _ctxlog.warn,
          "Cannot transfer leadership to node {} which is a learner",
          *target_rni);
        return seastar::make_ready_future<std::error_code>(
          make_error_code(errc::not_voter));
    }

    vlog(
      _ctxlog.info,
      "Starting leadership transfer from {} to {} in term {}",
      _self,
      *target_rni,
      _term);

    auto f = ss::with_gate(_bg, [this, target_rni = *target_rni, opts] {
        if (_transferring_leadership) {
            vlog(
              _ctxlog.warn,
              "Cannot transfer leadership. Transfer already in "
              "progress.");
            return seastar::make_ready_future<std::error_code>(
              make_error_code(errc::success));
        }

        /*
         * Transferring leadership requires that followers be up to
         * date. In general that requires a bounded amount of recovery.
         * Therefore, we stop activity that would cause recovery to not
         * complete (e.g. appending more data to the log). After
         * transfer completes this flag is cleared; either transfer was
         * successful or not, but operations may continue.
         *
         * NOTE: for acks=quroum recovery is usually quite fast or
         * unnecessary because followers are kept up-to-date. but for
         * asynchronous replication this is not true. a more
         * sophisticated strategy can be taken from online vm transfer:
         * slow operations so that recovery can make progress, and once
         * a threshold has been reached fully stop new activity and
         * complete the transfer.
         */
        _transferring_leadership = true;

        if (!_fstats.contains(target_rni)) {
            return seastar::make_ready_future<std::error_code>(
              make_error_code(errc::node_does_not_exists));
        }

        return prepare_transfer_leadership(target_rni, opts)
          .then([this, target_rni](std::error_code prepare_err) {
              if (prepare_err) {
                  return ss::make_ready_future<std::error_code>(prepare_err);
              }
              /*
               * there are still several scenarios in which we will want
               * to not complete leadership transfer, all of which might
               * have occurred during the recovery process.
               *
               *   - we might have lost leadership status
               *   - shutdown may be in progress
               *   - other: identified by follower not caught-up
               */
              if (!is_leader()) {
                  vlog(
                    _ctxlog.warn, "Cannot transfer leadership from non-leader");
                  return seastar::make_ready_future<std::error_code>(
                    make_error_code(errc::not_leader));
              }

              if (_as.abort_requested()) {
                  vlog(
                    _ctxlog.warn,
                    "Cannot transfer leadership: abort requested");

                  return seastar::make_ready_future<std::error_code>(
                    make_error_code(errc::not_leader));
              }

              if (!_fstats.contains(target_rni)) {
                  vlog(
                    _ctxlog.warn,
                    "Cannot transfer leadership: no stats for {}",
                    target_rni);

                  return seastar::make_ready_future<std::error_code>(
                    make_error_code(errc::node_does_not_exists));
              }

              auto& meta = _fstats.get(target_rni);
              if (needs_recovery(meta, _log->offsets().dirty_offset)) {
                  vlog(
                    _ctxlog.warn,
                    "Cannot transfer leadership: {} needs recovery ({}, {}, "
                    "{})",
                    target_rni,
                    meta.match_index,
                    meta.last_dirty_log_index,
                    _log->offsets().dirty_offset);

                  return seastar::make_ready_future<std::error_code>(
                    make_error_code(errc::exponential_backoff));
              }

              timeout_now_request req{
                .target_node_id = target_rni,
                .node_id = _self,
                .group = _group,
                .term = _term,
              };

              auto timeout
                = raft::clock_type::now()
                  + config::shard_local_cfg().raft_timeout_now_timeout_ms();

              return _client_protocol
                .timeout_now(
                  target_rni.id(), std::move(req), rpc::client_opts(timeout))

                .then([this](result<timeout_now_reply> reply) {
                    if (!reply) {
                        return ss::make_ready_future<std::error_code>(
                          reply.error());
                    } else {
                        // Step down before setting _transferring_leadership
                        // to false, to ensure we do not accept any more
                        // writes in the gap between new leader acking timeout
                        // now and new leader sending a vote for its new term.
                        // (If we accepted more writes, our log could get
                        //  ahead of new leader, and it could lose election)

                        return _op_lock
                          .with([this] {
                              do_step_down("leadership_transfer");
                              if (_leader_id) {
                                  _leader_id = std::nullopt;
                                  trigger_leadership_notification();
                              }

                              return make_error_code(errc::success);
                          })
                          .handle_exception_type(
                            [](const ss::broken_semaphore&) {
                                return make_error_code(errc::shutting_down);
                            });
                    }
                });
          });
    });

    return f.finally([this] { _transferring_leadership = false; });
}

ss::future<> consensus::transfer_and_stepdown(std::string_view ctx) {
    // select a follower with longest log
    auto voters = _fstats
                  | std::views::filter(
                    [](const follower_stats::container_t::value_type& p) {
                        return !p.second.is_learner;
                    });
    auto it = std::max_element(
      voters.begin(), voters.end(), [](const auto& a, const auto& b) {
          return a.second.last_dirty_log_index < b.second.last_dirty_log_index;
      });

    if (unlikely(it == voters.end())) {
        vlog(
          _ctxlog.warn,
          "Unable to find a follower that would be an eligible candidate to "
          "take over the leadership");
        do_step_down(ctx);
        co_return;
    }

    const auto target = it->first;
    vlog(
      _ctxlog.info,
      "[{}] stepping down as leader in term {}, dirty offset {}, with "
      "leadership transfer to {}",
      ctx,
      _term,
      _log->offsets().dirty_offset,
      target);

    timeout_now_request req{
      .target_node_id = target,
      .node_id = _self,
      .group = _group,
      .term = _term,
    };
    auto timeout = raft::clock_type::now()
                   + config::shard_local_cfg().raft_timeout_now_timeout_ms();
    auto r = co_await _client_protocol.timeout_now(
      target.id(), std::move(req), rpc::client_opts(timeout));

    if (r.has_error()) {
        vlog(
          _ctxlog.warn,
          "[{}] stepping down - failed to request timeout_now from {} - "
          "{}",
          ctx,
          target,
          r.error().message());
    } else {
        vlog(
          _ctxlog.trace,
          "[{}]  stepping down - timeout now reply result: {} from node {}",
          ctx,
          r.value(),
          target);
    }
    do_step_down(ctx);
}

ss::future<> consensus::remove_persistent_state() {
    // voted for
    co_await _storage.kvs().remove(
      storage::kvstore::key_space::consensus, voted_for_key());
    // last applied key
    co_await _storage.kvs().remove(
      storage::kvstore::key_space::consensus, last_applied_key());
    // configuration manager
    co_await _configuration_manager.remove_persistent_state();
    // snapshot
    co_await _snapshot_mgr.remove_snapshot();
    co_await _snapshot_mgr.remove_partial_snapshots();
    _snapshot_size = 0;

    co_return;
}

void consensus::maybe_update_last_visible_index(model::offset offset) {
    _visibility_upper_bound_index = std::max(
      _visibility_upper_bound_index, offset);
    do_update_majority_replicated_index(offset);
    if (is_elected_leader()) {
        _last_leader_visible_offset = std::max(
          _last_leader_visible_offset, last_visible_index());
    }
    _consumable_offset_monitor.notify(last_visible_index());
}

void consensus::do_update_majority_replicated_index(model::offset offset) {
    vlog(
      _ctxlog.trace,
      "update majority_replicated_index, new offset: {}, current: {}",
      offset,
      _majority_replicated_index);
    auto previous_majority_replicated_index = _majority_replicated_index;
    _majority_replicated_index = std::max(_majority_replicated_index, offset);
    if (previous_majority_replicated_index != _majority_replicated_index) {
        _replication_monitor.notify_replicated();
    }
}

void consensus::maybe_update_majority_replicated_index() {
    // Expected to be called on the leader

    auto majority_match = config().quorum_match([this](vnode id) {
        if (id == _self) {
            return _log->offsets().dirty_offset;
        }
        if (auto it = _fstats.find(id); it != _fstats.end()) {
            return it->second.last_dirty_log_index;
        }
        return model::offset{};
    });
    do_update_majority_replicated_index(majority_match);
    _last_leader_visible_offset = std::max(
      _last_leader_visible_offset, last_visible_index());
    _consumable_offset_monitor.notify(last_visible_index());
}

consensus::inflight_appends_guard::inflight_appends_guard(
  consensus& parent, vnode target) noexcept
  : _parent(&parent)
  , _term(_parent->term())
  , _target(target) {
    vassert(
      _parent->is_elected_leader(),
      "ntp {}: tried to suppress heartbeats to vnode {} while not being leader "
      "in term {}",
      _parent->ntp(),
      _target,
      _term);

    auto it = _parent->_fstats.find(_target);
    if (it == _parent->_fstats.end()) {
        // make unsuppress() a noop
        _parent = nullptr;
        return;
    }
    ++it->second.inflight_append_request_count;
}

void consensus::inflight_appends_guard::mark_finished() {
    if (!_parent) {
        return;
    }
    if (!_parent->is_elected_leader() || _term != _parent->term()) {
        // lost leadership while the guard was alive.
        // do nothing as suppress_heartbeats_count was reset.
        return;
    }

    auto it = _parent->_fstats.find(_target);
    if (it == _parent->_fstats.end()) {
        // the follower could be removed while the guard is alive
        _parent = nullptr;
        return;
    }

    vassert(
      it->second.inflight_append_request_count > 0,
      "ntp {}: suppress/unsuppress_heartbeats mismatch for vnode {}",
      _parent->ntp(),
      _target);
    --it->second.inflight_append_request_count;
    _parent = nullptr;
}

consensus::inflight_appends_guard consensus::track_append_inflight(vnode id) {
    return inflight_appends_guard{*this, id};
}

void consensus::update_heartbeat_status(vnode id, bool success) {
    if (auto it = _fstats.find(id); it != _fstats.end()) {
        if (success) {
            it->second.last_received_reply_timestamp = clock_type::now();
            it->second.heartbeats_failed = 0;
        } else {
            it->second.heartbeats_failed++;
        }
    }
}

bool consensus::should_reconnect_follower(
  const follower_index_metadata& f_meta) {
    if (_heartbeat_disconnect_failures == 0) {
        // Force disconnection is disabled
        return false;
    }

    const auto last_at = f_meta.last_received_reply_timestamp;
    const auto fail_count = f_meta.heartbeats_failed;

    auto is_live = last_at + _jit.base_duration() > clock_type::now();
    auto since = std::chrono::duration_cast<std::chrono::milliseconds>(
                   clock_type::now() - last_at)
                   .count();
    vlog(
      _ctxlog.trace,
      "should_reconnect_follower({}): {}/{} fails, last ok {}ms ago",
      f_meta.node_id,
      fail_count,
      _heartbeat_disconnect_failures,
      since);
    return fail_count > _heartbeat_disconnect_failures && !is_live;
}

voter_priority consensus::next_target_priority() {
    auto node_count = std::max<size_t>(_fstats.size() + 1, 1);

    return voter_priority(std::max<voter_priority::type>(
      (_target_priority / node_count) * (node_count - 1), min_voter_priority));
}

/**
 * We use simple policy where we calculate priority based on the position of the
 * node in configuration broker vector. We shuffle brokers in raft configuration
 * so it should give us fairly even distribution of leaders across the nodes.
 */
voter_priority consensus::get_node_priority(vnode rni) const {
    if (_node_priority_override.has_value() && rni == _self) {
        return _node_priority_override.value();
    }

    auto& latest_cfg = _configuration_manager.get_latest();
    auto nodes = latest_cfg.all_nodes();

    auto it = std::find(nodes.begin(), nodes.end(), rni);

    if (it == nodes.end()) {
        /**
         * If node is not present in current configuration i.e. was added to the
         * cluster, return max, this way for joining node we will use
         * priorityless, classic raft leader election
         */
        return voter_priority::max();
    }

    auto idx = std::distance(nodes.begin(), it);

    /**
     * Voter priority is inversly proportion to node position in brokers
     * vector.
     */
    return voter_priority(
      (nodes.size() - idx) * (voter_priority::max() / nodes.size()));
}

model::offset consensus::get_latest_configuration_offset() const {
    return _configuration_manager.get_latest_offset();
}

follower_metrics build_follower_metrics(
  model::node_id id,
  const storage::offset_stats& lstats,
  std::chrono::milliseconds liveness_timeout,
  const follower_index_metadata& meta) {
    const auto is_live = meta.last_received_reply_timestamp + liveness_timeout
                         > clock_type::now();
    return follower_metrics{
      .id = id,
      .is_learner = meta.is_learner,
      .committed_log_index = meta.last_flushed_log_index,
      .dirty_log_index = meta.last_dirty_log_index,
      .match_index = meta.match_index,
      .last_heartbeat = meta.last_received_reply_timestamp,
      .is_live = is_live,
      .under_replicated = (meta.is_recovering || !is_live)
                          && meta.match_index < lstats.dirty_offset
                          && !meta.is_learner};
}

std::vector<follower_metrics> consensus::get_follower_metrics() const {
    // if not leader return empty vector, as metrics wouldn't have any sense
    if (!is_elected_leader()) {
        return {};
    }
    std::vector<follower_metrics> ret;
    ret.reserve(_fstats.size());
    const auto offsets = _log->offsets();
    for (const auto& f : _fstats) {
        ret.push_back(build_follower_metrics(
          f.first.id(),
          offsets,
          std::chrono::duration_cast<std::chrono::milliseconds>(
            _jit.base_duration()),
          f.second));
    }

    return ret;
}

result<follower_metrics>
consensus::get_follower_metrics(model::node_id id) const {
    // if not leader return empty vector, as metrics wouldn't have any sense
    if (!is_elected_leader()) {
        return errc::not_leader;
    }
    auto it = std::find_if(_fstats.begin(), _fstats.end(), [id](const auto& p) {
        return p.first.id() == id;
    });
    if (it == _fstats.end()) {
        return errc::node_does_not_exists;
    }
    return build_follower_metrics(
      id,
      _log->offsets(),
      std::chrono::duration_cast<std::chrono::milliseconds>(
        _jit.base_duration()),
      it->second);
}

size_t consensus::get_follower_count() const {
    return is_elected_leader() ? _fstats.size() : 0;
}

ss::future<std::optional<storage::timequery_result>>
consensus::timequery(storage::timequery_config cfg) {
    return _log->timequery(cfg);
}

std::optional<uint8_t> consensus::get_under_replicated() const {
    if (!is_leader()) {
        return std::nullopt;
    }

    uint8_t count = 0;
    for (const auto& f : _fstats) {
        auto f_metrics = build_follower_metrics(
          f.first.id(),
          _log->offsets(),
          std::chrono::duration_cast<std::chrono::milliseconds>(
            _jit.base_duration()),
          f.second);
        if (f_metrics.under_replicated) {
            count += 1;
        }
    }
    return count;
}

reply_result consensus::lightweight_heartbeat(
  model::node_id source_node, model::node_id target_node) {
    /**
     * Handle lightweight heartbeat
     */
    if (unlikely(target_node != _self.id())) {
        vlog(
          _ctxlog.warn,
          "received lw_heartbeat request addressed to different node: {}, "
          "current node: {}, source: {}",
          target_node,
          _self,
          source_node);
        return reply_result::failure;
    }

    /**
     * If leader has changed force full heartbeat
     */
    if (unlikely(
          !_leader_id.has_value() || (_leader_id->id() != source_node))) {
        vlog(
          _ctxlog.trace,
          "requesting full heartbeat from {}, leadership changed",
          source_node);
        return reply_result::failure;
    }
    /**
     * Not yet received heartbeats, follower was restarted while leader is
     * sending out lightweight heartbeats, reply with failure to force sending
     * full heartbeat.
     */
    if (unlikely(_hbeat == clock_type::time_point::min())) {
        vlog(
          _ctxlog.trace,
          "requesting full heartbeat from {}, follower still in an initial "
          "state",
          source_node);
        return reply_result::failure;
    }

    if (unlikely(
          _follower_recovery_state && _follower_recovery_state->is_active())) {
        // If for some reason the leader is sending us lightweight heartbeats
        // after we allowed recovery, notify it by forcing a full heartbeat.
        return reply_result::failure;
    }

    _hbeat = clock_type::now();
    return reply_result::success;
}
ss::future<full_heartbeat_reply> consensus::full_heartbeat(
  group_id group,
  model::node_id source_node,
  model::node_id target_node,
  const heartbeat_request_data& hb_data) {
    const vnode target_vnode(target_node, hb_data.target_revision);
    const vnode source_vnode(source_node, hb_data.source_revision);
    full_heartbeat_reply reply{.group = _group};

    if (unlikely(target_vnode != _self)) {
        vlog(
          _ctxlog.warn,
          "received full heartbeat request addressed to node with different "
          "revision: {}, current node: {}, source: {}",
          target_vnode,
          _self,
          source_vnode);
        reply.result = reply_result::failure;
        co_return reply;
    }
    /**
     * IMPORTANT: do not use request reference after the scheduling point
     */
    append_entries_reply r = co_await append_entries(append_entries_request(
      source_vnode,
      target_vnode,
      protocol_metadata{
        .group = group,
        .commit_index = hb_data.commit_index,
        .term = hb_data.term,
        .prev_log_index = hb_data.prev_log_index,
        .prev_log_term = hb_data.prev_log_term,
        .last_visible_index = hb_data.last_visible_index,
        .dirty_offset = hb_data.prev_log_index,
      },
      model::make_memory_record_batch_reader(
        ss::circular_buffer<model::record_batch>{}),
      flush_after_append::no));

    reply.result = r.result;
    reply.data = heartbeat_reply_data{
      .source_revision = _self.revision(),
      .target_revision = source_vnode.revision(),
      .term = r.term,
      .last_flushed_log_index = r.last_flushed_log_index,
      .last_dirty_log_index = r.last_dirty_log_index,
      .last_term_base_offset = r.last_term_base_offset,
      .may_recover = r.may_recover,
    };
    co_return reply;
}
void consensus::reset_last_sent_protocol_meta(const vnode& node) {
    if (auto it = _fstats.find(node); it != _fstats.end()) {
        it->second.last_sent_protocol_meta.reset();
    }
}

void consensus::upsert_recovery_state(
  model::offset our_last_offset,
  model::offset leader_last_offset,
  bool already_recovering) {
    bool force_active = already_recovering
                        || !_features.is_active(
                          features::feature::raft_coordinated_recovery);

    if (!_follower_recovery_state) {
        _follower_recovery_state.emplace(
          _recovery_scheduler,
          *this,
          our_last_offset,
          leader_last_offset,
          force_active);
        vlog(
          _ctxlog.debug,
          "entering follower_recovery_state, leader last offset: {} (already "
          "recovering: {}), our last offset: {}, may_recover: {}",
          leader_last_offset,
          already_recovering,
          our_last_offset,
          _follower_recovery_state->is_active());
    } else {
        if (force_active && !_follower_recovery_state->is_active()) {
            _follower_recovery_state->force_active();
        }

        _follower_recovery_state->update_progress(
          our_last_offset, leader_last_offset);
    }
}

ss::future<> consensus::do_flush() {
    try {
        auto holder = _bg.hold();
        auto flush = ssx::now(consensus::flushed::no);
        {
            auto u = co_await _op_lock.get_units();
            flush = flush_log();
        }
        auto flushed = co_await std::move(flush);
        if (flushed && is_leader()) {
            for (auto& [id, idx] : _fstats) {
                // force full heartbeat to move the committed index forward
                idx.last_sent_protocol_meta.reset();
            }
        }
    } catch (const ss::gate_closed_exception&) {
    } catch (const ss::broken_semaphore&) {
        // ignore exception, group is shutting down.
    }
}

std::optional<model::offset> consensus::get_learner_start_offset() const {
    const auto& latest_cfg = _configuration_manager.get_latest();

    if (latest_cfg.get_configuration_update()) {
        return latest_cfg.get_configuration_update()->learner_start_offset;
    }
    return std::nullopt;
}

consensus::flush_delay_t consensus::compute_max_flush_delay() const {
    auto delay = flush_jitter_t{log_config().flush_ms(), flush_ms_jitter}
                   .next_duration();
    if (delay > std::chrono::years{1}) {
        // For large delays (eg: long max), the condition variable waiting
        // on this delay runs into a UB because of an overflow. A total
        // hack here is to convert it to a larger duration type so the
        // integral value of the duration is much smaller thus avoiding
        // the overflow. This is a pretty bad hack but needed to
        // workaround seastar's inability to wait for arbitrary large yet
        // valid durations supposedly used for indefinite waits.
        auto delay_years = std::chrono::duration_cast<std::chrono::years>(
          delay);
        return flush_delay_t{delay_years};
    }
    return flush_delay_t{delay};
}

std::chrono::milliseconds consensus::flush_ms() const {
    return std::visit(
      [](auto&& delay) {
          using T = std::decay_t<decltype(delay)>;
          if constexpr (std::is_same_v<T, std::chrono::milliseconds>) {
              return delay;
          } else {
              return std::chrono::duration_cast<std::chrono::milliseconds>(
                delay);
          }
      },
      _max_flush_delay);
}

void consensus::notify_config_update() {
    _write_caching_enabled = log_config().write_caching();
    _max_pending_flush_bytes = log_config().flush_bytes();
    _max_flush_delay = compute_max_flush_delay();
    if (_deferred_flusher.armed()) {
        _deferred_flusher.rearm(ss::lowres_clock::now() + flush_ms());
    }
}

size_t consensus::bytes_to_deliver_to_learners() const {
    if (!is_leader()) {
        return 0;
    }

    size_t total = 0;
    for (auto& [f_id, f_meta] : _fstats) {
        if (f_meta.is_learner) [[unlikely]] {
            total += _log->size_bytes_after_offset(f_meta.match_index);
        }
    }
    return total;
}

} // namespace raft
