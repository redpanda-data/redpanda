// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/consensus.h"

#include "config/configuration.h"
#include "likely.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "prometheus/prometheus_sanitize.h"
#include "raft/consensus_client_protocol.h"
#include "raft/consensus_utils.h"
#include "raft/errc.h"
#include "raft/group_configuration.h"
#include "raft/logger.h"
#include "raft/prevote_stm.h"
#include "raft/recovery_stm.h"
#include "raft/replicate_entries_stm.h"
#include "raft/rpc_client_protocol.h"
#include "raft/types.h"
#include "raft/vote_stm.h"
#include "reflection/adl.h"
#include "storage/api.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>

#include <fmt/ostream.h>

#include <algorithm>
#include <iterator>

namespace raft {

static std::vector<model::record_batch_type>
offset_translator_batch_types(const model::ntp& ntp) {
    if (ntp.ns == model::kafka_namespace) {
        return {
          model::record_batch_type::raft_configuration,
          model::record_batch_type::archival_metadata};
    } else {
        return {};
    }
}

consensus::consensus(
  model::node_id nid,
  group_id group,
  group_configuration initial_cfg,
  timeout_jitter jit,
  storage::log l,
  scheduling_config scheduling_config,
  model::timeout_clock::duration disk_timeout,
  consensus_client_protocol client,
  consensus::leader_cb_t cb,
  storage::api& storage,
  std::optional<std::reference_wrapper<recovery_throttle>> recovery_throttle)
  : _self(nid, initial_cfg.revision_id())
  , _group(group)
  , _jit(std::move(jit))
  , _log(l)
  , _offset_translator(ss::make_lw_shared<offset_translator>(
      offset_translator_batch_types(_log.config().ntp()),
      group,
      _log.config().ntp(),
      storage))
  , _scheduling(scheduling_config)
  , _disk_timeout(disk_timeout)
  , _client_protocol(client)
  , _leader_notification(std::move(cb))
  , _fstats(
      _self,
      config::shard_local_cfg()
        .raft_max_concurrent_append_requests_per_follower())
  , _batcher(this, config::shard_local_cfg().raft_replicate_batch_window_size())
  , _event_manager(this)
  , _ctxlog(group, _log.config().ntp())
  , _replicate_append_timeout(
      config::shard_local_cfg().replicate_append_timeout_ms())
  , _recovery_append_timeout(
      config::shard_local_cfg().recovery_append_timeout_ms())
  , _heartbeat_disconnect_failures(
      config::shard_local_cfg().raft_heartbeat_disconnect_failures())
  , _storage(storage)
  , _recovery_throttle(recovery_throttle)
  , _snapshot_mgr(
      std::filesystem::path(_log.config().work_directory()),
      storage::simple_snapshot_manager::default_snapshot_filename,
      _scheduling.default_iopc)
  , _configuration_manager(std::move(initial_cfg), _group, _storage, _ctxlog)
  , _append_requests_buffer(*this, 256) {
    setup_metrics();
    update_follower_stats(_configuration_manager.get_latest());
    _vote_timeout.set_callback([this] {
        maybe_step_down();
        dispatch_vote(false);
    });
}

void consensus::setup_metrics() {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    _probe.setup_metrics(_log.config().ntp());
    auto labels = probe::create_metric_labels(_log.config().ntp());
    namespace sm = ss::metrics;

    _metrics.add_group(
      prometheus_sanitize::metrics_name("raft"),
      {sm::make_gauge(
        "leader_for",
        [this] { return is_leader(); },
        sm::description("Number of groups for which node is a leader"),
        labels)});
}

void consensus::do_step_down() {
    _hbeat = clock_type::now();
    if (_vstate == vote_state::leader) {
        vlog(
          _ctxlog.info,
          "Stepping down as leader in term {}, dirty offset {}",
          _term,
          _log.offsets().dirty_offset);
    }
    _vstate = vote_state::follower;
}

void consensus::maybe_step_down() {
    (void)ss::with_gate(_bg, [this] {
        return _op_lock.with([this] {
            if (_vstate == vote_state::leader) {
                auto majority_hbeat = majority_heartbeat();
                if (majority_hbeat < _became_leader_at) {
                    majority_hbeat = _became_leader_at;
                }

                if (majority_hbeat + _jit.base_duration() < clock_type::now()) {
                    do_step_down();
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
            return it->second.last_received_append_entries_reply_timestamp;
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
        _disk_append.broken();
        _follower_reply.broken();
    }
}

ss::future<> consensus::stop() {
    vlog(_ctxlog.info, "Stopping");
    shutdown_input();

    return _event_manager.stop()
      .then([this] { return _append_requests_buffer.stop(); })
      .then([this] { return _batcher.stop(); })
      .then([this] { return _bg.close(); })
      .then([this] {
          // close writer if we have to
          if (likely(!_snapshot_writer)) {
              return ss::now();
          }
          return _snapshot_writer->close().then(
            [this] { _snapshot_writer.reset(); });
      });
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

    auto it = _fstats.find(node);

    if (it == _fstats.end()) {
        return success_reply::no;
    }

    follower_index_metadata& idx = it->second;
    const append_entries_reply& reply = r.value();
    vlog(_ctxlog.trace, "Append entries response: {}", reply);
    if (unlikely(reply.result == append_entries_reply::status::timeout)) {
        // ignore this response, timed out on the receiver node
        vlog(_ctxlog.trace, "Append entries request timedout at node {}", node);
        return success_reply::no;
    }
    if (unlikely(
          reply.result == append_entries_reply::status::group_unavailable)) {
        // ignore this response since group is not yet bootstrapped at the
        // follower
        vlog(_ctxlog.trace, "Raft group not yet initialized at node {}", node);
        return success_reply::no;
    }
    if (unlikely(reply.group != _group)) {
        // logic bug
        throw std::runtime_error(fmt::format(
          "Append entries response send to wrong group: {}, current group: {}",
          reply.group));
    }

    update_node_hbeat_timestamp(node);

    if (
      seq < idx.last_received_seq
      && reply.last_dirty_log_index < _log.offsets().dirty_offset) {
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
    idx.last_received_seq = std::max(seq, idx.last_received_seq);

    // check preconditions for processing the reply
    if (!is_leader()) {
        vlog(_ctxlog.debug, "ignoring append entries reply, not leader");
        return success_reply::no;
    }
    // If RPC request or response contains term T > currentTerm:
    // set currentTerm = T, convert to follower (Raft paper: §5.1)
    if (reply.term > _term) {
        (void)with_gate(_bg, [this, term = reply.term] {
            return step_down(model::term_id(term));
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
        idx.next_index = details::next_offset(idx.last_dirty_log_index);
    }

    if (reply.result == append_entries_reply::status::success) {
        successfull_append_entries_reply(idx, std::move(reply));
        return success_reply::yes;
    }

    if (idx.is_recovering) {
        // we are already recovering, if follower dirty log index moved back
        // from some reason (i.e. truncation, data loss, trigger recovery)
        if (idx.last_dirty_log_index > reply.last_dirty_log_index) {
            // update follower state to allow recovery of follower with
            // missing entries
            idx.last_dirty_log_index = reply.last_dirty_log_index;
            idx.last_flushed_log_index = reply.last_flushed_log_index;
            idx.next_index = details::next_offset(idx.last_dirty_log_index);
            idx.follower_state_change.broadcast();
        }
        return success_reply::no;
    }

    if (needs_recovery(idx, dirty_offset)) {
        vlog(
          _ctxlog.trace,
          "Starting recovery process for {} - current reply: {}",
          idx.node_id,
          reply);
        dispatch_recovery(idx);
        return success_reply::no;
    }
    return success_reply::no;
    // TODO(agallego) - add target_replication_factor,
    // current_replication_factor to group_configuration so we can promote
    // learners to nodes and perform data movement to added replicas
}

void consensus::maybe_promote_to_voter(vnode id) {
    (void)ss::with_gate(_bg, [this, id] {
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

        vlog(_ctxlog.trace, "promoting node {} to voter", id);
        return _op_lock.get_units()
          .then([this, id](ss::semaphore_units<> u) mutable {
              auto latest_cfg = _configuration_manager.get_latest();
              latest_cfg.promote_to_voter(id);

              return replicate_configuration(
                std::move(u), std::move(latest_cfg));
          })
          .then([this, id](std::error_code ec) {
              vlog(
                _ctxlog.trace, "node {} promotion result {}", id, ec.message());
          });
    }).handle_exception_type([](const ss::gate_closed_exception&) {});
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
    idx.next_index = details::next_offset(idx.last_dirty_log_index);
    vlog(
      _ctxlog.trace,
      "Updated node {} match {} and next {} indices",
      idx.node_id,
      idx.match_index,
      idx.next_index);
}

bool consensus::needs_recovery(
  const follower_index_metadata& idx, model::offset dirty_offset) {
    // follower match_index is behind, we have to recover it

    return idx.match_index < dirty_offset
           || idx.match_index > idx.last_dirty_log_index;
}

void consensus::dispatch_recovery(follower_index_metadata& idx) {
    auto lstats = _log.offsets();
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
    }
    idx.is_recovering = true;
    // background
    (void)with_gate(_bg, [this, node_id = idx.node_id] {
        auto recovery = std::make_unique<recovery_stm>(
          this, node_id, _scheduling);
        auto ptr = recovery.get();
        return ptr->apply()
          .handle_exception([this, node_id](const std::exception_ptr& e) {
              vlog(_ctxlog.warn, "Node {} recovery failed - {}", node_id, e);
          })
          .finally([r = std::move(recovery)] {});
    }).handle_exception([this](const std::exception_ptr& e) {
        vlog(_ctxlog.warn, "Recovery error - {}", e);
    });
}

ss::future<result<model::offset>> consensus::linearizable_barrier() {
    using ret_t = result<model::offset>;
    struct state_snapshot {
        model::offset linearizable_offset;
        model::term_id term;
    };

    std::optional<ss::semaphore_units<>> u = co_await _op_lock.get_units();

    if (_vstate != vote_state::leader) {
        co_return result<model::offset>(make_error_code(errc::not_leader));
    }
    // store current commit index
    auto cfg = config();
    auto dirty_offset = _log.offsets().dirty_offset;
    /**
     * Dispatch round of heartbeats
     */

    absl::flat_hash_map<vnode, follower_req_seq> sequences;
    std::vector<ss::future<>> send_futures;
    send_futures.reserve(cfg.unique_voter_count());
    cfg.for_each_voter([this, dirty_offset, &sequences, &send_futures](
                         vnode target) {
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
          append_entries_request::flush_after_append::no);
        auto seq = next_follower_sequence(target);
        sequences.emplace(target, seq);

        update_node_append_timestamp(target);
        vlog(
          _ctxlog.trace, "Sending empty append entries request to {}", target);
        auto f
          = _client_protocol
              .append_entries(
                target.id(),
                std::move(req),
                rpc::client_opts(_replicate_append_timeout + clock_type::now()))
              .then([this, id = target.id(), seq, dirty_offset](
                      result<append_entries_reply> reply) {
                  process_append_entries_reply(id, reply, seq, dirty_offset);
              });

        send_futures.push_back(std::move(f));
    });
    // release semaphore
    // snapshot taken under the semaphore
    state_snapshot snapshot{
      .linearizable_offset = _commit_index, .term = _term};
    u.reset();

    // wait for responsens in background
    (void)ss::with_gate(_bg, [futures = std::move(send_futures)]() mutable {
        return ss::when_all_succeed(futures.begin(), futures.end());
    });

    auto majority_sequences_updated = [&cfg, &sequences, this] {
        return cfg.majority([this, &sequences](vnode id) {
            if (id == _self) {
                return true;
            }
            if (auto it = _fstats.find(id); it != _fstats.end()) {
                return it->second.last_received_seq >= sequences[id];
            }
            return false;
        });
    };

    try {
        // we do not hold the lock while waiting
        co_await _follower_reply.wait(
          [this, snapshot, &majority_sequences_updated] {
              return majority_sequences_updated() || _term != snapshot.term;
          });
    } catch (const ss::broken_condition_variable& e) {
        co_return ret_t(make_error_code(errc::shutting_down));
    }

    // term have changed, not longer a leader
    if (snapshot.term != _term) {
        co_return ret_t(make_error_code(errc::not_leader));
    }
    co_return ret_t(snapshot.linearizable_offset);
}

ss::future<result<replicate_result>> chain_stages(replicate_stages stages) {
    return stages.request_enqueued.then_wrapped(
      [f = std::move(stages.replicate_finished)](
        ss::future<> enqueued) mutable {
          if (enqueued.failed()) {
              return enqueued
                .handle_exception([](const std::exception_ptr& e) {
                    vlog(
                      raftlog.debug, "replicate first stage exception - {}", e);
                })
                .then([f = std::move(f)]() mutable {
                    return f.discard_result()
                      .handle_exception([](const std::exception_ptr& e) {
                          vlog(
                            raftlog.debug,
                            "ignoring replicate second stage exception - {}",
                            e);
                      })
                      .then([] {
                          return result<replicate_result>(make_error_code(
                            errc::replicate_first_stage_exception));
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

    if (!is_leader() || unlikely(_transferring_leadership)) {
        return replicate_stages(errc::not_leader);
    }

    if (opts.consistency == consistency_level::quorum_ack) {
        _probe.replicate_requests_ack_all();

        return wrap_stages_with_gate(
          _bg, _batcher.replicate(expected_term, std::move(rdr)));
    }

    if (opts.consistency == consistency_level::leader_ack) {
        _probe.replicate_requests_ack_leader();
    } else {
        _probe.replicate_requests_ack_none();
    }
    // For relaxed consistency, append data to leader disk without flush
    // asynchronous replication is provided by Raft protocol recovery mechanism.

    ss::promise<> enqueued;
    auto enqueued_f = enqueued.get_future();
    using ret_t = result<replicate_result>;

    auto replicated = _op_lock.get_units().then_wrapped(
      [this,
       expected_term,
       enqueued = std::move(enqueued),
       lvl = opts.consistency,
       rdr = std::move(rdr)](ss::future<ss::semaphore_units<>> f) mutable {
          if (!f.failed()) {
              enqueued.set_value();
              return do_append_replicate_relaxed(
                expected_term, std::move(rdr), lvl, f.get());
          }
          enqueued.set_exception(f.get_exception());
          return ss::make_ready_future<ret_t>(errc::leader_append_failed);
      });

    return wrap_stages_with_gate(
      _bg, replicate_stages(std::move(enqueued_f), std::move(replicated)));
}

ss::future<result<replicate_result>> consensus::do_append_replicate_relaxed(
  std::optional<model::term_id> expected_term,
  model::record_batch_reader rdr,
  consistency_level lvl,
  ss::semaphore_units<> u) {
    using ret_t = result<replicate_result>;
    if (!is_leader()) {
        return seastar::make_ready_future<ret_t>(errc::not_leader);
    }

    if (expected_term.has_value() && expected_term.value() != _term) {
        return seastar::make_ready_future<ret_t>(errc::not_leader);
    }
    _last_write_consistency_level = lvl;
    return disk_append(
             model::make_record_batch_reader<details::term_assigning_reader>(
               std::move(rdr), model::term_id(_term)),
             update_last_quorum_index::no)
      .then([this](storage::append_result res) {
          // only update visibility upper bound if all quorum
          // replicated entries are committed already
          if (_commit_index >= _last_quorum_replicated_index) {
              // for relaxed consistency mode update visibility
              // upper bound with last offset appended to the log
              _visibility_upper_bound_index = std::max(
                _visibility_upper_bound_index, res.last_offset);
              maybe_update_majority_replicated_index();
          }
          return ret_t(replicate_result{.last_offset = res.last_offset});
      })
      .finally([this, u = std::move(u)] { _probe.replicate_done(); });
}

ss::future<model::record_batch_reader>
consensus::do_make_reader(storage::log_reader_config config) {
    // limit to last visible index
    config.max_offset = std::min(config.max_offset, last_visible_index());
    return _log.make_reader(config);
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
            details::next_offset(_majority_replicated_index),
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

ss::future<bool> consensus::dispatch_prevote(bool leadership_transfer) {
    auto pvstm_p = std::make_unique<prevote_stm>(this);
    auto pvstm = pvstm_p.get();
    if (leadership_transfer) {
        return ss::make_ready_future<bool>(true);
    }
    return pvstm->prevote(leadership_transfer)
      .then_wrapped([this, pvstm_p = std::move(pvstm_p), pvstm](
                      ss::future<bool> prevote_f) mutable {
          bool ready = false;
          try {
              ready = prevote_f.get();
          } catch (...) {
              vlog(
                _ctxlog.warn,
                "Error returned from prevoting process {}",
                std::current_exception());
          }
          auto f = pvstm->wait().finally([pvstm_p = std::move(pvstm_p)] {});
          // make sure we wait for all futures when gate is closed
          if (_bg.is_closed()) {
              return f.then([ready] { return ready; });
          }
          // background
          (void)with_gate(
            _bg, [pvstm_p = std::move(pvstm_p), f = std::move(f)]() mutable {
                return std::move(f);
            });

          return ss::make_ready_future<bool>(ready);
      });
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

    // skip sending vote request if current node is not a voter
    if (!_configuration_manager.get_latest().is_voter(_self)) {
        arm_vote_timeout();
        return;
    }

    // if priority is to low, skip dispatching votes, do not take priority into
    // account when we transfer leadership
    if (current_priority_to_low && !leadership_transfer) {
        vlog(
          _ctxlog.trace,
          "current node priority {} is lower than target {} (next vote {})",
          self_priority,
          cur_target_priority,
          _target_priority);
        arm_vote_timeout();
        return;
    }
    // background, acquire lock, transition state
    (void)with_gate(_bg, [this, leadership_transfer] {
        return dispatch_prevote(leadership_transfer)
          .then([this, leadership_transfer](bool ready) mutable {
              if (!ready) {
                  return ss::make_ready_future<>();
              }
              auto vstm = std::make_unique<vote_stm>(this);
              auto p = vstm.get();

              // CRITICAL: vote performs locking on behalf of consensus
              return p->vote(leadership_transfer)
                .then_wrapped([this, p, vstm = std::move(vstm)](
                                ss::future<> vote_f) mutable {
                    try {
                        vote_f.get();
                    } catch (...) {
                        vlog(
                          _ctxlog.warn,
                          "Error returned from voting process {}",
                          std::current_exception());
                    }
                    auto f = p->wait().finally([vstm = std::move(vstm)] {});
                    // make sure we wait for all futures when gate is closed
                    if (_bg.is_closed()) {
                        return f;
                    }
                    // background
                    (void)with_gate(
                      _bg,
                      [vstm = std::move(vstm), f = std::move(f)]() mutable {
                          return std::move(f);
                      });

                    return ss::make_ready_future<>();
                });
          })
          .handle_exception([this](const std::exception_ptr& e) {
              vlog(_ctxlog.warn, "Exception thrown while voting - {}", e);
          })
          .finally([this] { arm_vote_timeout(); });
    });
}
void consensus::arm_vote_timeout() {
    if (!_bg.is_closed()) {
        _vote_timeout.rearm(_jit());
    }
}

ss::future<std::error_code>
consensus::update_group_member(model::broker broker) {
    return _op_lock.get_units().then(
      [this, broker = std::move(broker)](ss::semaphore_units<> u) mutable {
          auto cfg = _configuration_manager.get_latest();
          if (!cfg.contains_broker(broker.id())) {
              vlog(
                _ctxlog.warn,
                "Node with id {} does not exists in current configuration");
              return ss::make_ready_future<std::error_code>(
                errc::node_does_not_exists);
          }
          // update broker information
          cfg.update(std::move(broker));

          return replicate_configuration(std::move(u), std::move(cfg));
      });
}

template<typename Func>
ss::future<std::error_code> consensus::change_configuration(Func&& f) {
    return _op_lock.get_units().then(
      [this, f = std::forward<Func>(f)](ss::semaphore_units<> u) mutable {
          auto latest_cfg = config();
          // latest configuration is of joint type
          if (latest_cfg.type() == configuration_type::joint) {
              return ss::make_ready_future<std::error_code>(
                errc::configuration_change_in_progress);
          }

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
      });
}

ss::future<std::error_code> consensus::add_group_members(
  std::vector<model::broker> nodes, model::revision_id new_revision) {
    vlog(_ctxlog.trace, "Adding members: {}", nodes);
    return change_configuration([nodes = std::move(nodes), new_revision](
                                  group_configuration current) mutable {
        auto contains_already = std::any_of(
          std::cbegin(nodes),
          std::cend(nodes),
          [&current](const model::broker& broker) {
              return current.contains_broker(broker.id());
          });

        if (contains_already) {
            return result<group_configuration>(errc::node_already_exists);
        }
        current.set_revision(new_revision);
        current.add(std::move(nodes), new_revision);

        return result<group_configuration>(std::move(current));
    });
}

ss::future<std::error_code> consensus::remove_members(
  std::vector<model::node_id> ids, model::revision_id new_revision) {
    vlog(_ctxlog.trace, "Removing members: {}", ids);
    return change_configuration(
      [ids = std::move(ids), new_revision](group_configuration current) {
          auto all_exists = std::all_of(
            std::cbegin(ids), std::cend(ids), [&current](model::node_id id) {
                return current.contains_broker(id);
            });
          if (!all_exists) {
              return result<group_configuration>(errc::node_does_not_exists);
          }
          current.set_revision(new_revision);
          current.remove(ids);

          if (current.current_config().voters.empty()) {
              return result<group_configuration>(
                errc::invalid_configuration_update);
          }
          return result<group_configuration>(std::move(current));
      });
}

ss::future<std::error_code> consensus::replace_configuration(
  std::vector<model::broker> new_brokers, model::revision_id new_revision) {
    return change_configuration(
      [new_brokers = std::move(new_brokers),
       new_revision](group_configuration current) mutable {
          current.replace(std::move(new_brokers), new_revision);
          current.set_revision(new_revision);
          return result<group_configuration>(std::move(current));
      });
}

ss::future<> consensus::start() {
    return ss::try_with_gate(_bg, [this] { return do_start(); });
}

ss::future<> consensus::do_start() {
    vlog(_ctxlog.info, "Starting");
    return _op_lock.with([this] {
        read_voted_for();

        /*
         * temporary workaround:
         *
         * if the group's ntp matches the pattern, then do not load the initial
         * configuration snapshto from the keyvalue store. more info here:
         *
         * https://github.com/vectorizedio/redpanda/issues/1870
         */
        const auto& ntp = _log.config().ntp();
        const auto normalized_ntp = fmt::format(
          "{}.{}.{}", ntp.ns(), ntp.tp.topic(), ntp.tp.partition());
        const auto& patterns = config::shard_local_cfg()
                                 .full_raft_configuration_recovery_pattern();
        auto initial_state = std::any_of(
          patterns.cbegin(),
          patterns.cend(),
          [&normalized_ntp](const ss::sstring& pattern) {
              return pattern == "*" || normalized_ntp.starts_with(pattern);
          });
        if (!initial_state) {
            initial_state = is_initial_state();
        }

        vlog(
          _ctxlog.info,
          "Starting with voted_for {} term {} initial_state {}",
          _voted_for,
          _term,
          initial_state);

        return _configuration_manager.start(initial_state, _self.revision())
          .then([this] {
              vlog(
                _ctxlog.trace,
                "Configuration manager started: {}",
                _configuration_manager);
          })
          .then([this, initial_state] {
              offset_translator::must_reset must_reset{initial_state};

              absl::btree_map<model::offset, int64_t> offset2delta;
              for (auto it = _configuration_manager.begin();
                   it != _configuration_manager.end();
                   ++it) {
                  offset2delta.emplace(it->first, it->second.idx());
              }

              auto bootstrap = offset_translator::bootstrap_state{
                .offset2delta = std::move(offset2delta),
                .highest_known_offset
                = _configuration_manager.get_highest_known_offset(),
              };

              return _offset_translator->start(
                must_reset, std::move(bootstrap));
          })
          .then([this] { return hydrate_snapshot(); })
          .then([this] {
              vlog(
                _ctxlog.debug,
                "Starting raft bootstrap from {}",
                _configuration_manager.get_highest_known_offset());
              return details::read_bootstrap_state(
                _log,
                details::next_offset(
                  _configuration_manager.get_highest_known_offset()),
                _as);
          })
          .then([this](configuration_bootstrap_state st) {
              auto lstats = _log.offsets();

              vlog(_ctxlog.info, "Read bootstrap state: {}", st);
              vlog(_ctxlog.info, "Current log offsets: {}", lstats);

              // if log term is newer than the one comming from voted_for state,
              // we reset voted_for state
              if (lstats.dirty_offset_term > _term) {
                  _term = lstats.dirty_offset_term;
                  _voted_for = {};
              }
              /**
               * since we are starting, there were no new writes to the log
               * before that point. It is safe to use dirty offset as a initial
               * flushed offset since it is equal to last offset that exists on
               * disk and was read in log recovery process.
               */
              _flushed_offset = lstats.dirty_offset;
              /**
               * The configuration manager state may be divereged from the log
               * state, as log is flushed lazily, we have to make sure that the
               * log and configuration manager has exactly the same offsets
               * range
               */
              vlog(
                _ctxlog.info,
                "Truncating configurations at {}",
                lstats.dirty_offset);

              auto f = _configuration_manager.truncate(
                details::next_offset(lstats.dirty_offset));

              /**
               * We read some batches from the log and have to update the
               * configuration manager.
               */
              if (st.config_batches_seen() > 0) {
                  f = f.then([this, st = std::move(st)]() mutable {
                      return _configuration_manager.add(
                        std::move(st).release_configurations());
                  });
              }

              return f.then([this] {
                  update_follower_stats(_configuration_manager.get_latest());
              });
          })
          .then([this] { return _offset_translator->sync_with_log(_log, _as); })
          .then([this] {
              /**
               * fix for incorrectly persisted configuration index. In previous
               * version of redpanda due to the issue with incorrectly assigned
               * raft configuration indicies
               * (https://github.com/vectorizedio/redpanda/issues/2326) there
               * may be a persistent corruption in offset translation caused by
               * incorrectly persited configuration index. It may cause log
               * offset to be negative. Here we check if this problem exists and
               * if so apply necessary offset translation.
               */
              const auto so = start_offset();
              // no prefix truncation was applied we do not need adjustment
              if (so <= model::offset(0)) {
                  return ss::now();
              }
              auto delta = _configuration_manager.offset_delta(start_offset());

              if (so >= delta) {
                  return ss::now();
              }
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
              return _configuration_manager.adjust_configuration_idx(new_idx);
          })
          .then([this] {
              auto next_election = clock_type::now();
              // set last heartbeat timestamp to prevent skipping first
              // election
              _hbeat = clock_type::time_point::min();
              auto conf = _configuration_manager.get_latest().brokers();
              if (!conf.empty() && _self.id() == conf.begin()->id()) {
                  // for single node scenarios arm immediate election,
                  // use standard election timeout otherwise.
                  if (conf.size() > 1) {
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
          })
          .then([this] {
              auto last_applied = read_last_applied();
              if (last_applied > _commit_index) {
                  _commit_index = last_applied;
                  maybe_update_last_visible_index(_commit_index);
                  vlog(
                    _ctxlog.trace, "Recovered commit_index: {}", _commit_index);
              }
          })
          .then([this] {
              start_dispatching_disk_append_events();
              return _event_manager.start();
          })
          .then([this] { _append_requests_buffer.start(); })
          .then([this] {
              vlog(
                _ctxlog.info,
                "started raft, log offsets: {}, term: {}, configuration: {}",
                _log.offsets(),
                _term,
                _configuration_manager.get_latest());
          });
    });
}

void consensus::start_dispatching_disk_append_events() {
    // forward disk appends to follower state changed condition
    // variables
    (void)ss::with_gate(_bg, [this] {
        return ss::do_until(
          [this] { return _as.abort_requested(); },
          [this] {
              return _disk_append.wait()
                .then([this] {
                    for (auto& idx : _fstats) {
                        idx.second.follower_state_change.broadcast();
                    }
                })
                .handle_exception_type(
                  [this](const ss::broken_condition_variable&) {
                      for (auto& idx : _fstats) {
                          idx.second.follower_state_change.broken();
                      }
                  });
          });
    });
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
    auto const limited_offset = std::min(o, _log.offsets().committed_offset);
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
                return do_vote(std::move(r));
            })
          .handle_exception_type(
            [this, target_node_id](const ss::semaphore_timed_out&) {
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
      _log.config().ntp());

    return _last_snapshot_term;
}

ss::future<vote_reply> consensus::do_vote(vote_request&& r) {
    vote_reply reply;
    reply.term = _term;
    reply.target_node_id = r.node_id;
    auto lstats = _log.offsets();
    auto last_log_index = lstats.dirty_offset;
    _probe.vote_request();
    auto last_entry_term = get_last_entry_term(lstats);
    vlog(_ctxlog.trace, "Vote request: {}", r);

    if (unlikely(is_request_target_node_invalid("vote", r))) {
        reply.log_ok = false;
        reply.granted = false;
        return ss::make_ready_future<vote_reply>(reply);
    }

    // Optimization: for vote requests from nodes that are likely
    // to have been recently restarted (have failed heartbeats
    // and an <= present term), reset their RPC backoff to get
    // a heartbeat sent out sooner.
    if (is_leader() and r.term <= _term) {
        // Look up follower stats for the requester
        if (auto it = _fstats.find(r.node_id); it != _fstats.end()) {
            auto& fstats = it->second;
            if (fstats.heartbeats_failed) {
                vlog(
                  _ctxlog.debug,
                  "Vote request from peer {} with heartbeat failures, "
                  "resetting backoff",
                  r.node_id);
                return _client_protocol.reset_backoff(r.node_id.id())
                  .then([reply]() {
                      return ss::make_ready_future<vote_reply>(reply);
                  });
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
        return ss::make_ready_future<vote_reply>(std::move(reply));
    }

    /// set to true if the caller's log is as up to date as the recipient's
    /// - extension on raft. see Diego's phd dissertation, section 9.6
    /// - "Preventing disruptions when a server rejoins the cluster"
    reply.log_ok
      = r.prev_log_term > last_entry_term
        || (r.prev_log_term == last_entry_term && r.prev_log_index >= last_log_index);

    // raft.pdf: reply false if term < currentTerm (§5.1)
    if (r.term < _term) {
        return ss::make_ready_future<vote_reply>(std::move(reply));
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
        return ss::make_ready_future<vote_reply>(reply);
    }

    if (r.term > _term) {
        vlog(
          _ctxlog.info,
          "Received vote request with larger term from node {}, received "
          "{}, "
          "current {}",
          r.node_id,
          r.term,
          _term);
        reply.term = r.term;
        _term = r.term;
        _voted_for = {};
        do_step_down();
        if (_leader_id) {
            _leader_id = std::nullopt;
            trigger_leadership_notification();
        }

        // do not grant vote if log isn't ok
        if (!reply.log_ok) {
            // even tough we step down we do not want to update the hbeat as it
            // would cause subsequent votes to fail (_hbeat is updated by the
            // leader)
            _hbeat = clock_type::time_point::min();
            return ss::make_ready_future<vote_reply>(reply);
        }
    }

    // do not grant vote if log isn't ok
    if (!reply.log_ok) {
        return ss::make_ready_future<vote_reply>(reply);
    }

    if (_voted_for.id()() < 0) {
        return write_voted_for({r.node_id, _term})
          .then_wrapped([this, reply = std::move(reply), r = std::move(r)](
                          ss::future<> f) mutable {
              bool granted = false;

              if (f.failed()) {
                  vlog(
                    _ctxlog.warn,
                    "Unable to persist raft group state, vote not granted "
                    "- {}",
                    f.get_exception());
              } else {
                  _voted_for = r.node_id;
                  _hbeat = clock_type::now();
                  granted = true;
              }

              reply.granted = granted;

              return ss::make_ready_future<vote_reply>(std::move(reply));
          });
    } else {
        reply.granted = (r.node_id == _voted_for);
        if (reply.granted) {
            _hbeat = clock_type::now();
        }
        return ss::make_ready_future<vote_reply>(std::move(reply));
    }
}

ss::future<append_entries_reply>
consensus::append_entries(append_entries_request&& r) {
    return with_gate(_bg, [this, r = std::move(r)]() mutable {
        return _append_requests_buffer.enqueue(std::move(r));
    });
}

ss::future<append_entries_reply>
consensus::do_append_entries(append_entries_request&& r) {
    auto lstats = _log.offsets();
    append_entries_reply reply;
    reply.node_id = _self;
    reply.target_node_id = r.node_id;
    reply.group = r.meta.group;
    reply.term = _term;
    reply.last_dirty_log_index = lstats.dirty_offset;
    reply.last_flushed_log_index = _flushed_offset;
    reply.result = append_entries_reply::status::failure;
    _probe.append_request();

    if (unlikely(is_request_target_node_invalid("append_entries", r))) {
        return ss::make_ready_future<append_entries_reply>(reply);
    }
    // no need to trigger timeout
    vlog(_ctxlog.trace, "Append entries request: {}", r.meta);

    // raft.pdf: Reply false if term < currentTerm (§5.1)
    if (r.meta.term < _term) {
        reply.result = append_entries_reply::status::failure;
        return ss::make_ready_future<append_entries_reply>(std::move(reply));
    }
    /**
     * When the current leader is alive, whenever a follower receives heartbeat,
     * it updates its target priority to the initial value
     */
    _target_priority = voter_priority::max();
    do_step_down();
    if (r.meta.term > _term) {
        vlog(
          _ctxlog.debug,
          "Append entries request term:{} is greater than current: {}. "
          "Setting "
          "new term",
          r.meta.term,
          _term);
        _term = r.meta.term;
        _voted_for = {};
        return do_append_entries(std::move(r));
    }

    // raft.pdf:If AppendEntries RPC received from new leader: convert to
    // follower (§5.2)
    _vstate = vote_state::follower;
    if (unlikely(_leader_id != r.node_id)) {
        _leader_id = r.node_id;
        _follower_reply.broadcast();
        trigger_leadership_notification();
    }

    // raft.pdf: Reply false if log doesn’t contain an entry at
    // prevLogIndex whose term matches prevLogTerm (§5.3)
    // broken into 3 sections

    auto last_log_offset = lstats.dirty_offset;
    // section 1
    // For an entry to fit into our log, it must not leave a gap.
    if (r.meta.prev_log_index > last_log_offset) {
        if (!r.batches.is_end_of_stream()) {
            vlog(
              _ctxlog.debug,
              "Rejecting append entries. Would leave gap in log, last log "
              "offset: {} request previous log offset: {}",
              last_log_offset,
              r.meta.prev_log_index);
        }
        return ss::make_ready_future<append_entries_reply>(std::move(reply));
    }

    // section 2
    // must come from the same term
    // if prev log index from request is the same as current we can use
    // prev_log_term as an optimization
    auto last_log_term
      = lstats.dirty_offset == r.meta.prev_log_index
          ? lstats.dirty_offset_term // use term from lstats
          : get_term(model::offset(
            r.meta.prev_log_index)); // lookup for request term in log
    // We can only check prev_log_term for entries that are present in the
    // log. When leader installed snapshot on the follower we may require to
    // skip the term check as term of prev_log_idx may not be available.
    if (
      r.meta.prev_log_index >= lstats.start_offset
      && r.meta.prev_log_term != last_log_term) {
        vlog(
          _ctxlog.debug,
          "Rejecting append entries. mismatching entry term at offset: "
          "{}, current term: {} request term: {}",
          r.meta.prev_log_index,
          last_log_term,
          r.meta.prev_log_term);
        reply.result = append_entries_reply::status::failure;
        return ss::make_ready_future<append_entries_reply>(std::move(reply));
    }

    // special case heartbeat case
    // we need to handle it early (before executing truncation)
    // as timeouts are asynchronous to append calls and can have stall data
    if (r.batches.is_end_of_stream()) {
        if (r.meta.prev_log_index < last_log_offset) {
            // do not tuncate on heartbeat just response with false
            reply.result = append_entries_reply::status::failure;
            return ss::make_ready_future<append_entries_reply>(
              std::move(reply));
        }
        auto last_visible = std::min(
          lstats.dirty_offset, r.meta.last_visible_index);
        // on the follower leader control visibility of entries in the log
        maybe_update_last_visible_index(last_visible);
        return maybe_update_follower_commit_idx(
                 model::offset(r.meta.commit_index))
          .then([reply = std::move(reply)]() mutable {
              reply.result = append_entries_reply::status::success;
              return ss::make_ready_future<append_entries_reply>(
                std::move(reply));
          });
    }

    // section 3
    if (r.meta.prev_log_index < last_log_offset) {
        if (unlikely(r.meta.prev_log_index < _commit_index)) {
            reply.result = append_entries_reply::status::success;
            vlog(
              _ctxlog.info,
              "Stale append entries request processed, entry is already "
              "present, request: {}, current state: {}",
              r.meta,
              meta());
            return ss::make_ready_future<append_entries_reply>(
              std::move(reply));
        }
        auto truncate_at = details::next_offset(
          model::offset(r.meta.prev_log_index));
        vlog(
          _ctxlog.info,
          "Truncating log in term: {}, Request previous log index: {} is "
          "earlier than log end offset: {}. Truncating to: {}",
          r.meta.term,
          r.meta.prev_log_index,
          lstats.dirty_offset,
          truncate_at);
        _probe.log_truncated();

        // We are truncating the offset translator before truncating the log
        // because if saving offset translator state fails, we will retry and
        // eventually log and offset translator will become consistent. OTOH if
        // log truncation were first and saving offset translator state failed,
        // we wouldn't retry and log and offset translator could diverge.
        return _offset_translator->truncate(truncate_at)
          .then([this, truncate_at] {
              return _log.truncate(storage::truncate_config(
                truncate_at, _scheduling.default_iopc));
          })
          .then([this, truncate_at] {
              _last_quorum_replicated_index = std::min(
                details::prev_offset(truncate_at),
                _last_quorum_replicated_index);
              // update flushed offset since truncation may happen to already
              // flushed entries
              _flushed_offset = std::min(
                details::prev_offset(truncate_at), _flushed_offset);

              return _configuration_manager.truncate(truncate_at).then([this] {
                  _probe.configuration_update();
                  update_follower_stats(_configuration_manager.get_latest());
              });
          })
          .then([this, r = std::move(r), truncate_at]() mutable {
              auto lstats = _log.offsets();
              if (unlikely(lstats.dirty_offset != r.meta.prev_log_index)) {
                  vlog(
                    _ctxlog.error,
                    "Log truncation error, expected offset: {}, log "
                    "offsets: "
                    "{}, requested truncation at {}",
                    r.meta.prev_log_index,
                    lstats,
                    truncate_at);
              }
              return do_append_entries(std::move(r));
          })
          .handle_exception([this, reply = std::move(reply)](
                              const std::exception_ptr& e) mutable {
              vlog(_ctxlog.warn, "Error occurred while truncating log - {}", e);
              reply.result = append_entries_reply::status::failure;
              return ss::make_ready_future<append_entries_reply>(
                std::move(reply));
          });
    }

    // success. copy entries for each subsystem
    using offsets_ret = storage::append_result;
    return disk_append(std::move(r.batches), update_last_quorum_index::no)
      .then([this, m = r.meta, target = r.node_id](offsets_ret ofs) {
          auto f = ss::make_ready_future<>();
          auto last_visible = std::min(ofs.last_offset, m.last_visible_index);
          maybe_update_last_visible_index(last_visible);
          return maybe_update_follower_commit_idx(model::offset(m.commit_index))
            .then([this, ofs, target] {
                return make_append_entries_reply(target, ofs);
            });
      })
      .handle_exception([this, reply = std::move(reply)](
                          const std::exception_ptr& e) mutable {
          vlog(
            _ctxlog.warn, "Error occurred while appending log entries - {}", e);
          reply.result = append_entries_reply::status::failure;
          return ss::make_ready_future<append_entries_reply>(std::move(reply));
      })
      .finally([this] {
          // we do not want to include our disk flush latency into
          // the leader vote timeout
          _hbeat = clock_type::now();
      });
}

ss::future<install_snapshot_reply>
consensus::install_snapshot(install_snapshot_request&& r) {
    return _op_lock.with([this, r = std::move(r)]() mutable {
        return do_install_snapshot(std::move(r));
    });
}

ss::future<> consensus::hydrate_snapshot() {
    // Read snapshot, reset state machine using snapshot contents (and load
    // snapshot’s cluster configuration) (§7.8)
    return _snapshot_mgr.open_snapshot().then(
      [this](std::optional<storage::snapshot_reader> reader) {
          // no snapshot do nothing
          if (!reader) {
              return ss::now();
          }
          return ss::do_with(
            std::move(*reader), [this](storage::snapshot_reader& reader) {
                return do_hydrate_snapshot(reader).finally(
                  [&reader] { return reader.close(); });
            });
      });
}

ss::future<> consensus::truncate_to_latest_snapshot() {
    auto lstats = _log.offsets();
    if (lstats.start_offset > _last_snapshot_index) {
        return ss::now();
    }
    // we have to prefix truncate config manage at exactly last offset included
    // in snapshot as this is the offset of configuration included in snapshot
    // metadata
    return _configuration_manager.prefix_truncate(_last_snapshot_index)
      .then([this] {
          return _offset_translator->prefix_truncate(_last_snapshot_index);
      })
      .then([this] {
          return _log.truncate_prefix(storage::truncate_prefix_config(
            details::next_offset(_last_snapshot_index),
            _scheduling.default_iopc));
      })
      .then([this] {
          // when log was prefix truncate flushed offset should be equal to at
          // least last snapshot index
          _flushed_offset = std::max(_last_snapshot_index, _flushed_offset);
      });
}

ss::future<> consensus::do_hydrate_snapshot(storage::snapshot_reader& reader) {
    return reader.read_metadata().then([this](iobuf buf) {
        auto parser = iobuf_parser(std::move(buf));
        auto metadata = reflection::adl<snapshot_metadata>{}.from(parser);
        vassert(
          metadata.last_included_index >= _last_snapshot_index,
          "Tried to load stale snapshot. Loaded snapshot last "
          "index {}, current snapshot last index {}",
          metadata.last_included_index,
          _last_snapshot_index);

        _last_snapshot_index = metadata.last_included_index;
        _last_snapshot_term = metadata.last_included_term;

        // TODO: add applying snapshot content to state machine
        _commit_index = std::max(_last_snapshot_index, _commit_index);
        maybe_update_last_visible_index(_commit_index);

        update_follower_stats(metadata.latest_configuration);
        return _configuration_manager
          .add(_last_snapshot_index, std::move(metadata.latest_configuration))
          .then([this, delta = metadata.log_start_delta] {
              return _offset_translator->prefix_truncate_reset(
                _last_snapshot_index, delta);
          })
          .then([this] {
              _probe.configuration_update();
              _log.set_collectible_offset(_last_snapshot_index);
              return truncate_to_latest_snapshot();
          });
    });
}

ss::future<install_snapshot_reply>
consensus::do_install_snapshot(install_snapshot_request&& r) {
    vlog(_ctxlog.trace, "Install snapshot request: {}", r);

    install_snapshot_reply reply{
      .term = _term, .bytes_stored = r.chunk.size_bytes(), .success = false};
    reply.target_node_id = r.node_id;

    if (unlikely(is_request_target_node_invalid("install_snapshot", r))) {
        return ss::make_ready_future<install_snapshot_reply>(reply);
    }

    bool is_done = r.done;
    // Raft paper: Reply immediately if term < currentTerm (§7.1)
    if (r.term < _term) {
        return ss::make_ready_future<install_snapshot_reply>(reply);
    }

    // no need to trigger timeout
    _hbeat = clock_type::now();

    // request received from new leader
    if (r.term > _term) {
        _term = r.term;
        _voted_for = {};
        do_step_down();
        return do_install_snapshot(std::move(r));
    }

    auto f = ss::now();
    // Create new snapshot file if first chunk (offset is 0) (§7.2)
    if (r.file_offset == 0) {
        // discard old chunks, previous snaphost wasn't finished
        if (_snapshot_writer) {
            f = _snapshot_writer->close().then(
              [this] { return _snapshot_mgr.remove_partial_snapshots(); });
        }
        f = f.then([this] {
            return _snapshot_mgr.start_snapshot().then(
              [this](storage::snapshot_writer w) {
                  _snapshot_writer.emplace(std::move(w));
              });
        });
    }

    // Write data into snapshot file at given offset (§7.3)
    f = f.then([this, chunk = std::move(r.chunk)]() mutable {
        return write_iobuf_to_output_stream(
          std::move(chunk), _snapshot_writer->output());
    });

    // Reply and wait for more data chunks if done is false (§7.4)
    if (!is_done) {
        return f.then([reply]() mutable {
            reply.success = true;
            return reply;
        });
    }
    // Last chunk, finish storing snapshot
    return f.then([this, r = std::move(r), reply]() mutable {
        return finish_snapshot(std::move(r), reply);
    });
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
    return _op_lock.with([this, cfg = std::move(cfg)]() mutable {
        // do nothing, we already have snapshot for this offset
        // MUST be checked under the _op_lock
        if (cfg.last_included_index <= _last_snapshot_index) {
            return ss::now();
        }
        auto max_offset = cfg.should_truncate ? _commit_index
                                              : last_visible_index();

        vassert(
          cfg.last_included_index <= max_offset,
          "Can not take snapshot, requested offset: {} is greater than max "
          "snapshot offset: {}",
          cfg.last_included_index,
          _commit_index);

        return do_write_snapshot(cfg.last_included_index, std::move(cfg.data))
          .then([this, should_truncate = cfg.should_truncate] {
              if (!should_truncate) {
                  return ss::now();
              }
              return truncate_to_latest_snapshot();
          })
          .then([this] { _log.set_collectible_offset(_last_snapshot_index); });
    });
}

ss::future<>
consensus::do_write_snapshot(model::offset last_included_index, iobuf&& data) {
    vlog(
      _ctxlog.trace,
      "Persisting snapshot with last included offset {} of size {}",
      last_included_index,
      data.size_bytes());

    auto last_included_term = _log.get_term(last_included_index);
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
        _offset_translator->delta(details::next_offset(last_included_index))),
    };

    return details::persist_snapshot(
             _snapshot_mgr, std::move(md), std::move(data))
      .then([this, last_included_index, term = *last_included_term]() mutable {
          // update consensus state
          _last_snapshot_index = last_included_index;
          _last_snapshot_term = term;
          // update configuration manager
          return _configuration_manager.prefix_truncate(_last_snapshot_index)
            .then([this] {
                return _offset_translator->prefix_truncate(
                  _last_snapshot_index);
            });
      });
}

ss::future<std::error_code> consensus::replicate_configuration(
  ss::semaphore_units<> u, group_configuration cfg) {
    // under the _op_sem lock
    if (!is_leader()) {
        return ss::make_ready_future<std::error_code>(errc::not_leader);
    }
    vlog(_ctxlog.debug, "Replicating group configuration {}", cfg);
    return ss::with_gate(
      _bg, [this, u = std::move(u), cfg = std::move(cfg)]() mutable {
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
          std::vector<ss::semaphore_units<>> units;
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

ss::future<result<replicate_result>> consensus::dispatch_replicate(
  append_entries_request req,
  std::vector<ss::semaphore_units<>> u,
  absl::flat_hash_map<vnode, follower_req_seq> seqs) {
    auto stm = ss::make_lw_shared<replicate_entries_stm>(
      this, std::move(req), std::move(seqs));

    return stm->apply(std::move(u)).finally([this, stm] {
        auto f = stm->wait().finally([stm] {});
        // if gate is closed wait for all futures
        if (_bg.is_closed()) {
            _ctxlog.info("gate-closed, waiting to finish background requests");
            return f;
        }
        // background
        (void)with_gate(_bg, [this, stm, f = std::move(f)]() mutable {
            return std::move(f).handle_exception(
              [this](const std::exception_ptr& e) {
                  _ctxlog.error(
                    "Error waiting for background acks to finish - {}", e);
              });
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
    reply.result = append_entries_reply::status::success;
    return reply;
}

ss::future<> consensus::flush_log() {
    _probe.log_flushed();
    auto flushed_up_to = _log.offsets().dirty_offset;
    return _log.flush().then([this, flushed_up_to] {
        _flushed_offset = flushed_up_to;
        // TODO: remove this assertion when we will remove committed_offset
        // from storage.
        auto lstats = _log.offsets();
        vassert(
          lstats.committed_offset >= _flushed_offset,
          "Raft incorrectly tracking flushed log offset. Expected offset: {}, "
          " current log offsets: {}, log: {}",
          _flushed_offset,
          lstats,
          _log);
        _has_pending_flushes = false;
    });
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
      model::timeout_clock::now() + _disk_timeout};

    class consumer {
    public:
        consumer(offset_translator& translator, storage::log_appender appender)
          : _translator(translator)
          , _appender(std::move(appender)) {}

        ss::future<ss::stop_iteration> operator()(model::record_batch& batch) {
            auto ret = co_await _appender(batch);
            // passing batch to translator after appender so that correct batch
            // offsets are filled.
            _translator.process(batch);
            co_return ret;
        }

        auto end_of_stream() { return _appender.end_of_stream(); }

    private:
        offset_translator& _translator;
        storage::log_appender _appender;
    };

    return details::for_each_ref_extract_configuration(
             _log.offsets().dirty_offset,
             std::move(reader),
             consumer(*_offset_translator, _log.make_appender(cfg)),
             cfg.timeout)
      .then([this, should_update_last_quorum_idx](
              std::tuple<ret_t, std::vector<offset_configuration>> t) {
          auto& [ret, configurations] = t;
          if (should_update_last_quorum_idx) {
              /**
               * We have to update last quorum replicated index before we
               * trigger read for followers recovery as recovery_stm will have
               * to deceide if follower flush is required basing on last quorum
               * replicated index.
               */
              _last_quorum_replicated_index = ret.last_offset;
          }
          _disk_append.broadcast();
          _has_pending_flushes = true;
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
              update_follower_stats(configurations.back().cfg);
              f = _configuration_manager.add(std::move(configurations));
          }

          return f.then([this, ret = ret] {
              // if we are already shutting down, do nothing
              if (_bg.is_closed()) {
                  return ret;
              }

              // Do checkpointing in the background to avoid latency spikes in
              // the write path caused by KVStore flush debouncing.

              (void)ss::with_gate(
                _bg, [this] { return _offset_translator->maybe_checkpoint(); });

              (void)ss::with_gate(
                _bg, [this, last_offset = ret.last_offset, sz = ret.byte_size] {
                    return _configuration_manager
                      .maybe_store_highest_known_offset(last_offset, sz);
                });
              return ret;
          });
      });
}

model::term_id consensus::get_term(model::offset o) {
    if (unlikely(o < model::offset(0))) {
        return model::term_id{};
    }
    return _log.get_term(o).value_or(model::term_id{});
}

clock_type::time_point
consensus::last_sent_append_entries_req_timesptamp(vnode id) {
    return _fstats.get(id).last_sent_append_entries_req_timesptamp;
}

void consensus::update_node_append_timestamp(vnode id) {
    if (auto it = _fstats.find(id); it != _fstats.end()) {
        it->second.last_sent_append_entries_req_timesptamp = clock_type::now();
    }
}

void consensus::update_node_hbeat_timestamp(vnode id) {
    _fstats.get(id).last_received_append_entries_reply_timestamp
      = clock_type::now();
}

follower_req_seq consensus::next_follower_sequence(vnode id) {
    if (auto it = _fstats.find(id); it != _fstats.end()) {
        return it->second.last_sent_seq++;
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
    return _op_lock.get_units().then([this](ss::semaphore_units<> u) mutable {
        if (!is_leader()) {
            return ss::now();
        }
        auto f = ss::now();
        if (_has_pending_flushes) {
            f = flush_log();
        }
        return f.then([this, u = std::move(u)]() mutable {
            return do_maybe_update_leader_commit_idx(std::move(u));
        });
    });
}

void consensus::maybe_update_leader_commit_idx() {
    (void)with_gate(_bg, [this] {
        return _op_lock.get_units().then(
          [this](ss::semaphore_units<> u) mutable {
              // do not update committed index if not the leader, this check has
              // to be done under the semaphore
              if (!is_leader()) {
                  return ss::now();
              }
              return do_maybe_update_leader_commit_idx(std::move(u));
          });
    }).handle_exception([this](const std::exception_ptr& e) {
        vlog(_ctxlog.warn, "Error updating leader commit index", e);
    });
}

ss::future<> consensus::maybe_commit_configuration(ss::semaphore_units<> u) {
    // we are not a leader, do nothing
    if (_vstate != vote_state::leader) {
        return ss::now();
    }

    auto latest_offset = _configuration_manager.get_latest_offset();
    // no configurations were committed
    if (latest_offset > _commit_index) {
        return ss::now();
    }

    auto latest_cfg = _configuration_manager.get_latest();
    // as a leader replicate new simple configuration
    if (
      latest_cfg.type() == configuration_type::joint
      && latest_cfg.current_config().learners.empty()) {
        latest_cfg.discard_old_config();
        vlog(
          _ctxlog.trace,
          "leaving joint consensus, new simple configuration {}",
          latest_cfg);
        auto contains_current = latest_cfg.contains(_self);
        return replicate_configuration(std::move(u), std::move(latest_cfg))
          .then([this, contains_current](std::error_code ec) {
              if (ec) {
                  vlog(
                    _ctxlog.error,
                    "unable to replicate simple configuration  - {}",
                    ec);
                  return;
              }
              // leader was removed, step down.
              if (!contains_current) {
                  vlog(
                    _ctxlog.trace,
                    "current node is not longer group member, stepping down");
                  do_step_down();
              }
          });
    }

    return ss::now();
}

ss::future<>
consensus::do_maybe_update_leader_commit_idx(ss::semaphore_units<> u) {
    auto lstats = _log.offsets();
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

    if (
      majority_match > _commit_index
      && _log.get_term(majority_match) == _term) {
        vlog(_ctxlog.trace, "Leader commit index updated {}", majority_match);
        auto old_commit_idx = _commit_index;
        _commit_index = majority_match;
        auto range_start = details::next_offset(model::offset(old_commit_idx));
        vlog(
          _ctxlog.trace,
          "Applying entries from {} to {}",
          range_start,
          _commit_index);
        _commit_index_updated.broadcast();
        _event_manager.notify_commit_index(_commit_index);
        // if we successfully acknowledged all quorum writes we can make pending
        // relaxed consistency requests visible
        if (_commit_index >= _last_quorum_replicated_index) {
            maybe_update_last_visible_index(lstats.dirty_offset);
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
ss::future<>
consensus::maybe_update_follower_commit_idx(model::offset request_commit_idx) {
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
            _commit_index_updated.broadcast();
            _event_manager.notify_commit_index(_commit_index);
        }
    }
    return ss::make_ready_future<>();
}

void consensus::update_follower_stats(const group_configuration& cfg) {
    vlog(_ctxlog.trace, "Updating follower stats with config {}", cfg);
    _fstats.update_with_configuration(cfg);
}

void consensus::trigger_leadership_notification() {
    _probe.leadership_changed();
    vlog(
      _ctxlog.debug,
      "triggering leadership notification with term: {}, new leader: {}",
      _term,
      _leader_id);
    _leader_notification(leadership_status{
      .term = _term, .group = _group, .current_leader = _leader_id});
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
            f = step_down(r.term);
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
    auto err = co_await do_transfer_leadership(req.target);
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
}

/**
 * After we have accepted the request to transfer leadership, carry out
 * preparatory phase before we actually send a timeout_now to the new
 * leader.  During this phase we endeavor to make sure the new leader
 * is sufficiently up to date that they will win the election when
 * they start it.
 */
ss::future<std::error_code>
consensus::prepare_transfer_leadership(vnode target_rni) {
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
      _log.offsets().dirty_offset);

    // Enforce ordering wrt anyone currently doing an append under op_lock
    {
        auto units = co_await _op_lock.get_units();
        vlog(_ctxlog.trace, "transfer leadership: cleared oplock");
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
      && needs_recovery(meta, _log.offsets().dirty_offset)) {
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
          _log.offsets().dirty_offset);
    }

    auto timeout = ss::semaphore::clock::duration(
      config::shard_local_cfg().raft_transfer_leader_recovery_timeout_ms());

    if (meta.is_recovering) {
        vlog(
          _ctxlog.info,
          "transfer leadership: waiting for node {} to catch up",
          target_rni);
        meta.follower_state_change.broadcast();
        try {
            co_await meta.recovery_finished.wait(timeout);
        } catch (const ss::timed_out_error&) {
            vlog(
              _ctxlog.warn,
              "transfer leadership: timed out waiting on node {} "
              "recovery",
              target_rni);
            co_return make_error_code(errc::timeout);
        }
        vlog(
          _ctxlog.warn,
          "transfer leadership: finished waiting on node {} "
          "recovery",
          target_rni);
    } else {
        vlog(
          _ctxlog.debug,
          "transfer leadership: node {} is not recovering, proceeding "
          "(dirty offset {})",
          target_rni,
          _log.offsets().dirty_offset);
    }

    co_return make_error_code(errc::success);
}

ss::future<std::error_code>
consensus::do_transfer_leadership(std::optional<model::node_id> target) {
    if (!is_leader()) {
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
          make_error_code(errc::not_leader));
    }

    auto conf = _configuration_manager.get_latest();
    if (
      _configuration_manager.get_latest_offset() > last_visible_index()
      || conf.type() == configuration_type::joint) {
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

    auto f = ss::with_gate(_bg, [this, target_rni = *target_rni] {
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

        return prepare_transfer_leadership(target_rni)
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
              if (needs_recovery(meta, _log.offsets().dirty_offset)) {
                  vlog(
                    _ctxlog.warn,
                    "Cannot transfer leadership: {} needs recovery ({}, {}, "
                    "{})",
                    target_rni,
                    meta.match_index,
                    meta.last_dirty_log_index,
                    _log.offsets().dirty_offset);

                  return seastar::make_ready_future<std::error_code>(
                    make_error_code(errc::timeout));
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
                        return seastar::make_ready_future<std::error_code>(
                          reply.error());
                    } else {
                        // Step down before setting _transferring_leadership
                        // to false, to ensure we do not accept any more writes
                        // in the gap between new leader acking timeout now
                        // and new leader sending a vote for its new term.
                        // (If we accepted more writes, our log could get
                        //  ahead of new leader, and it could lose election)
                        do_step_down();
                        if (_leader_id) {
                            _leader_id = std::nullopt;
                            trigger_leadership_notification();
                        }

                        return seastar::make_ready_future<std::error_code>(
                          make_error_code(errc::success));
                    }
                });
          });
    });

    return f.finally([this] { _transferring_leadership = false; });
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
    // offset translator
    co_await _offset_translator->remove_persistent_state();
    // snapshot
    co_await _snapshot_mgr.remove_snapshot();
    co_await _snapshot_mgr.remove_partial_snapshots();

    co_return;
}

void consensus::maybe_update_last_visible_index(model::offset offset) {
    _visibility_upper_bound_index = std::max(
      _visibility_upper_bound_index, offset);
    _majority_replicated_index = std::max(_majority_replicated_index, offset);
    _consumable_offset_monitor.notify(last_visible_index());
}

void consensus::maybe_update_majority_replicated_index() {
    auto majority_match = config().quorum_match([this](vnode id) {
        if (id == _self) {
            return _log.offsets().dirty_offset;
        }
        if (auto it = _fstats.find(id); it != _fstats.end()) {
            return it->second.last_dirty_log_index;
        }
        return model::offset{};
    });

    _majority_replicated_index = std::max(
      _majority_replicated_index, majority_match);
    _consumable_offset_monitor.notify(last_visible_index());
}

heartbeats_suppressed consensus::are_heartbeats_suppressed(vnode id) const {
    if (!_fstats.contains(id)) {
        return heartbeats_suppressed::yes;
    }

    return _fstats.get(id).suppress_heartbeats;
}

void consensus::update_suppress_heartbeats(
  vnode id, follower_req_seq last_seq, heartbeats_suppressed suppressed) {
    if (auto it = _fstats.find(id); it != _fstats.end()) {
        /**
         * Since there may be concurrent sources causing heartbeats suppression
         * we use last_suppress_heartbeats_seq to control concurrency of
         * heartbeats state update
         */
        if (last_seq >= it->second.last_suppress_heartbeats_seq) {
            it->second.last_suppress_heartbeats_seq = last_seq;
            it->second.suppress_heartbeats = suppressed;
        }
    }
}

void consensus::update_heartbeat_status(vnode id, bool success) {
    if (auto it = _fstats.find(id); it != _fstats.end()) {
        if (success) {
            it->second.heartbeats_failed = 0;
        } else {
            it->second.heartbeats_failed++;
        }
    }
}

bool consensus::should_reconnect_follower(vnode id) {
    if (_heartbeat_disconnect_failures == 0) {
        // Force disconnection is disabled
        return false;
    }

    if (auto it = _fstats.find(id); it != _fstats.end()) {
        auto last_at = it->second.last_received_append_entries_reply_timestamp;
        const auto fail_count = it->second.heartbeats_failed;

        auto is_live = last_at + _jit.base_duration() > clock_type::now();
        auto since = std::chrono::duration_cast<std::chrono::milliseconds>(
                       clock_type::now() - last_at)
                       .count();
        vlog(
          _ctxlog.trace,
          "should_reconnect_follower({}): {}/{} fails, last ok {}ms ago",
          id,
          fail_count,
          _heartbeat_disconnect_failures,
          since);
        return fail_count > _heartbeat_disconnect_failures && !is_live;
    }

    return false;
}

voter_priority consensus::next_target_priority() {
    return voter_priority(std::max<voter_priority::type>(
      (_target_priority / 5) * 4, min_voter_priority));
}

/**
 * We use simple policy where we calculate priority based on the position of the
 * node in configuration broker vector. We shuffle brokers in raft configuration
 * so it should give us fairly even distribution of leaders across the nodes.
 */
voter_priority consensus::get_node_priority(vnode rni) const {
    auto& latest_cfg = _configuration_manager.get_latest();
    auto& brokers = latest_cfg.brokers();

    auto it = std::find_if(
      brokers.cbegin(), brokers.cend(), [rni](const model::broker& b) {
          return b.id() == rni.id();
      });

    if (it == brokers.cend()) {
        /**
         * If node is not present in current configuration i.e. was added to the
         * cluster, return max, this way for joining node we will use
         * priorityless, classic raft leader election
         */
        return voter_priority::max();
    }

    auto idx = std::distance(brokers.cbegin(), it);

    /**
     * Voter priority is inversly proportion to node position in brokers
     * vector.
     */
    return voter_priority(
      (brokers.size() - idx) * (voter_priority::max() / brokers.size()));
}

model::offset consensus::get_latest_configuration_offset() const {
    return _configuration_manager.get_latest_offset();
}

std::vector<follower_metrics> consensus::get_follower_metrics() const {
    // if not leader return empty vector, as metrics wouldn't have any sense
    if (!is_leader()) {
        return {};
    }
    std::vector<follower_metrics> ret;
    ret.reserve(_fstats.size());
    auto dirty_offset = _log.offsets().dirty_offset;
    for (const auto& f : _fstats) {
        auto last_hbeat = f.second.last_received_append_entries_reply_timestamp;
        auto is_live = last_hbeat + _jit.base_duration() > clock_type::now();
        ret.push_back(follower_metrics{
          .id = f.first.id(),
          .is_learner = f.second.is_learner,
          .committed_log_index = f.second.last_flushed_log_index,
          .dirty_log_index = f.second.last_dirty_log_index,
          .match_index = f.second.match_index,
          .last_heartbeat = last_hbeat,
          .is_live = is_live,
          .under_replicated = (f.second.is_recovering || !is_live)
                              && f.second.match_index < dirty_offset});
    }

    return ret;
}

} // namespace raft
