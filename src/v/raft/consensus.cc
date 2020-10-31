#include "raft/consensus.h"

#include "config/configuration.h"
#include "likely.h"
#include "prometheus/prometheus_sanitize.h"
#include "raft/configuration.h"
#include "raft/consensus_client_protocol.h"
#include "raft/consensus_utils.h"
#include "raft/errc.h"
#include "raft/logger.h"
#include "raft/prevote_stm.h"
#include "raft/recovery_stm.h"
#include "raft/rpc_client_protocol.h"
#include "raft/types.h"
#include "raft/vote_stm.h"
#include "reflection/adl.h"
#include "utils/state_crc_file.h"
#include "utils/state_crc_file_errc.h"
#include "vlog.h"

#include <seastar/core/fstream.hh>
#include <seastar/core/future.hh>

#include <fmt/ostream.h>

#include <iterator>

namespace raft {
consensus::consensus(
  model::node_id nid,
  group_id group,
  group_configuration initial_cfg,
  timeout_jitter jit,
  storage::log l,
  ss::io_priority_class io_priority,
  model::timeout_clock::duration disk_timeout,
  consensus_client_protocol client,
  consensus::leader_cb_t cb,
  storage::api& storage)
  : _self(std::move(nid))
  , _group(group)
  , _jit(std::move(jit))
  , _log(l)
  , _io_priority(io_priority)
  , _disk_timeout(disk_timeout)
  , _client_protocol(client)
  , _leader_notification(std::move(cb))
  , _fstats({})
  , _batcher(this)
  , _event_manager(this)
  , _ctxlog(group, _log.config().ntp())
  , _replicate_append_timeout(
      config::shard_local_cfg().replicate_append_timeout_ms())
  , _recovery_append_timeout(
      config::shard_local_cfg().recovery_append_timeout_ms())
  , _storage(storage)
  , _snapshot_mgr(
      std::filesystem::path(_log.config().work_directory()), _io_priority)
  , _configuration_manager(std::move(initial_cfg), _group, _storage, _ctxlog) {
    setup_metrics();
    update_follower_stats(_configuration_manager.get_latest());
    _vote_timeout.set_callback([this] {
        maybe_step_down();
        dispatch_flush_with_lock();
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
    _voted_for = {};
    _hbeat = clock_type::now();
    _vstate = vote_state::follower;
}

void consensus::maybe_step_down() {
    (void)ss::with_gate(_bg, [this] {
        return _op_lock.with([this] {
            if (_vstate == vote_state::leader) {
                auto majority_hbeat = config().quorum_match(
                  [this](model::node_id id) {
                      if (id == _self) {
                          return clock_type::now();
                      }

                      return _fstats.get(id).last_hschlag_timestamp;
                  });

                if (majority_hbeat < _became_leader_at) {
                    majority_hbeat = _became_leader_at;
                }

                if (
                  majority_hbeat + _jit.base_duration() < clock_type::now()) {
                    do_step_down();
                }
            }
        });
    });
}

ss::future<> consensus::stop() {
    vlog(_ctxlog.info, "Stopping");
    _vote_timeout.cancel();
    _as.request_abort();
    _commit_index_updated.broken();

    return _event_manager.stop()
      .then([this] { return _bg.close(); })
      .then([this] { return _batcher.stop(); })
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
  model::node_id node, result<append_entries_reply> r, follower_req_seq seq) {
    // do not process replies when stoping
    if (unlikely(_as.abort_requested())) {
        return success_reply::no;
    }

    if (!r) {
        vlog(
          _ctxlog.trace,
          "Error append entries response from {}, {}",
          node,
          r.error().message());
        // add stats to group
        return success_reply::no;
    }

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

    follower_index_metadata& idx = _fstats.get(node);
    append_entries_reply& reply = r.value();
    vlog(_ctxlog.trace, "Append entries response: {}", reply);
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
    if (seq < idx.last_received_seq) {
        vlog(
          _ctxlog.trace,
          "ignorring reordered reply from node {} - last: {} current: {} ",
          reply.node_id,
          idx.last_received_seq,
          seq);
        return success_reply::no;
    }
    // only update for in order sequences
    idx.last_received_seq = seq;
    // check preconditions for processing the reply
    if (!is_leader()) {
        vlog(_ctxlog.debug, "ignorring append entries reply, not leader");
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

    update_node_hschlag_timestamp(node);

    // If recovery is in progress the recovery STM will handle follower index
    // updates
    if (!idx.is_recovering) {
        vlog(
          _ctxlog.trace,
          "Updated node {} last committed log index: {}",
          idx.node_id,
          reply.last_committed_log_index);
        idx.last_dirty_log_index = reply.last_dirty_log_index;
        idx.last_committed_log_index = reply.last_committed_log_index;
        idx.next_index = details::next_offset(idx.last_dirty_log_index);
    }

    if (reply.result == append_entries_reply::status::success) {
        successfull_append_entries_reply(idx, std::move(reply));
        return success_reply::yes;
    }

    if (idx.is_recovering) {
        // we are already recovering, do nothing
        return success_reply::no;
    }

    if (needs_recovery(idx)) {
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

void consensus::process_append_entries_reply(
  model::node_id node,
  result<append_entries_reply> r,
  follower_req_seq seq_id) {
    auto is_success = update_follower_index(node, std::move(r), seq_id);
    if (is_success) {
        maybe_update_leader_commit_idx();
    }
}

void consensus::successfull_append_entries_reply(
  follower_index_metadata& idx, append_entries_reply reply) {
    // follower and leader logs matches
    idx.last_dirty_log_index = reply.last_dirty_log_index;
    idx.last_committed_log_index = reply.last_committed_log_index;
    idx.match_index = idx.last_dirty_log_index;
    idx.next_index = details::next_offset(idx.last_dirty_log_index);
    vlog(
      _ctxlog.trace,
      "Updated node {} match {} and next {} indicies",
      idx.node_id,
      idx.match_index,
      idx.next_index);
}

bool consensus::needs_recovery(const follower_index_metadata& idx) {
    // follower match_index is behind, we have to recover it
    auto lstats = _log.offsets();
    return idx.match_index < lstats.dirty_offset
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
    (void)with_gate(_bg, [this, &idx] {
        auto recovery = std::make_unique<recovery_stm>(
          this, idx.node_id, _io_priority);
        auto ptr = recovery.get();
        return ptr->apply()
          .handle_exception([this, &idx](const std::exception_ptr& e) {
              vlog(
                _ctxlog.warn, "Node {} recovery failed - {}", idx.node_id, e);
          })
          .finally([r = std::move(recovery)] {});
    }).handle_exception([this](const std::exception_ptr& e) {
        vlog(_ctxlog.warn, "Recovery error - {}", e);
    });
}

ss::future<result<replicate_result>>
consensus::replicate(model::record_batch_reader&& rdr, replicate_options opts) {
    if (!is_leader() || unlikely(_transferring_leadership)) {
        return seastar::make_ready_future<result<replicate_result>>(
          errc::not_leader);
    }

    _last_replicate_consistency = opts.consistency;
    if (opts.consistency == consistency_level::quorum_ack) {
        _probe.replicate_requests_ack_all();
        return ss::with_gate(_bg, [this, rdr = std::move(rdr)]() mutable {
            return _batcher.replicate(std::move(rdr)).finally([this] {
                _probe.replicate_done();
            });
        });
    }

    if (opts.consistency == consistency_level::leader_ack) {
        _probe.replicate_requests_ack_leader();
    } else {
        _probe.replicate_requests_ack_none();
    }
    // For relaxed consistency, append data to leader disk without flush
    // asynchronous replication is provided by Raft protocol recovery mechanism.
    return _op_lock
      .with([this, rdr = std::move(rdr)]() mutable {
          if (!is_leader()) {
              return seastar::make_ready_future<result<replicate_result>>(
                errc::not_leader);
          }

          return disk_append(model::make_record_batch_reader<
                               details::term_assigning_reader>(
                               std::move(rdr), model::term_id(_term)))
            .then([](storage::append_result res) {
                return result<replicate_result>(
                  replicate_result{.last_offset = res.last_offset});
            });
      })
      .finally([this] { _probe.replicate_done(); });
}

void consensus::dispatch_flush_with_lock() {
    if (!_has_pending_flushes) {
        return;
    }
    (void)ss::with_gate(_bg, [this] {
        return _op_lock.with([this] {
            if (!_has_pending_flushes) {
                return ss::make_ready_future<>();
            }
            return flush_log();
        });
    });
}

model::offset consensus::last_stable_offset() const {
    // for quroum acks level, limit reads to fully replicated entries.
    if (_last_replicate_consistency == consistency_level::quorum_ack) {
        return _commit_index;
    }
    // for relaxed consistency we can read up to commited offset
    return _log.offsets().committed_offset;
}

ss::future<model::record_batch_reader>
consensus::make_reader(storage::log_reader_config config) {
    auto lstats = _log.offsets();
    // for quroum acks level, limit reads to fully replicated entries. the
    // covered offset range is guranteed to already be flushed and visible so we
    // can build the reader immediately.
    if (_last_replicate_consistency == consistency_level::quorum_ack) {
        config.max_offset = std::min(
          config.max_offset, model::offset(_commit_index));
        return _log.make_reader(config);
    }

    // at relaxed consistency / safety levels we can read immediately if there
    // is no pending writes or we'll read part of the log from an area that
    // requires no flushing then build the reader immediately. in the later
    // case, the intention is that the reader will either see the data because
    // the pending data was flushed before the read made it to that non-flushed
    // region or the reader will enounter the end of log adn the reader will
    // flush and retry, making progress.
    if (!_has_pending_flushes || config.start_offset <= lstats.dirty_offset) {
        config.max_offset = std::min(config.max_offset, lstats.dirty_offset);
        return _log.make_reader(config);
    }

    // otherwise flush the log to make pending writes visible
    return _op_lock.with([this, config] {
        auto f = ss::make_ready_future<>();
        if (_has_pending_flushes) {
            f = flush_log();
        }
        return f.then([this, config = config]() mutable {
            auto lstats = _log.offsets();
            config.max_offset = std::min(
              config.max_offset, lstats.dirty_offset);
            return _log.make_reader(config);
        });
    });
}

bool consensus::should_skip_vote(bool ignore_heartbeat) {
    bool skip_vote = false;

    if (likely(!ignore_heartbeat)) {
        auto last_election = clock_type::now() - _jit.base_duration();
        skip_vote |= (_hbeat > last_election); // nothing to do.
    }

    skip_vote |= _vstate == vote_state::leader; // already a leader
    skip_vote |= !_configuration_manager.get_latest().is_voter(
      _self); // not a voter

    return skip_vote;
}

ss::future<bool> consensus::dispatch_prevote(bool leadership_transfer) {
    auto pvstm_p = std::make_unique<prevote_stm>(this);
    auto pvstm = pvstm_p.get();

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
          auto latest_cfg = _configuration_manager.get_latest();
          // latest configuration is of joint type
          if (latest_cfg.type() == configuration_type::joint) {
              return ss::make_ready_future<std::error_code>(
                errc::configuration_change_in_progress);
          }

          result<group_configuration> res = f(std::move(latest_cfg));
          if (res) {
              return replicate_configuration(
                std::move(u), std::move(res.value()));
          }
          return ss::make_ready_future<std::error_code>(res.error());
      });
}

ss::future<std::error_code>
consensus::add_group_members(std::vector<model::broker> nodes) {
    vlog(_ctxlog.trace, "Adding members: {}", nodes);
    return change_configuration(
      [nodes = std::move(nodes)](group_configuration current) mutable {
          auto contains_already = std::any_of(
            std::cbegin(nodes),
            std::cend(nodes),
            [&current](const model::broker& broker) {
                return current.contains_broker(broker.id());
            });

          if (contains_already) {
              return result<group_configuration>(errc::node_already_exists);
          }

          current.add(std::move(nodes));

          return result<group_configuration>(std::move(current));
      });
}

ss::future<std::error_code>
consensus::remove_members(std::vector<model::node_id> ids) {
    vlog(_ctxlog.trace, "Removing members: {}", ids);
    return change_configuration(
      [ids = std::move(ids)](group_configuration current) {
          auto all_exists = std::all_of(
            std::cbegin(ids), std::cend(ids), [&current](model::node_id id) {
                return current.contains_broker(id);
            });
          if (!all_exists) {
              return result<group_configuration>(errc::node_does_not_exists);
          }
          current.remove(ids);

          if (current.current_config().voters.empty()) {
              return result<group_configuration>(
                errc::invalid_configuration_update);
          }
          return result<group_configuration>(std::move(current));
      });
}

ss::future<std::error_code>
consensus::replace_configuration(std::vector<model::broker> new_brokers) {
    return change_configuration([new_brokers = std::move(new_brokers)](
                                  group_configuration current) mutable {
        current.replace(std::move(new_brokers));
        return result<group_configuration>(std::move(current));
    });
}

ss::future<> consensus::start() {
    vlog(_ctxlog.info, "Starting");
    return _op_lock.with([this] {
        read_voted_for();
        return _configuration_manager.start()
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
              // if log term is newer than the one comming from voted_for state,
              // we reset voted_for state
              if (lstats.dirty_offset_term > _term) {
                  _term = lstats.dirty_offset_term;
                  _voted_for = {};
              }
              vlog(
                _ctxlog.info,
                "Recovered, log offsets: {}, term:{}",
                lstats,
                _term);
              /**
               * The configuration manager state may be divereged from the log
               * state, as log is flushed lazily, we have to make sure that the
               * log and configuration manager has exactly the same offsets
               * range
               */
              auto f = _configuration_manager.truncate(
                details::next_offset(lstats.dirty_offset));

              /**
               * We read some batches from the log and have to update the
               * configuration manager.
               */
              if (st.config_batches_seen() > 0) {
                  f = f.then([this, st = std::move(st)]() mutable {
                      return _configuration_manager.add(
                        st.prev_log_index(), st.release_config());
                  });
              }

              return f.then([this] {
                  update_follower_stats(_configuration_manager.get_latest());
              });
          })
          .then([this] {
              auto next_election = clock_type::now();
              // set last heartbeat timestamp to prevent skipping first
              // election
              _hbeat = clock_type::time_point::min();
              auto conf = _configuration_manager.get_latest().brokers();
              if (!conf.empty() && _self == conf.begin()->id()) {
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
                  vlog(
                    _ctxlog.trace, "Recovered commit_index: {}", _commit_index);
              }
          })
          .then([this] { return _event_manager.start(); });
    });
}

bytes consensus::voted_for_key() const {
    iobuf buf;
    reflection::serialize(buf, metadata_key::voted_for, _group);
    return iobuf_to_bytes(buf);
}

ss::future<>
consensus::write_voted_for(consensus::voted_for_configuration config) {
    auto key = voted_for_key();
    iobuf val = reflection::to_iobuf(config);
    return _storage.kvs().put(
      storage::kvstore::key_space::consensus, std::move(key), std::move(val));
}

bytes consensus::last_applied_key() const {
    iobuf buf;
    reflection::serialize(buf, metadata_key::last_applied_offset, _group);
    return iobuf_to_bytes(buf);
}

ss::future<> consensus::write_last_applied(model::offset o) {
    auto key = last_applied_key();
    iobuf val = reflection::to_iobuf(o);
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
    _voted_for = model::node_id{};
    _term = model::term_id(0);

    /*
     * decode the metadata from the key-value store, and delete the old
     * voted_for file, if it exists.
     */
    const auto key = voted_for_key();
    auto value = _storage.kvs().get(
      storage::kvstore::key_space::consensus, key);
    if (value) {
        auto config = reflection::adl<consensus::voted_for_configuration>{}
                        .from(std::move(*value));

        _voted_for = config.voted_for;
        _term = config.term;

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
        return _op_lock.with(
          [this, r = std::move(r)]() mutable { return do_vote(std::move(r)); });
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
      "offset. Log offsets: {}. Last snapshot index: {}",
      lstats,
      _last_snapshot_index);

    return _last_snapshot_term;
}

ss::future<vote_reply> consensus::do_vote(vote_request&& r) {
    vote_reply reply;
    reply.term = _term;
    auto lstats = _log.offsets();
    auto last_log_index = lstats.dirty_offset;
    _probe.vote_request();
    auto last_entry_term = get_last_entry_term(lstats);
    vlog(_ctxlog.trace, "Vote request: {}", r);
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
    if (_hbeat > prev_election && !r.leadership_transfer) {
        vlog(
          _ctxlog.trace,
          "Already heard from the leader, not granting vote to node {}",
          r.node_id);
        reply.granted = false;
        return ss::make_ready_future<vote_reply>(std::move(reply));
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
        _term = r.term;
        reply.term = _term;
        do_step_down();
        // even tough we step down we do not want to update the hbeat as it
        // would cause subsequent votes to fail (_hbeat is updated by the
        // leader)
        _hbeat = clock_type::time_point::min();
    }

    // do not grant vote if log isn't ok
    if (!reply.log_ok) {
        return ss::make_ready_future<vote_reply>(reply);
    }

    auto f = ss::make_ready_future<>();

    if (_voted_for() < 0) {
        _voted_for = model::node_id(r.node_id);
        vlog(_ctxlog.trace, "Voting for {} in term {}", r.node_id, _term);
        f = f.then([this] {
            return write_voted_for({_voted_for, _term})
              .handle_exception([this](const std::exception_ptr& e) {
                  vlog(
                    _ctxlog.warn,
                    "Unable to persist raft group state, vote not granted "
                    "- {}",
                    e);
                  _voted_for = {};
              });
        });
    }

    // vote for the same term, same server_id
    reply.granted = (r.node_id == _voted_for);
    if (reply.granted) {
        _hbeat = clock_type::now();
    }

    return f.then([reply = std::move(reply)] {
        return ss::make_ready_future<vote_reply>(std::move(reply));
    });
}

ss::future<append_entries_reply>
consensus::append_entries(append_entries_request&& r) {
    return with_gate(_bg, [this, r = std::move(r)]() mutable {
        return _op_lock.with([this, r = std::move(r)]() mutable {
            return do_append_entries(std::move(r));
        });
    });
}

ss::future<append_entries_reply>
consensus::do_append_entries(append_entries_request&& r) {
    auto lstats = _log.offsets();
    append_entries_reply reply;
    reply.node_id = _self;
    reply.group = r.meta.group;
    reply.term = _term;
    reply.last_dirty_log_index = lstats.dirty_offset;
    reply.last_committed_log_index = lstats.committed_offset;
    reply.result = append_entries_reply::status::failure;
    _probe.append_request();

    // no need to trigger timeout
    vlog(_ctxlog.trace, "Append entries request: {}", r.meta);

    // raft.pdf: Reply false if term < currentTerm (§5.1)
    if (r.meta.term < _term) {
        reply.result = append_entries_reply::status::failure;
        return ss::make_ready_future<append_entries_reply>(std::move(reply));
    }

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
        return do_append_entries(std::move(r));
    }

    // raft.pdf:If AppendEntries RPC received from new leader: convert to
    // follower (§5.2)
    _vstate = vote_state::follower;
    if (unlikely(_leader_id != r.node_id)) {
        _leader_id = r.node_id;
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
          "Rejecting append entries. missmatching entry term at offset: "
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
              "present");
            return ss::make_ready_future<append_entries_reply>(
              std::move(reply));
        }
        auto truncate_at = details::next_offset(
          model::offset(r.meta.prev_log_index));
        vlog(
          _ctxlog.debug,
          "Truncate log, request for the same term:{}. Request offset:{} "
          "is "
          "earlier than what we have:{}. Truncating to: {}",
          r.meta.term,
          r.meta.prev_log_index,
          lstats.dirty_offset,
          truncate_at);
        _probe.log_truncated();
        return _log
          .truncate(storage::truncate_config(truncate_at, _io_priority))
          .then([this, truncate_at] {
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
    return disk_append(std::move(r.batches))
      .then([this, m = r.meta, flush = r.flush](offsets_ret ofs) mutable {
          auto f = ss::make_ready_future<>();
          if (flush) {
              f = f.then([this] { return flush_log(); });
          }

          return f.then(
            [this, m = std::move(m), ofs = std::move(ofs)]() mutable {
                return maybe_update_follower_commit_idx(
                         model::offset(m.commit_index))
                  .then([this, ofs = std::move(ofs)]() mutable {
                      return make_append_entries_reply(std::move(ofs));
                  });
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
          return _log.truncate_prefix(storage::truncate_prefix_config(
            details::next_offset(_last_snapshot_index), _io_priority));
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

        update_follower_stats(metadata.latest_configuration);
        return _configuration_manager
          .add(_last_snapshot_index, std::move(metadata.latest_configuration))
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
    vassert(
      last_included_index <= _commit_index,
      "Can not take snapshot that contains not commited batches, requested "
      "offset: {}, commit_index: {}",
      last_included_index,
      _commit_index);
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
    };

    return details::persist_snapshot(
             _snapshot_mgr, std::move(md), std::move(data))
      .then([this,
             last_included_index,
             term = *last_included_term,
             cfg = std::move(*config)]() mutable {
          // update consensus state
          _last_snapshot_index = last_included_index;
          _last_snapshot_term = term;
          // update configuration manager
          return _configuration_manager
            .add(_last_snapshot_index, std::move(cfg))
            .then([this] {
                return _configuration_manager.prefix_truncate(
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
           * We use replicate_batcher::do_flush directly as we already hold the
           * _op_lock mutex when replicating configuration
           */
          return _batcher
            .do_flush({}, std::move(req), std::move(u), std::move(seqs))
            .then([] { return std::error_code(errc::success); });
      });
}

append_entries_reply
consensus::make_append_entries_reply(storage::append_result disk_results) {
    auto lstats = _log.offsets();
    append_entries_reply reply;
    reply.node_id = _self;
    reply.group = _group;
    reply.term = _term;
    reply.last_dirty_log_index = disk_results.last_offset;
    reply.last_committed_log_index = lstats.committed_offset;
    reply.result = append_entries_reply::status::success;
    return reply;
}

ss::future<> consensus::flush_log() {
    _probe.log_flushed();
    return _log.flush().then([this] { _has_pending_flushes = false; });
}

ss::future<storage::append_result>
consensus::disk_append(model::record_batch_reader&& reader) {
    using ret_t = storage::append_result;
    auto cfg = storage::log_append_config{
      // no fsync explicit on a per write, we verify at the end to
      // batch fsync
      storage::log_append_config::fsync::no,
      _io_priority,
      model::timeout_clock::now() + _disk_timeout};
    return details::for_each_ref_extract_configuration(
             _log.offsets().dirty_offset,
             std::move(reader),
             _log.make_appender(cfg),
             cfg.timeout)
      .then([this](std::tuple<ret_t, std::vector<offset_configuration>> t) {
          auto& [ret, configurations] = t;
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
              return _configuration_manager
                .maybe_store_highest_known_offset(
                  ret.last_offset, ret.byte_size)
                .then([ret = ret]() mutable {
                    return ss::make_ready_future<ret_t>(std::move(ret));
                });
          });
      });
}

model::term_id consensus::get_term(model::offset o) {
    if (unlikely(o < model::offset(0))) {
        return model::term_id{};
    }
    return _log.get_term(o).value_or(model::term_id{});
}

clock_type::time_point consensus::last_hbeat_timestamp(model::node_id id) {
    return _fstats.get(id).last_hbeat_timestamp;
}

void consensus::update_node_hbeat_timestamp(model::node_id id) {
    _fstats.get(id).last_hbeat_timestamp = clock_type::now();
    update_node_hschlag_timestamp(id);
}

void consensus::update_node_hschlag_timestamp(model::node_id id) {
    _fstats.get(id).last_hschlag_timestamp = clock_type::now();
}

follower_req_seq consensus::next_follower_sequence(model::node_id id) {
    return _fstats.get(id).last_sent_seq++;
}

absl::flat_hash_map<model::node_id, follower_req_seq>
consensus::next_followers_request_seq() {
    absl::flat_hash_map<model::node_id, follower_req_seq> ret;
    ret.reserve(_fstats.size());
    auto range = boost::make_iterator_range(_fstats.begin(), _fstats.end());
    for (const auto& [node_id, _] : range) {
        ret.emplace(node_id, next_follower_sequence(node_id));
    }
    return ret;
}

void consensus::maybe_update_leader_commit_idx() {
    (void)with_gate(_bg, [this] {
        return _op_lock.get_units().then(
          [this](ss::semaphore_units<> u) mutable {
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
    if (latest_cfg.type() == configuration_type::joint) {
        latest_cfg.discard_old_config();
        vlog(
          _ctxlog.trace,
          "leaving joint consensus, new simple configuration {}",
          latest_cfg);
        auto contains_current = latest_cfg.contains_broker(_self);
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
    auto majority_match = config().quorum_match(
      [this, committed_offset = lstats.committed_offset](model::node_id id) {
          // current node - we just return commited offset
          if (id == _self) {
              return committed_offset;
          }
          return _fstats.get(id).match_committed_index();
      });
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
        return maybe_commit_configuration(std::move(u));
    }
    return ss::now();
}
ss::future<>
consensus::maybe_update_follower_commit_idx(model::offset request_commit_idx) {
    auto lstats = _log.offsets();
    // Raft paper:
    //
    // If leaderCommit > commitIndex, set commitIndex =
    // min(leaderCommit, index of last new entry)
    if (request_commit_idx > _commit_index) {
        auto new_commit_idx = std::min(
          request_commit_idx, lstats.committed_offset);
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
    cfg.for_each_broker([this](const model::broker& n) {
        if (n.id() == _self || _fstats.contains(n.id())) {
            return;
        }
        _fstats.emplace(n.id(), follower_index_metadata(n.id()));
    });
}

void consensus::trigger_leadership_notification() {
    _probe.leadership_changed();
    _leader_notification(leadership_status{
      .term = model::term_id(_term),
      .group = group_id(_group),
      .current_leader = _leader_id});
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

static std::ostream&
operator<<(std::ostream& os, const consensus::vote_state& state) {
    switch (state) {
    case consensus::vote_state::leader:
        return os << "{leader}";
    case consensus::vote_state::follower:
        return os << "{follower}";
    case consensus::vote_state::candidate:
        return os << "{candidate}";
    }
    std::terminate(); // make gcc happy
}

ss::future<timeout_now_reply> consensus::timeout_now(timeout_now_request&& r) {
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
      .term = _term,
      .result = timeout_now_reply::status::success,
    });
}

ss::future<std::error_code>
consensus::transfer_leadership(std::optional<model::node_id> target) {
    if (!is_leader()) {
        vlog(_ctxlog.debug, "Cannot transfer leadership from non-leader");
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
              _ctxlog.debug,
              "Cannot transfer leadership. No suitable target node found");
            return seastar::make_ready_future<std::error_code>(
              make_error_code(errc::node_does_not_exists));
        }

        target = it->first;
    }

    if (*target == _self) {
        vlog(_ctxlog.debug, "Cannot transfer leadership to self");
        return seastar::make_ready_future<std::error_code>(
          make_error_code(errc::not_leader));
    }

    if (_configuration_manager.get_latest_offset() > _commit_index) {
        vlog(
          _ctxlog.debug,
          "Cannot transfer leadership during configuration change");
        return ss::make_ready_future<std::error_code>(
          make_error_code(errc::configuration_change_in_progress));
    }
    auto conf = _configuration_manager.get_latest();

    if (!conf.contains_broker(*target)) {
        vlog(
          _ctxlog.debug,
          "Cannot transfer leadership to node {} not found in configuration",
          *target);
        return seastar::make_ready_future<std::error_code>(
          make_error_code(errc::node_does_not_exists));
    }

    vlog(
      _ctxlog.info,
      "Transferring leadership from {} to {} in term {}",
      _self,
      *target,
      _term);

    auto f = ss::with_gate(_bg, [this, target = *target] {
        if (_transferring_leadership) {
            vlog(
              _ctxlog.info,
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

        /*
         * the follower's log needs to be up-to-date so that it will
         * receive votes when we ask it to trigger an immediate
         * election. so check if the followers needs some recovery, and
         * then wait on that process to complete before sending the
         * election request.
         */
        auto& meta = _fstats.get(target);
        if (!meta.is_recovering && needs_recovery(meta)) {
            dispatch_recovery(meta); // sets is_recovering flag
        }

        auto f = ss::now();
        if (meta.is_recovering) {
            vlog(
              _ctxlog.info,
              "Waiting on node to recover before requesting election");
            auto timeout = ss::semaphore::clock::duration(
              config::shard_local_cfg()
                .raft_transfer_leader_recovery_timeout_ms());
            f = meta.recovery_finished.wait(timeout);
        }

        return f.then([this, target] {
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
                  _ctxlog.debug, "Cannot transfer leadership from non-leader");
                return seastar::make_ready_future<std::error_code>(
                  make_error_code(errc::not_leader));
            }

            if (_as.abort_requested()) {
                return seastar::make_ready_future<std::error_code>(
                  make_error_code(errc::not_leader));
            }

            auto& meta = _fstats.get(target);
            if (needs_recovery(meta)) {
                return seastar::make_ready_future<std::error_code>(
                  make_error_code(errc::timeout));
            }

            timeout_now_request req{
              .node_id = _self,
              .group = _group,
              .term = _term,
            };

            auto timeout
              = raft::clock_type::now()
                + config::shard_local_cfg().raft_timeout_now_timeout_ms();

            return _client_protocol
              .timeout_now(target, std::move(req), rpc::client_opts(timeout))
              .then([](result<timeout_now_reply>) {
                  return seastar::make_ready_future<std::error_code>(
                    make_error_code(errc::success));
              });
        });
    });

    return f.finally([this] { _transferring_leadership = false; });
}

ss::future<> consensus::remove_persistent_state() {
    return _storage.kvs()
      .remove(storage::kvstore::key_space::consensus, voted_for_key())
      .then([this] {
          return _storage.kvs().remove(
            storage::kvstore::key_space::consensus, last_applied_key());
      })
      .then(
        [this] { return _configuration_manager.remove_persistent_state(); });
}

} // namespace raft
