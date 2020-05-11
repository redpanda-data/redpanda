#include "raft/consensus.h"

#include "config/configuration.h"
#include "likely.h"
#include "raft/consensus_client_protocol.h"
#include "raft/consensus_utils.h"
#include "raft/errc.h"
#include "raft/logger.h"
#include "raft/recovery_stm.h"
#include "raft/rpc_client_protocol.h"
#include "raft/vote_stm.h"
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
  std::optional<append_entries_cb_t>&& append_callback)
  : _self(std::move(nid))
  , _group(group)
  , _jit(std::move(jit))
  , _log(l)
  , _io_priority(io_priority)
  , _disk_timeout(disk_timeout)
  , _client_protocol(client)
  , _leader_notification(std::move(cb))
  , _conf(std::move(initial_cfg))
  , _fstats({})
  , _batcher(this)
  , _append_entries_notification(std::move(append_callback))
  , _ctxlog(_self, group)
  , _replicate_append_timeout(
      config::shard_local_cfg().replicate_append_timeout_ms())
  , _recovery_append_timeout(
      config::shard_local_cfg().recovery_append_timeout_ms()) {
    update_follower_stats(_conf);
    _vote_timeout.set_callback([this] {
        dispatch_flush_with_lock();
        dispatch_vote();
    });
}
void consensus::do_step_down() {
    _voted_for = {};
    _hbeat = clock_type::now();
    _vstate = vote_state::follower;
}

ss::future<> consensus::stop() {
    vlog(_ctxlog.info, "Stopping");
    _vote_timeout.cancel();
    _commit_index_updated.broken();
    return _bg.close();
}

ss::sstring consensus::voted_for_filename() const {
    return _log.config().work_directory() + "/voted_for";
}

consensus::success_reply consensus::update_follower_index(
  model::node_id node, result<append_entries_reply> r, follower_req_seq seq) {
    if (!r) {
        vlog(
          _ctxlog.trace,
          "Error response from {}, {}",
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
          "update_follower_index was sent wrong group: {}", reply.group));
    }
    vlog(_ctxlog.trace, "append entries reply {}", reply);
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
        _term = reply.term;
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
          "Updated node {} last log idx: {}",
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

    auto lstats = _log.offsets();
    if (idx.match_index < lstats.dirty_offset) {
        // follower match_index is behind, we have to recover it
        dispatch_recovery(idx, std::move(reply));
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

void consensus::dispatch_recovery(
  follower_index_metadata& idx, append_entries_reply reply) {
    auto lstats = _log.offsets();
    auto log_max_offset = lstats.dirty_offset;
    if (idx.last_dirty_log_index >= log_max_offset) {
        // follower is ahead of current leader
        // try to send last batch that leader have
        vlog(
          _ctxlog.trace,
          "Follower {} is ahead of leader setting next offset to {}",
          idx.next_index,
          log_max_offset);
        idx.next_index = log_max_offset;
    }
    idx.is_recovering = true;
    vlog(
      _ctxlog.trace,
      "Starting recovery process for {} - current reply: {}",
      idx.node_id,
      reply);
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
    if (!is_leader()) {
        return seastar::make_ready_future<result<replicate_result>>(
          errc::not_leader);
    }

    _last_replicate_consistency = opts.consistency;
    if (opts.consistency == consistency_level::quorum_ack) {
        _probe.replicate_requests_ack_all();
        return _batcher.replicate(std::move(rdr)).finally([this] {
            _probe.replicate_done();
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

bool consensus::should_skip_vote() {
    auto last_election = clock_type::now() - _jit.base_duration();

    bool skip_vote = false;

    skip_vote |= (_hbeat > last_election);      // nothing to do.
    skip_vote |= _vstate == vote_state::leader; // already a leader
    skip_vote |= !_conf.has_voters();           // no voters

    return skip_vote;
}

/// performs no raft-state mutation other than resetting the timer
void consensus::dispatch_vote() {
    // 5.2.1.4 - prepare next timeout
    if (should_skip_vote()) {
        arm_vote_timeout();
        return;
    }

    // background, acquire lock, transition state
    (void)with_gate(_bg, [this] {
        auto vstm = std::make_unique<vote_stm>(this);
        auto p = vstm.get();

        // CRITICAL: vote performs locking on behalf of consensus
        return p->vote()
          .then_wrapped(
            [this, p, vstm = std::move(vstm)](ss::future<> vote_f) mutable {
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
                  _bg, [vstm = std::move(vstm), f = std::move(f)]() mutable {
                      return std::move(f);
                  });

                return ss::make_ready_future<>();
            })
          .handle_exception([this](const std::exception_ptr& e) {
              vlog(_ctxlog.warn, "Exception while voting - {}", e);
          })
          .finally([this] { arm_vote_timeout(); });
    });
}
void consensus::arm_vote_timeout() {
    if (!_bg.is_closed()) {
        _vote_timeout.rearm(_jit());
    }
}
ss::future<> consensus::add_group_member(model::broker node) {
    return _op_lock.get_units().then(
      [this, node = std::move(node)](ss::semaphore_units<> u) mutable {
          auto cfg = _conf;
          // check once again under the lock
          if (!cfg.contains_broker(node.id())) {
              // New broker
              // FIXME: Change this so that the node is added to followers
              //        not the nodes directly
              cfg.nodes.push_back(std::move(node));

              // append new configuration to log
              return replicate_configuration(std::move(u), std::move(cfg));
          }
          return ss::make_ready_future<>();
      });
}

ss::future<> consensus::start() {
    vlog(_ctxlog.info, "Starting");
    return _op_lock.with([this] {
        return details::read_voted_for(voted_for_filename())
          .then([this](voted_for_configuration r) {
              if (r.voted_for < 0) {
                  vlog(_ctxlog.debug, "Persistent state file not present");
                  _term = model::term_id(0);
                  return;
              }
              vlog(
                _ctxlog.info,
                "Recovered persistent state: voted for: {}, term: {}",
                r.voted_for,
                r.term);
              _voted_for = r.voted_for;
              _term = r.term;
          })
          .handle_exception([this](const std::exception_ptr& e) {
              vlog(_ctxlog.warn, "Error reading raft persistent state - {}", e);
              _term = model::term_id(0);
          })
          .then([this] { return details::read_bootstrap_state(_log); })
          .then([this](configuration_bootstrap_state st) {
              if (st.config_batches_seen() > 0) {
                  _last_seen_config_offset = st.prev_log_index();
                  _conf = std::move(st.release_config());
                  update_follower_stats(_conf);
              }
              auto lstats = _log.offsets();
              vlog(
                _ctxlog.info,
                "Log offsets: {}, term:{}, commit index: {}",
                lstats,
                _term,
                _commit_index);
          })
          .then([this] {
              // Arm leader election timeout.
              arm_vote_timeout();
          });
    });
}

ss::future<vote_reply> consensus::vote(vote_request&& r) {
    return with_gate(_bg, [this, r = std::move(r)]() mutable {
        return _op_lock.with(
          [this, r = std::move(r)]() mutable { return do_vote(std::move(r)); });
    });
}

ss::future<vote_reply> consensus::do_vote(vote_request&& r) {
    vote_reply reply;
    reply.term = _term;
    auto lstats = _log.offsets();
    auto last_log_index = lstats.dirty_offset;
    auto last_entry_term = lstats.dirty_offset_term;
    _probe.vote_request();
    vlog(_ctxlog.trace, "Vote requested {}, voted for {}", r, _voted_for);
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
    /// When current node is a leader (we set _hbeat to max after successfull
    /// election) or already processed request from active leader  do not grant
    /// a vote to follower. This will prevent restarted nodes to disturb all
    /// groups leadership
    // Check if we updated the heartbeat timepoint in the last election timeout
    // duration
    auto prev_election = clock_type::now() - _jit.base_duration();
    if (_hbeat > prev_election) {
        vlog(_ctxlog.trace, "We already heard from the leader");
        reply.granted = false;
        return ss::make_ready_future<vote_reply>(std::move(reply));
    }

    if (r.term > _term) {
        vlog(
          _ctxlog.info,
          "received vote response with larger term from node {}, received {}, "
          "current {}",
          r.node_id,
          r.term,
          _term);
        _term = r.term;
        reply.term = _term;
        do_step_down();
    }

    auto f = ss::make_ready_future<>();

    if (reply.log_ok && _voted_for() < 0) {
        _voted_for = model::node_id(r.node_id);
        _hbeat = clock_type::now();
        vlog(_ctxlog.trace, "Voting for {} in term {}", r.node_id, _term);
        f = f.then([this] {
            return details::persist_voted_for(
                     voted_for_filename(), {_voted_for, _term})
              .handle_exception([this](const std::exception_ptr& e) {
                  vlog(
                    _ctxlog.warn,
                    "Unable to persist raft group state, vote not granted - {}",
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
    _hbeat = clock_type::now();
    vlog(
      _ctxlog.trace,
      "Append batches, request {}, term {}, commit_index: {}, prev_log_idx: "
      "{}, prev_log_term: {}",
      r.meta,
      _term,
      _commit_index,
      lstats.dirty_offset,
      lstats.dirty_offset_term);

    // raft.pdf: Reply false if term < currentTerm (§5.1)
    if (r.meta.term < _term) {
        reply.result = append_entries_reply::status::failure;
        return ss::make_ready_future<append_entries_reply>(std::move(reply));
    }

    if (r.meta.term > _term) {
        vlog(
          _ctxlog.debug,
          "append_entries request::term:{}  > ours: {}. Setting new term",
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
              "rejecting append_entries. would leave gap in log");
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

    if (r.meta.prev_log_term != last_log_term) {
        vlog(
          _ctxlog.debug,
          "rejecting append_entries missmatching entry at previous log term {} "
          "at offset {}",
          last_log_term,
          r.meta.prev_log_index);
        reply.result = append_entries_reply::status::failure;
        return ss::make_ready_future<append_entries_reply>(std::move(reply));
    }

    // special case heartbeat case
    // we need to handle it early (before executing truncation)
    // as timeouts are asynchronous to append calls and can have stall data
    if (r.batches.is_end_of_stream()) {
        vlog(_ctxlog.trace, "Empty append entries, meta {}", r.meta);
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
              "Stale append entries request processed, entry is "
              "already present");
            return ss::make_ready_future<append_entries_reply>(
              std::move(reply));
        }
        auto truncate_at = details::next_offset(
          model::offset(r.meta.prev_log_index));
        vlog(
          _ctxlog.debug,
          "truncate log: request for the same term:{}. Request offset:{} is "
          "earlier than what we have:{}. Truncating to: {}",
          r.meta.term,
          r.meta.prev_log_index,
          lstats.dirty_offset,
          truncate_at);
        _probe.log_truncated();
        return _log
          .truncate(storage::truncate_config(truncate_at, _io_priority))
          .then([this, r = std::move(r)]() mutable {
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

ss::future<> consensus::notify_entries_commited(
  model::offset start_offset, model::offset end_offset) {
    auto fut = ss::make_ready_future<>();
    if (_append_entries_notification) {
        vlog(
          _ctxlog.debug,
          "Append entries notification range [{},{}]",
          start_offset,
          end_offset);
        fut = fut.then([this, start_offset, end_offset]() {
            return _log
              .make_reader(storage::log_reader_config(
                start_offset, end_offset, _io_priority))
              .then([this](model::record_batch_reader reader) {
                  _append_entries_notification.value()(std::move(reader));
              });
        });
    }
    auto cfg_reader_start_offset = details::next_offset(
      _last_seen_config_offset);
    fut = fut.then([this, cfg_reader_start_offset, end_offset] {
        vlog(
          _ctxlog.trace,
          "Process configurations range [{},{}]",
          cfg_reader_start_offset,
          end_offset);
        return _log
          .make_reader(storage::log_reader_config(
            cfg_reader_start_offset,
            end_offset,
            0,
            std::numeric_limits<size_t>::max(),
            _io_priority,
            raft::configuration_batch_type,
            std::nullopt))
          .then([this, end_offset](model::record_batch_reader reader) {
              return process_configurations(std::move(reader), end_offset);
          });
    });
    return fut;
}

ss::future<> consensus::process_configurations(
  model::record_batch_reader&& rdr, model::offset last_config_offset) {
    _last_seen_config_offset = last_config_offset;
    return details::extract_configuration(std::move(rdr))
      .then([this](std::optional<group_configuration> cfg) {
          if (cfg) {
              update_follower_stats(*cfg);
              _conf = std::move(*cfg);
              vlog(_ctxlog.info, "configuration updated {}", _conf);
              _probe.configuration_update();
          }
      });
}

ss::future<> consensus::replicate_configuration(
  ss::semaphore_units<> u, group_configuration cfg) {
    // under the _op_sem lock
    vlog(_ctxlog.debug, "Replicating group configuration {}", cfg);
    auto batches = details::serialize_configuration_as_batches(std::move(cfg));
    for (auto& b : batches) {
        b.set_term(model::term_id(_term));
    }
    auto seqs = next_followers_request_seq();
    append_entries_request req(
      _self,
      meta(),
      model::make_memory_record_batch_reader(std::move(batches)));
    return _batcher.do_flush({}, std::move(req), std::move(u), std::move(seqs));
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

    return std::move(reader)
      .for_each_ref(_log.make_appender(cfg), cfg.timeout)
      .then([this](ret_t ret) {
          _has_pending_flushes = true;
          // TODO
          // if we rolled a log segment. write current configuration
          // for speedy recovery in the background

          // leader never flush just after write
          // for quorum_ack it flush in parallel to dispatching RPCs to
          // followers for other consistency flushes are done separately.
          return ss::make_ready_future<ret_t>(std::move(ret));
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
        return _op_lock.with(
          [this]() mutable { return do_maybe_update_leader_commit_idx(); });
    }).handle_exception([this](const std::exception_ptr& e) {
        vlog(_ctxlog.warn, "Error updating leader commit index", e);
    });
}

ss::future<> consensus::do_maybe_update_leader_commit_idx() {
    auto lstats = _log.offsets();
    // Raft paper:
    //
    // If there exists an N such that N > commitIndex, a majority
    // of matchIndex[i] ≥ N, and log[N].term == currentTerm:
    // set commitIndex = N (§5.3, §5.4).

    std::vector<model::offset> offsets;
    // self offsets
    offsets.push_back(lstats.committed_offset);
    for (const auto& [_, f_idx] : _fstats) {
        offsets.push_back(f_idx.match_committed_index());
    }
    std::sort(offsets.begin(), offsets.end());
    size_t majority_match_idx = (offsets.size() - 1) / 2;
    auto majority_match = offsets[majority_match_idx];
    if (
      majority_match > _commit_index
      && _log.get_term(majority_match) == _term) {
        vlog(_ctxlog.trace, "Leader commit index updated {}", majority_match);
        auto old_commit_idx = _commit_index;
        _commit_index = majority_match;
        auto range_start = details::next_offset(model::offset(old_commit_idx));
        vlog(
          _ctxlog.trace,
          "Committing entries from {} to {}",
          range_start,
          _commit_index);
        _commit_index_updated.broadcast();
        return notify_entries_commited(
          details::next_offset(model::offset(old_commit_idx)),
          model::offset(_commit_index));
    }
    return ss::make_ready_future<>();
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
            auto previous_commit_idx = _commit_index;
            _commit_index = new_commit_idx;
            vlog(
              _ctxlog.trace, "Follower commit index updated {}", _commit_index);
            _commit_index_updated.broadcast();
            return notify_entries_commited(
              details::next_offset(model::offset(previous_commit_idx)),
              model::offset(_commit_index));
        }
    }
    return ss::make_ready_future<>();
}

void consensus::update_follower_stats(const group_configuration& cfg) {
    for (auto& n : cfg.nodes) {
        if (n.id() == _self || _fstats.contains(n.id())) {
            continue;
        }
        _fstats.emplace(n.id(), follower_index_metadata(n.id()));
    }

    for (auto& n : cfg.learners) {
        if (n.id() == _self || _fstats.contains(n.id())) {
            continue;
        }
        auto idx = follower_index_metadata(n.id());
        idx.is_learner = true;
        _fstats.emplace(n.id(), idx);
    }
}

void consensus::trigger_leadership_notification() {
    _leader_notification(leadership_status{.term = model::term_id(_term),
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

} // namespace raft
