#include "raft/consensus.h"

#include "likely.h"
#include "raft/consensus_utils.h"
#include "raft/errc.h"
#include "raft/logger.h"
#include "raft/recovery_stm.h"
#include "raft/replicate_entries_stm.h"
#include "raft/vote_stm.h"

#include <seastar/core/fstream.hh>
#include <seastar/core/future.hh>

#include <boost/exception/diagnostic_information.hpp>

#include <iterator>

namespace raft {
consensus::consensus(
  model::node_id nid,
  group_id group,
  group_configuration initial_cfg,
  timeout_jitter jit,
  storage::log l,
  storage::log_append_config::fsync should_fsync,
  ss::io_priority_class io_priority,
  model::timeout_clock::duration disk_timeout,
  ss::sharded<rpc::connection_cache>& clis,
  consensus::leader_cb_t cb,
  std::optional<append_entries_cb_t>&& append_callback)
  : _self(std::move(nid))
  , _jit(std::move(jit))
  , _log(l)
  , _should_fsync(should_fsync)
  , _io_priority(io_priority)
  , _disk_timeout(disk_timeout)
  , _clients(clis)
  , _leader_notification(std::move(cb))
  , _conf(std::move(initial_cfg))
  , _ctxlog(_self, group)
  , _append_entries_notification(std::move(append_callback)) {
    _meta.group = group();
    _vote_timeout.set_callback([this] { dispatch_vote(); });
    for (auto& n : _conf.nodes) {
        if (n.id() == _self) {
            continue;
        }
        _follower_stats.emplace(n.id(), follower_index_metadata(n.id()));
    }
}
void consensus::do_step_down() {
    _probe.step_down();
    _voted_for = {};
    _vstate = vote_state::follower;
}

ss::future<> consensus::stop() {
    _vote_timeout.cancel();
    return _bg.close();
}

ss::sstring consensus::voted_for_filename() const {
    return _log.work_directory() + "/voted_for";
}

void consensus::process_append_reply(
  model::node_id node, result<append_entries_reply> r) {
    if (!r) {
        _ctxlog.debug("Error response from {}, {}", node, r.error().message());
        // add stats to group
        return;
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

        return maybe_update_leader_commit_idx();
    }

    follower_index_metadata& idx = get_follower_stats(node);
    append_entries_reply& reply = r.value();

    if (unlikely(reply.group != _meta.group)) {
        // logic bug
        throw std::runtime_error(fmt::format(
          "process_append_reply was sent wrong group: {}", reply.group));
    }
    _ctxlog.trace("append entries reply {}", reply);

    // check preconditions for processing the reply
    if (!is_leader()) {
        _ctxlog.debug("ignorring append entries reply, not leader");
        return;
    }
    if (reply.term > _meta.term) {
        return do_step_down();
    }

    // If recovery is in progress the recovery STM will handle follower index
    // updates
    if (!idx.is_recovering) {
        _ctxlog.trace(
          "Updated node {} last log idx: {}",
          idx.node_id,
          reply.last_log_index);
        idx.last_log_index = model::offset(reply.last_log_index);
        idx.next_index = details::next_offset(idx.last_log_index);
    }

    if (reply.success) {
        return successfull_append_entries_reply(idx, std::move(reply));
    }

    if (idx.is_recovering) {
        // we are already recovering, do nothing
        return;
    }

    if (idx.match_index < _log.max_offset()) {
        // follower match_index is behind, we have to recover it
        dispatch_recovery(idx, std::move(reply));
    }
    // TODO(agallego) - add target_replication_factor,
    // current_replication_factor to group_configuration so we can promote
    // learners to nodes and perform data movement to added replicas
}

void consensus::successfull_append_entries_reply(
  follower_index_metadata& idx, append_entries_reply reply) {
    // follower and leader logs matches
    idx.last_log_index = model::offset(reply.last_log_index);
    idx.match_index = idx.last_log_index;
    idx.next_index = details::next_offset(idx.match_index);
    _ctxlog.trace(
      "Updated node {} match {} and next {} indicies",
      idx.node_id,
      idx.match_index,
      idx.next_index);
    maybe_update_leader_commit_idx();
}

void consensus::dispatch_recovery(
  follower_index_metadata& idx, append_entries_reply reply) {
    auto log_max_offset = _log.max_offset();
    if (idx.last_log_index >= log_max_offset) {
        // follower is ahead of current leader
        // try to send last batch that leader have
        _ctxlog.trace(
          "Follower {} is ahead of leader setting next offset to {}",
          idx.next_index,
          log_max_offset);
        idx.next_index = log_max_offset;
    }
    idx.is_recovering = true;
    _ctxlog.info(
      "Starting recovery process for {} - current reply: {}",
      idx.node_id,
      reply);
    // background
    (void)with_gate(_bg, [this, &idx] {
        auto recovery = std::make_unique<recovery_stm>(this, idx, _io_priority);
        auto ptr = recovery.get();
        return ptr->apply()
          .handle_exception([this, &idx](const std::exception_ptr& e) {
              _ctxlog.warn(
                "Node {} recovery failed - {}",
                idx.node_id,
                boost::diagnostic_information(e));
          })
          .finally([r = std::move(recovery), this] {});
    });
}

ss::future<result<replicate_result>>
consensus::replicate(model::record_batch_reader&& rdr) {
    return with_semaphore(_op_sem, 1, [this, rdr = std::move(rdr)]() mutable {
        return do_replicate(std::move(rdr));
    });
}

ss::future<result<replicate_result>>
consensus::do_replicate(model::record_batch_reader&& rdr) {
    if (!is_leader()) {
        return seastar::make_ready_future<result<replicate_result>>(
          errc::not_leader);
    }

    if (_bg.is_closed()) {
        return ss::make_exception_future<result<replicate_result>>(
          ss::gate_closed_exception());
    }
    return with_gate(_bg, [this, rdr = std::move(rdr)]() mutable {
        append_entries_request req{
          .node_id = _self,
          .meta = _meta,
          .batches
          = model::make_record_batch_reader<details::term_assigning_reader>(
            std::move(rdr), model::term_id(_meta.term))};

        auto stm = ss::make_lw_shared<replicate_entries_stm>(
          this, 3, std::move(req));

        return stm->apply().finally([this, stm] {
            auto f = stm->wait().finally([stm] {});
            // if gate is closed wait for all futures
            if (_bg.is_closed()) {
                return std::move(f);
            }
            // background
            (void)with_gate(_bg, [this, stm, f = std::move(f)]() mutable {
                return std::move(f);
            });
            return ss::make_ready_future<>();
        });
    });
}

/// performs no raft-state mutation other than resetting the timer
void consensus::dispatch_vote() {
    // 5.2.1.4 - prepare next timeout
    arm_vote_timeout();

    auto now = clock_type::now();
    auto expiration = _hbeat + _jit.base_duration();
    if (now < expiration) {
        // nothing to do.
        return;
    }
    if (_vstate == vote_state::leader) {
        return;
    }
    // do not vote when there are no voters available
    if (!_conf.has_voters()) {
        return;
    }

    // background, acquire lock, transition state
    (void)with_gate(_bg, [this] {
        auto vstm = std::make_unique<vote_stm>(this);
        auto p = vstm.get();

        // CRITICAL: vote performs locking on behalf of consensus
        return p->vote().then_wrapped([this, p, vstm = std::move(vstm)](
                                        ss::future<> vote_f) mutable {
            try {
                vote_f.get();
            } catch (...) {
                _ctxlog.warn(
                  "Error returned from voting process {}",
                  boost::diagnostic_information(std::current_exception()));
            }
            auto f = p->wait().finally([vstm = std::move(vstm), this] {});
            // make sure we wait for all futures when gate is closed
            if (_bg.is_closed()) {
                return std::move(f);
            }
            // background
            (void)with_gate(
              _bg, [this, vstm = std::move(vstm), f = std::move(f)]() mutable {
                  return std::move(f);
              });

            return ss::make_ready_future<>();
        });
    });
}
void consensus::arm_vote_timeout() {
    if (!_bg.is_closed()) {
        _vote_timeout.rearm(_jit());
    }
}
ss::future<> consensus::add_group_member(model::broker node) {
    return with_semaphore(_op_sem, 1, [this, node = std::move(node)]() mutable {
        auto cfg = _conf;
        // check once again under the lock
        if (!cfg.contains_broker(node.id())) {
            // New broker
            // FIXME: Change this so that the node is added to followers
            //        not the nodes directly
            cfg.nodes.push_back(std::move(node));

            // append new configuration to log
            return replicate_configuration(std::move(cfg));
        }
        return ss::make_ready_future<>();
    });
}

ss::future<> consensus::start() {
    return with_semaphore(_op_sem, 1, [this] {
        return details::read_voted_for(voted_for_filename())
          .then([this](voted_for_configuration r) {
              if (r.voted_for < 0) {
                  _ctxlog.debug(
                    "Found default voted_for. Skipping term recovery");
                  _meta.term = 0;
                  return details::read_bootstrap_state(_log);
              }
              _ctxlog.info(
                "recovered last voted for: {} for term: {}",
                r.voted_for,
                r.term);
              _voted_for = r.voted_for;
              _meta.term = r.term;
              return details::read_bootstrap_state(_log);
          })
          .then([this](configuration_bootstrap_state st) {
              if (st.config_batches_seen() > 0) {
                  _conf = std::move(st.release_config());
              }
              _meta.prev_log_index = _log.max_offset();
              _meta.prev_log_term = get_term(_log.max_offset());
          })
          .then([this] {
              // Arm leader election timeout.
              arm_vote_timeout();
          });
    });
}
ss::future<vote_reply> consensus::do_vote(vote_request&& r) {
    vote_reply reply;
    reply.term = _meta.term;
    auto last_log_index = _meta.prev_log_index;
    auto last_entry_term = _meta.prev_log_term;
    _probe.vote_requested();
    _ctxlog.trace("Vote requested {}, voted for {}", r, _voted_for);
    /// set to true if the caller's log is as up to date as the recipient's
    /// - extension on raft. see Diego's phd dissertation, section 9.6
    /// - "Preventing disruptions when a server rejoins the cluster"
    reply.log_ok
      = r.prev_log_term > last_entry_term
        || (r.prev_log_term == last_entry_term && r.prev_log_index >= last_log_index);

    // raft.pdf: reply false if term < currentTerm (§5.1)
    if (r.term < _meta.term) {
        _probe.vote_request_term_older();
        return ss::make_ready_future<vote_reply>(std::move(reply));
    }

    if (r.term > _meta.term) {
        _ctxlog.info(
          "received vote response with larger term from node {}, received {}, "
          "current {}",
          r.node_id,
          r.term,
          _meta.term);
        _probe.vote_request_term_newer();
        _meta.term = r.term;
        reply.term = _meta.term;
        do_step_down();
    }

    auto f = ss::make_ready_future<>();

    if (reply.log_ok && _voted_for() < 0) {
        _voted_for = model::node_id(r.node_id);
        _hbeat = clock_type::now();
        _ctxlog.trace("Voting for {} in term {}", r.node_id, _meta.term);
        f = f.then([this] {
            return details::persist_voted_for(
              voted_for_filename(), {_voted_for, model::term_id(_meta.term)});
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
consensus::do_append_entries(append_entries_request&& r) {
    append_entries_reply reply;
    reply.node_id = _self();
    reply.group = r.meta.group;
    reply.term = _meta.term;
    reply.last_log_index = _meta.prev_log_index;
    reply.success = false;
    _probe.append_requested();

    // no need to trigger timeout
    _hbeat = clock_type::now();
    _ctxlog.trace("Append batches, request {}, local {}", r.meta, _meta);

    // raft.pdf: Reply false if term < currentTerm (§5.1)
    if (r.meta.term < _meta.term) {
        _probe.append_request_term_older();
        reply.success = false;
        return ss::make_ready_future<append_entries_reply>(std::move(reply));
    }

    if (r.meta.term > _meta.term) {
        _ctxlog.debug(
          "append_entries request::term:{}  > ours: {}. Setting new term",
          r.meta.term,
          _meta.term);
        _probe.append_request_term_newer();
        _meta.term = r.meta.term;
        return do_append_entries(std::move(r));
    }

    // raft.pdf:If AppendEntries RPC received from new leader: convert to
    // follower (§5.2)
    _vstate = vote_state::follower;

    // raft.pdf: Reply false if log doesn’t contain an entry at
    // prevLogIndex whose term matches prevLogTerm (§5.3)
    // broken into 3 sections

    auto last_log_offset = _log.max_offset();
    // section 1
    // For an entry to fit into our log, it must not leave a gap.
    if (r.meta.prev_log_index > last_log_offset) {
        _ctxlog.debug("rejecting append_entries. would leave gap in log");
        _probe.append_request_log_commited_index_mismatch();
        return ss::make_ready_future<append_entries_reply>(std::move(reply));
    }

    // section 2
    // must come from the same term

    // if prev log index from request is the same as current we can use
    // prev_log_term as an optimization
    auto last_log_term
      = _meta.prev_log_index == r.meta.prev_log_index
          ? _meta.prev_log_term // use term from meta
          : get_term(model::offset(
            r.meta.prev_log_index)); // lookup for request term in log

    if (r.meta.prev_log_term != last_log_term) {
        _ctxlog.debug(
          "rejecting append_entries missmatching entry at previous log term {} "
          "at offset {}",
          last_log_term,
          r.meta.prev_log_index);
        _probe.append_request_log_term_older();
        reply.success = false;
        return ss::make_ready_future<append_entries_reply>(std::move(reply));
    }

    // special case heartbeat case
    // we need to handle it early (before executing truncation)
    // as timeouts are asynchronous to append calls and can have stall data
    if (r.batches.end_of_stream()) {
        _ctxlog.trace("Empty append entries, meta {}", r.meta);
        if (r.meta.prev_log_index < last_log_offset) {
            // do not tuncate on heartbeat just response with false
            reply.success = false;
            return ss::make_ready_future<append_entries_reply>(
              std::move(reply));
        }
        auto previous_commit_idx = _meta.commit_index;
        return maybe_update_follower_commit_idx(
                 model::offset(r.meta.commit_index))
          .then([this, reply = std::move(reply)]() mutable {
              reply.success = true;
              _probe.append_request_heartbeat();
              return ss::make_ready_future<append_entries_reply>(
                std::move(reply));
          });
    }

    // section 3
    if (r.meta.prev_log_index < last_log_offset) {
        auto truncate_at = details::next_offset(
          model::offset(r.meta.prev_log_index));
        _ctxlog.debug(
          "truncate log: request for the same term:{}. Request offset:{} is "
          "earlier than what we have:{}. Truncating to: {}",
          r.meta.term,
          r.meta.prev_log_index,
          _log.max_offset(),
          truncate_at);
        _probe.append_request_log_truncate();
        if (unlikely(r.meta.prev_log_index < _meta.commit_index)) {
            return ss::make_exception_future<append_entries_reply>(
              std::logic_error("Cannot truncate beyond commit index"));
        }
        return _log.truncate(truncate_at)
          .then([this, r = std::move(r)]() mutable {
              _meta.prev_log_index = r.meta.prev_log_index;
              _meta.prev_log_term = r.meta.prev_log_term;
              return do_append_entries(std::move(r));
          });
    }

    // success. copy entries for each subsystem
    using offsets_ret = storage::append_result;
    return disk_append(std::move(r.batches))
      .then([this, m = r.meta](offsets_ret ofs) mutable {
          return maybe_update_follower_commit_idx(model::offset(m.commit_index))
            .then([this, m, ofs = std::move(ofs)]() mutable {
                return make_append_entries_reply(std::move(ofs));
            });
      });
}

ss::future<>
consensus::notify_entries_commited(model::record_batch_reader&& entries) {
    using entries_t = std::vector<model::record_batch_reader>;
    const uint32_t entries_copies = 1 + (_append_entries_notification ? 1 : 0);

    return details::share_n(std::move(entries), entries_copies)
      .then([this](entries_t shared) mutable {
          using ret_t = append_entries_reply;
          std::vector<ss::future<>> n_futures;

          if (!shared.empty()) {
              n_futures.push_back(
                process_configurations(std::move(shared.back())));
              shared.pop_back();
          }
          if (!shared.empty() && _append_entries_notification) {
              n_futures.push_back(
                _append_entries_notification.value()(std::move(shared.back())));
          }
          return ss::when_all(n_futures.begin(), n_futures.end())
            .discard_result();
      });
}

ss::future<>
consensus::process_configurations(model::record_batch_reader&& rdr) {
    return details::extract_configuration(std::move(rdr))
      .then([this](std::optional<group_configuration> cfg) {
          if (cfg) {
              _conf = std::move(*cfg);
              _ctxlog.info("configuration updated {}", _conf);
              for (auto& n : _conf.all_brokers()) {
                  if (n.id() == _self) {
                      // skip
                      continue;
                  }
                  if (auto it = _follower_stats.find(n.id());
                      it == _follower_stats.end()) {
                      _follower_stats.emplace(
                        n.id(), follower_index_metadata(n.id()));
                  }
              }
          }
      });
}

ss::future<> consensus::replicate_configuration(group_configuration cfg) {
    _ctxlog.debug("Replicating group configuration {}", cfg);
    return do_replicate(details::serialize_configuration(std::move(cfg)))
      .discard_result();
}

append_entries_reply
consensus::make_append_entries_reply(storage::append_result disk_results) {
    append_entries_reply reply;
    reply.node_id = _self();
    reply.group = _meta.group;
    reply.term = _meta.term;
    reply.last_log_index = disk_results.last_offset;
    reply.success = true;
    return reply;
}

ss::future<storage::append_result>
consensus::disk_append(model::record_batch_reader&& reader) {
    using ret_t = storage::append_result;
    return ss::do_with(
      std::move(reader), [this](model::record_batch_reader& in) {
          auto cfg = storage::log_append_config{
            // no fsync explicit on a per write, we verify at the end to
            // batch fsync
            storage::log_append_config::fsync::no,
            _io_priority,
            model::timeout_clock::now() + _disk_timeout};

          return in.consume(_log.make_appender(cfg), cfg.timeout)
            .then([this](ret_t ret) {
                // TODO
                // if we rolled a log segment. write current configuration for
                // speedy recovery in the background

                // NOTE: raft can only work with fsync enabled
                if (_should_fsync) {
                    return _log.flush().then([ret = std::move(ret), this] {
                        _probe.entries_appended(1);
                        _meta.prev_log_index = ret.last_offset;
                        _meta.prev_log_term = get_term(ret.last_offset);
                        return ss::make_ready_future<ret_t>(std::move(ret));
                    });
                }
                return ss::make_ready_future<ret_t>(std::move(ret));
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
    return get_follower_stats(id).last_hbeat_timestamp;
}

void consensus::update_node_hbeat_timestamp(model::node_id id) {
    get_follower_stats(id).last_hbeat_timestamp = clock_type::now();
}

follower_index_metadata& consensus::get_follower_stats(model::node_id id) {
    auto it = _follower_stats.find(id);
    if (__builtin_expect(it == _follower_stats.end(), false)) {
        throw std::invalid_argument(
          fmt::format("Node {} is not a group {} follower", id, _meta.group));
    }
    return it->second;
}

void consensus::maybe_update_leader_commit_idx() {
    (void)with_gate(_bg, [this] {
        return seastar::with_semaphore(_op_sem, 1, [this]() mutable {
            return do_maybe_update_leader_commit_idx();
        });
    });
}

ss::future<> consensus::do_maybe_update_leader_commit_idx() {
    // Raft paper:
    //
    // If there exists an N such that N > commitIndex, a majority
    // of matchIndex[i] ≥ N, and log[N].term == currentTerm:
    // set commitIndex = N (§5.3, §5.4).

    std::vector<model::offset> offsets;
    // self offsets
    offsets.push_back(_log.max_offset());
    for (const auto& [_, f_idx] : _follower_stats) {
        offsets.push_back(f_idx.match_index);
    }
    std::sort(offsets.begin(), offsets.end());
    auto majority_match = offsets.at((offsets.size() - 1) / 2);
    if (majority_match > _meta.commit_index) {
        _ctxlog.debug("Leader commit index updated {}", majority_match);
        auto old_commit_idx = _meta.commit_index;
        _meta.commit_index = majority_match;
        auto range_start = details::next_offset(model::offset(old_commit_idx));
        _ctxlog.trace(
          "Commiting entries from {} to {}", range_start, _meta.commit_index);
        auto reader = _log.make_reader(storage::log_reader_config{
          .start_offset = details::next_offset(
            model::offset(old_commit_idx)), // next batch
          .prio = _io_priority,
          .type_filter = {},
          .max_offset = model::offset(_meta.commit_index)});

        return notify_entries_commited(std::move(reader));
    }
    return ss::make_ready_future<>();
}
ss::future<>
consensus::maybe_update_follower_commit_idx(model::offset request_commit_idx) {
    // Raft paper:
    //
    // If leaderCommit > commitIndex, set commitIndex =
    // min(leaderCommit, index of last new entry)
    if (request_commit_idx > _meta.commit_index) {
        auto new_commit_idx = std::min(request_commit_idx, _log.max_offset());
        if (new_commit_idx != _meta.commit_index) {
            auto previous_commit_idx = _meta.commit_index;
            _meta.commit_index = new_commit_idx;
            _ctxlog.debug(
              "Follower commit index updated {}", _meta.commit_index);
            auto reader = _log.make_reader(storage::log_reader_config{
              .start_offset = details::next_offset(
                model::offset(previous_commit_idx)), // next batch
              .prio = _io_priority,
              .type_filter = {},
              .max_offset = model::offset(_meta.commit_index)});

            return notify_entries_commited(std::move(reader));
        }
    }
    return ss::make_ready_future<>();
}
} // namespace raft
