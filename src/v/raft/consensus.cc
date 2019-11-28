#include "raft/consensus.h"

#include "raft/consensus_utils.h"
#include "raft/logger.h"
#include "raft/replicate_entries_stm.h"
#include "raft/vote_stm.h"
#include "seastarx.h"

#include <seastar/core/fstream.hh>

#include <iterator>
namespace raft {
using vote_request_ptr = consensus::vote_request_ptr;
using vote_reply_ptr = consensus::vote_reply_ptr;

consensus::consensus(
  model::node_id nid,
  group_id group,
  group_configuration initial_cfg,
  timeout_jitter jit,
  storage::log& l,
  storage::log_append_config::fsync should_fsync,
  io_priority_class io_priority,
  model::timeout_clock::duration disk_timeout,
  sharded<client_cache>& clis,
  consensus::leader_cb_t cb)
  : _self(std::move(nid))
  , _jit(std::move(jit))
  , _log(l)
  , _should_fsync(should_fsync)
  , _io_priority(io_priority)
  , _disk_timeout(disk_timeout)
  , _clients(clis)
  , _leader_notification(std::move(cb))
  , _conf(std::move(initial_cfg)) {
    _meta.group = group();
    _vote_timeout.set_callback([this] { dispatch_vote(); });
}
void consensus::step_down() {
    _probe.step_down();
    _voted_for = {};
    _vstate = vote_state::follower;
}

future<> consensus::stop() {
    _vote_timeout.cancel();
    return _bg.close();
}

void consensus::process_heartbeat(append_entries_reply&&) {}

future<> consensus::replicate(raft::entry&& e) {
    if (!is_leader()) {
        return make_exception_future<>(std::runtime_error(fmt::format(
          "Not the leader(self.node_id:{}, meta:{}). Cannot "
          "consensus::replicate(entry&&)",
          _self,
          _meta.group)));
    }

    return with_semaphore(_op_sem, 1, [this, e = std::move(e)]() mutable {
        // entyr point for append_entries_request throughout the system
        append_entries_request req;
        req.node_id = _self;
        req.meta = _meta;
        req.entries.reserve(1);
        req.entries.push_back(std::move(e));
        auto stm = make_lw_shared<replicate_entries_stm>(
          this, 3, std::move(req));
        return stm->apply().finally(
          [stm] { (void)stm->wait().finally([stm] {}); });
    });
} // namespace raft

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
        return p->vote().then([p, vstm = std::move(vstm)]() mutable {
            // background
            (void)p->wait().finally([vstm = std::move(vstm)] {});
        });
    });
}
void consensus::arm_vote_timeout() {
    if (!_bg.is_closed()) {
        _vote_timeout.rearm(_jit());
    }
}
future<> consensus::update_machines_configuration(model::broker node) {
           // FIXME: Add node to followers if it does not exists yet.
       // STUB: As only one node will join the cluster add it to list to
           //       allow raft to work
     if (!_conf.contains_machine(node.id())) {
        _conf.nodes.push_back(std::move(node));
    }
    return make_ready_future<>();
}

future<> consensus::start() {
    return with_semaphore(_op_sem, 1, [this] {
        return details::read_voted_for(voted_for_filename())
          .then([this](voted_for_configuration r) {
              if (r.voted_for < 0) {
                  raftlog.debug(
                    "Found default voted_for. Skipping term recovery");
                  _meta.term = 0;
                  return details::read_bootstrap_state(_log);
              }
              raftlog.info(
                "group '{}' recovered last leader: {} for term: {}",
                _meta.group,
                r.voted_for,
                r.term);
              _voted_for = r.voted_for;
              _meta.term = r.term;
              return details::read_bootstrap_state(_log);
          })
          .then([this](configuration_bootstrap_state st) {
              _meta.commit_index = 0;
              _meta.prev_log_index = 0;
              _meta.prev_log_term = 0;
              if (st.data_batches_seen() > 0) {
                  _meta.commit_index = st.commit_index();
                  _meta.prev_log_index = st.prev_log_index();
                  _meta.prev_log_term = st.prev_log_term();
              }
              if (st.config_batches_seen() > 0) {
                  _conf = std::move(st.release_config());
              }
          })
          .then([this] {
              // Arm leader election timeout.
              arm_vote_timeout();
          });
    });
}
future<vote_reply> consensus::do_vote(vote_request&& r) {
    vote_reply reply;
    reply.term = _meta.term;
    _probe.vote_requested();
    /// set to true if the caller's log is as up to date as the recipient's
    /// - extension on raft. see Diego's phd dissertation, section 9.6
    /// - "Preventing disruptions when a server rejoins the cluster"
    reply.log_ok
      = r.prev_log_term > _meta.term
        || (r.prev_log_term == _meta.term && r.prev_log_index >= _meta.commit_index);

    // raft.pdf: reply false if term < currentTerm (§5.1)
    if (r.term < _meta.term) {
        _probe.vote_request_term_older();
        return make_ready_future<vote_reply>(std::move(reply));
    }

    if (r.term > _meta.term) {
        raftlog.info(
          "self node-id:{}, remote node-id:{}, group {} recevied vote with "
          "larger term:{} than ours:{}",
          _self,
          r.node_id,
          _meta.group,
          r.term,
          _meta.term);
        _probe.vote_request_term_newer();
        step_down();
    }

    // raft.pdf: If votedFor is null or candidateId, and candidate’s log is
    // atleast as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
    if (_voted_for() < 0 || (reply.log_ok && r.node_id == _voted_for)) {
        _meta.term = r.term;
        _voted_for = model::node_id(r.node_id);
        reply.granted = true;
        _hbeat = clock_type::now();
        return details::persist_voted_for(
                 voted_for_filename(), {_voted_for, model::term_id(_meta.term)})
          .then([reply = std::move(reply)] {
              return make_ready_future<vote_reply>(std::move(reply));
          });
    }
    // vote for the same term, same server_id
    reply.granted = (r.node_id == _voted_for);
    if (reply.granted) {
        _hbeat = clock_type::now();
    }
    return make_ready_future<vote_reply>(std::move(reply));
}

template<typename Container>
static inline bool has_configuration_entires(const Container& c) {
    return std::any_of(std::cbegin(c), std::cend(c), [](const entry& r) {
        return r.entry_type() == model::record_batch_type{2};
    });
}

future<append_entries_reply>
consensus::do_append_entries(append_entries_request&& r) {
    append_entries_reply reply;
    reply.node_id = _self();
    reply.group = r.meta.group;
    reply.term = _meta.term;
    reply.last_log_index = _meta.commit_index;
    reply.success = false;
    _probe.append_requested();
    // raft.pdf: Reply false if term < currentTerm (§5.1)
    if (r.meta.term < _meta.term) {
        _probe.append_request_term_older();
        reply.success = false;
        return make_ready_future<append_entries_reply>(std::move(reply));
    }
    if (r.meta.term > _meta.term) {
        raftlog.debug(
          "append_entries request::term:{}  > ours: {}. Setting new term",
          r.meta.term,
          _meta.term);
        _probe.append_request_term_newer();
        return _log
          .truncate(
            model::offset(_meta.commit_index), model::term_id(r.meta.term))
          .then([this, r = std::move(r)]() mutable {
              step_down();
              _meta.term = r.meta.term;
              return do_append_entries(std::move(r));
          });
    }
    // raft.pdf: Reply false if log doesn’t contain an entry at
    // prevLogIndex whose term matches prevLogTerm (§5.3)
    // broken into 3 sections

    // section 1
    // For an entry to fit into our log, it must not leave a gap.
    if (r.meta.prev_log_index > _meta.commit_index) {
        raftlog.debug("rejecting append_entries. would leave gap in log");
        _probe.append_request_log_commited_index_mismatch();
        reply.success = false;
        return make_ready_future<append_entries_reply>(std::move(reply));
    }

    // section 2
    // must come from the same term
    if (r.meta.prev_log_term != _meta.prev_log_term) {
        raftlog.debug("rejecting append_entries missmatching prev_log_term");
        _probe.append_request_log_term_older();
        reply.success = false;
        return make_ready_future<append_entries_reply>(std::move(reply));
    }

    // the success case
    // section 3
    if (r.meta.prev_log_index < _meta.commit_index) {
        raftlog.debug(
          "truncate log: request for the same term:{}. Request "
          "offset:{} is earlier than what we have:{}. Truncating to: {}",
          r.meta.term,
          r.meta.prev_log_index,
          _meta.commit_index,
          r.meta.prev_log_index);
        _probe.append_request_log_truncate();
        return _log
          .truncate(
            model::offset(r.meta.prev_log_index), model::term_id(r.meta.term))
          .then([this, r = std::move(r)]() mutable {
              return do_append_entries(std::move(r));
          });
    }

    // no need to trigger timeout
    _hbeat = clock_type::now();

    // special case heartbeat case
    if (r.entries.empty()) {
        reply.success = true;
        _probe.append_request_heartbeat();
        return make_ready_future<append_entries_reply>(std::move(reply));
    }
    // if we have already committed the offset, return success
    if (r.meta.commit_index == _meta.commit_index) {
        reply.success = true;
        // TODO: add probe
        //_probe.append_request_duplicate_batch();
        return make_ready_future<append_entries_reply>(std::move(reply));
    }
    // success. copy entries for each subsystem
    using entries_t = std::vector<entry>;
    using offsets_ret = std::vector<storage::log::append_result>;
    return details::share_n(std::move(r.entries), 2)
      .then([this, m = r.meta](std::vector<entries_t> dups) mutable {
          entries_t entries_for_disk = std::move(dups.back());
          dups.pop_back();
          return disk_append(std::move(entries_for_disk))
            .then([this, m = std::move(m), dups = std::move(dups)](
                    offsets_ret ofs) mutable {
                return make_append_entries_reply(m, ofs).then(
                  [this,
                   dups = std::move(dups)](append_entries_reply repl) mutable {
                      return commit_entries(
                        std::move(dups.back()), std::move(repl));
                  });
            });
      });
}

future<append_entries_reply> consensus::commit_entries(
  std::vector<entry> entries, append_entries_reply reply) {
    // update metadata first
    _meta.prev_log_index = _meta.commit_index;
    _meta.prev_log_term = _meta.term;
    _meta.commit_index = reply.last_log_index;
    raftlog.debug("Entries commited, protocol metadata {}", _meta);
    using entries_t = std::vector<entry>;
    const uint32_t entries_copies
      = (_append_entries_notification ? 1 : 0)
        + (has_configuration_entires(entries) ? 1 : 0);
    return details::share_n(std::move(entries), entries_copies)
      .then([this, r = std::move(reply)](std::vector<entries_t> dups) mutable {
          using ret_t = append_entries_reply;
          if (!dups.empty() && _append_entries_notification) {
              _append_entries_notification(std::move(dups.back()));
              dups.pop_back();
          }
          if (!dups.empty() && has_configuration_entires(dups.back())) {
              return process_configurations(std::move(dups.back()))
                .then([r = std::move(r)]() mutable {
                    return make_ready_future<ret_t>(std::move(r));
                });
          }
          return make_ready_future<ret_t>(std::move(r));
      });
}

future<> consensus::process_configurations(std::vector<entry>&& e) {
    return do_with(std::move(e), [this](std::vector<entry>& entries) {
        return do_for_each(entries, [this](entry& e) {
            if (e.entry_type() == configuration_batch_type) {
                return details::extract_configuration(std::move(e))
                  .then([this](group_configuration cfg) mutable {
                      _conf = std::move(cfg);
                      raftlog.info(
                        "group({}) configuration update", _meta.group);
                  });
            }
            return make_ready_future<>();
        });
    });
}
future<append_entries_reply> consensus::make_append_entries_reply(
  protocol_metadata sender,
  std::vector<storage::log::append_result> disk_results) {
    // always update metadata first!
    const model::offset last_offset = disk_results.back().last_offset;

    append_entries_reply reply;
    reply.node_id = _self();
    reply.group = sender.group;
    reply.term = _meta.term;
    reply.last_log_index = last_offset;
    reply.success = true;
    return make_ready_future<append_entries_reply>(std::move(reply));
}

future<std::vector<storage::log::append_result>>
consensus::disk_append(std::vector<entry>&& entries) {
    using ret_t = std::vector<storage::log::append_result>;
    // used to detect if we roll the last segment
    model::offset prev_base_offset = _log.segments().empty()
                                       ? model::offset(0)
                                       : _log.segments().last()->base_offset();

    // clang-format off
    return do_with(std::move(entries), [this](std::vector<entry>& in) {
            auto no_of_entries = in.size();
            auto cfg = storage::log_append_config{
            // no fsync explicit on a per write, we verify at the end to
            // batch fsync
            storage::log_append_config::fsync::no,
            _io_priority,
            model::timeout_clock::now() + _disk_timeout};
            return copy_range<ret_t>(in, [this, cfg](entry& e) {
                   return _log.append(std::move(e.reader()), cfg);
            }).then([this, no_of_entries](ret_t ret) {
                   _probe.entries_appended(no_of_entries);
                   return ret;
            });
      })
      // clang-format on
      .then([this, prev_base_offset](ret_t ret) {
          model::offset base_offset = _log.segments().last()->base_offset();
          if (prev_base_offset != base_offset) {
              // we rolled a log segment. write current configuration for speedy
              // recovery in the background
              // FIXME
          }
          if (_should_fsync) {
              return _log.flush().then([ret = std::move(ret)] {
                  return make_ready_future<ret_t>(std::move(ret));
              });
          }
          return make_ready_future<ret_t>(std::move(ret));
      });
}
} // namespace raft
