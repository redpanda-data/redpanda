#include "raft/consensus.h"

#include "raft/consensus_utils.h"
#include "raft/logger.h"
#include "seastarx.h"

#include <seastar/core/fstream.hh>

namespace raft {
using vote_request_ptr = consensus::vote_request_ptr;
using vote_reply_ptr = consensus::vote_reply_ptr;

consensus::consensus(
  model::node_id nid,
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
  , _leader_notification(std::move(cb)) {
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

static future<vote_reply_ptr> one_vote(
  model::node_id node, const sharded<client_cache>& cls, vote_request r) {
    // FIXME #206
    auto shard = client_cache::shard_for(node);
    using freq = vote_request_ptr;
    freq req = make_foreign(std::make_unique<vote_request>(std::move(r)));
    return smp::submit_to(shard, [node, &cls, r = std::move(req)]() mutable {
        using fptr = vote_reply_ptr;
        auto& local = cls.local();
        if (!local.contains(node)) {
            return make_exception_future<fptr>(std::runtime_error(fmt::format(
              "Could not vote(). Node {} not in client_cache", node)));
        }
        // local copy of vote
        return local.get(node)->with_client(
          [rem = std::move(r)](reconnect_client::client_type& cli) mutable {
              vote_request r(*rem); // local copy
              // FIXME: #137
              return cli.vote(std::move(r))
                .then([](rpc::client_context<vote_reply> r) {
                    auto ptr = std::make_unique<vote_reply>(std::move(r.data));
                    return make_ready_future<fptr>(
                      make_foreign(std::move(ptr)));
                });
          });
    });
}

/// state mutation must happen inside `_ops_sem`
future<> consensus::process_vote_replies(std::vector<vote_reply_ptr> reqs) {
    if (_vstate != vote_state::candidate) {
        raftlog().debug(
          "We are no longer a candidate. Active term:{}", _meta.term);
        return make_ready_future<>();
    }
    const size_t majority = (_conf.nodes.size() / 2) + 1;
    const size_t votes_granted = std::accumulate(
      reqs.begin(),
      reqs.end(),
      size_t(0),
      [](size_t acc, const vote_reply_ptr& reply) {
          if (reply->granted) {
              ++acc;
          }
          return acc;
      });
    if (votes_granted < majority) {
        raftlog().info(
          "Majority vote failed. Got {}/{} votes, need:{}",
          votes_granted,
          _conf.nodes.size(),
          majority);
        return make_ready_future<>();
    }
    // section vote:5.2.2
    return with_semaphore(
             _op_sem,
             1,
             [this] {
                 // race on acquiring lock requiers double check
                 if (_vstate != vote_state::candidate) {
                     return make_ready_future<>();
                 }
                 raftlog().info("We are the new leader, term:{}", _meta.term);
                 _vstate = vote_state::leader;
                 return make_ready_future<>();
             })
      .then([this] {
          // execute callback oustide of critical section
          _leader_notification(group_id(_meta.group));
      });
}

// FIXME need to see if we should inform the learners
future<std::vector<vote_reply_ptr>>
consensus::send_vote_requests(clock_type::time_point timeout) {
    auto sem = make_lw_shared<semaphore>(_conf.nodes.size());
    auto ret = make_lw_shared<std::vector<vote_reply_ptr>>();
    ret->reserve(_conf.nodes.size());
    // force copy
    const vote_request vreq{_self(),
                            _meta.group,
                            _meta.term,
                            _meta.prev_log_index,
                            _meta.prev_log_term};
    // background
    (void)do_for_each(
      _conf.nodes.begin(),
      _conf.nodes.end(),
      [this, timeout, sem, ret, vreq](const model::broker& b) mutable {
          model::node_id n = b.id();
          // ensure 'sem' capture & 'vreq' copy
          future<> f
            = one_vote(n, _clients, vreq).then([ret, sem](vote_reply_ptr r) {
                  ret->push_back(std::move(r));
              });
          f = with_semaphore(
            *sem, 1, [f = std::move(f)]() mutable { return std::move(f); });
          // background
          (void)with_timeout(timeout, std::move(f))
            .handle_exception([n](std::exception_ptr e) {
                raftlog().info("Node:{} could not vote() - {} ", n, e);
            });
      });
    // wait for safety, or timeout
    const size_t majority = (_conf.nodes.size() / 2) + 1;
    return sem->wait(majority).then([ret, sem] {
        std::vector<vote_reply_ptr> clean;
        clean.reserve(ret->size());
        std::move(ret->begin(), ret->end(), clean.begin());
        ret->clear();
        return clean;
    });
}

future<> consensus::do_dispatch_vote(clock_type::time_point timeout) {
    // Section 5.2
    // 1 start election
    // 1.2 increment term
    // 1.3 vote for self
    // 1.4 reset election timer
    // 1.5 send all votes
    // 2 if votes from majority become leader
    // 3 if got append_entries() from new leader, become follower
    // 4 if election timeout elapses, start new lection

    // 5.2.1
    _vstate = vote_state::candidate;
    // 5.2.1.2
    _meta.term = _meta.term + 1;
    // 5.2.1.3
    auto req = vote_request{_self(),
                            _meta.group,
                            _meta.term,
                            _meta.prev_log_index,
                            _meta.prev_log_term};
    auto f = do_vote(std::move(req)).then([this](vote_reply r) {
        if (!r.granted) {
            // we are under _ops_sem. should never happen
            return make_exception_future<>(std::runtime_error(fmt::format(
              "Logic error. Self vote granting failed. term:{}", _meta.term)));
        }
        raftlog().debug("self vote granted. term:{}", _meta.term);
        return make_ready_future<>();
    });

    // 5.2.1.5 - background
    (void)send_vote_requests(timeout).then([this](auto replies) {
        return process_vote_replies(std::move(replies));
    });

    // exec
    return std::move(f);
}
/// performs no raft-state mutation other than resetting the timer
/// state manipulation needs to happen under the `_ops_sem`
/// see do_dispatch_vote()
void consensus::dispatch_vote() {
    auto now = clock_type::now();
    auto expiration = _hbeat + _jit.base_duration();
    if (now < expiration) {
        // nothing to do.
        return;
    }
    if (_vstate == vote_state::leader) {
        return;
    }
    if (!_bg.is_closed()) {
        // 5.2.1.4 - prepare next timeout
        _vote_timeout.arm(_jit());
    }
    // background, acquire lock, transition state
    (void)with_gate(_bg, [this] {
        // must be oustside semaphore
        const auto timeout = clock_type::now() + _jit.base_duration();
        return with_semaphore(_op_sem, 1, [this, timeout] {
            return with_timeout(timeout, do_dispatch_vote(timeout))
              .handle_exception([](std::exception_ptr e) {
                  raftlog().info("Could not finish vote(): {}", e);
              });
            _hbeat = clock_type::now();
        });
    });
}
future<> consensus::start() {
    return with_semaphore(_op_sem, 1, [this] {
        // TODO(agallego) - recover _conf & _meta
        return details::read_voted_for(voted_for_filename())
          .then([this](voted_for_configuration r) {
              raftlog().debug(
                "recovered last leader: {} for term: {}", r.voted_for, r.term);
              if (_voted_for != r.voted_for) {
                  // FIXME - look at etcd
              }
              _voted_for = r.voted_for;
              // bug! because we may increase the term while we have this leader
              // so
              _meta.term = r.term;
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
        raftlog().info(
          "{}-group recevied vote with larger term:{} than ours:{}",
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
        return details::persist_voted_for(
                 voted_for_filename(), {_voted_for, model::term_id(_meta.term)})
          .then([reply = std::move(reply)] {
              return make_ready_future<vote_reply>(std::move(reply));
          });
    }
    // vote for the same term, same server_id
    reply.granted = (r.node_id == _voted_for);
    return make_ready_future<vote_reply>(std::move(reply));
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
        raftlog().debug(
          "append_entries request::term:{}  > ours: {}. Setting new term",
          r.meta.term,
          _meta.term);
        _probe.append_request_term_newer();
        return _log
          .roll(model::offset(_meta.commit_index), model::term_id(r.meta.term))
          .then([this, r = std::move(r)]() mutable {
              step_down();
              _meta.term = r.meta.term;
              return append_entries(std::move(r));
          });
    }
    // raft.pdf: Reply false if log doesn’t contain an entry at
    // prevLogIndex whose term matches prevLogTerm (§5.3)
    // broken into 3 sections

    // section 1
    // For an entry to fit into our log, it must not leave a gap.
    if (r.meta.prev_log_index > _meta.commit_index) {
        raftlog().debug("rejecting append_entries. would leave gap in log");
        _probe.append_request_log_commited_index_mismatch();
        reply.success = false;
        return make_ready_future<append_entries_reply>(std::move(reply));
    }

    // section 2
    // must come from the same term
    if (r.meta.prev_log_term != _meta.prev_log_term) {
        raftlog().debug("rejecting append_entries missmatching prev_log_term");
        _probe.append_request_log_term_older();
        reply.success = false;
        return make_ready_future<append_entries_reply>(std::move(reply));
    }

    // the success case
    // section 3
    if (r.meta.prev_log_index < _meta.commit_index) {
        raftlog().debug(
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

    // call custom serializers/hooks
    for (auto h : _hooks) {
        h->pre_commit(model::offset(_meta.commit_index), r.entries);
    }

    // move the vector and copy the header metadata
    return disk_append(std::move(r.entries))
      .then([this, r = std::move(r), reply = std::move(reply)](
              std::vector<storage::log::append_result> ret) mutable {
          // always update metadata first! to allow next put to truncate log
          const model::offset last_offset = ret.back().last_offset;
          const model::offset begin_offset = model::offset(_meta.commit_index);
          _meta.commit_index = last_offset;
          _meta.prev_log_term = r.meta.term;
          if (r.meta.commit_index < last_offset) {
              step_down();
              for (auto h : _hooks) {
                  h->abort(begin_offset);
              }
              _probe.leader_commit_index_mismatch();
              throw std::runtime_error(fmt::format(
                "Log is now in an inconsistent state. Leader commit_index:{} "
                "vs. our commit_index:{}, for term:{}",
                r.meta.commit_index,
                _meta.commit_index,
                _meta.prev_log_term));
          }
          reply.term = _meta.term;
          reply.last_log_index = _meta.commit_index;
          reply.success = true;
          for (auto h : _hooks) {
              h->commit(begin_offset, last_offset);
          }
          return make_ready_future<append_entries_reply>(std::move(reply));
      });
}

future<std::vector<storage::log::append_result>>
consensus::disk_append(std::vector<entry>&& entries) {
    using ret_t = std::vector<storage::log::append_result>;
    return do_with(
             std::move(entries),
             [this](std::vector<entry>& in) {
                 // needs a ref to the range
                 auto no_of_entries = in.size();
                 return copy_range<ret_t>(
                          in,
                          [this](entry& e) {
                              return _log.append(
                                std::move(e.reader()),
                                storage::log_append_config{
                                  // explicit here
                                  storage::log_append_config::fsync::no,
                                  _io_priority,
                                  model::timeout_clock::now() + _disk_timeout});
                          })
                   .then([this, no_of_entries](ret_t ret) {
                       _probe.entries_appended(no_of_entries);
                       return ret;
                   });
             })
      .then([this](ret_t ret) {
          if (_should_fsync == storage::log_append_config::fsync::yes) {
              return _log.appender().flush().then([ret = std::move(ret)] {
                  return make_ready_future<ret_t>(std::move(ret));
              });
          }
          return make_ready_future<ret_t>(std::move(ret));
      });
}

} // namespace raft
