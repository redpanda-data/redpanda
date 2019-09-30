#include "raft/consensus.h"

#include "raft/consensus_utils.h"
#include "raft/logger.h"
#include "seastarx.h"

#include <seastar/core/fstream.hh>

namespace raft {
consensus::consensus(
  model::node_id nid,
  storage::log& l,
  storage::log_append_config::fsync should_fsync,
  io_priority_class io_priority,
  model::timeout_clock::duration disk_timeout,
  sharded<client_cache>& clis)
  : _self(std::move(nid))
  , _log(l)
  , _should_fsync(should_fsync)
  , _io_priority(io_priority)
  , _disk_timeout(disk_timeout)
  , _clients(clis) {
}
void consensus::step_down() {
    _voted_for = {};
    _vstate = vote_state::follower;
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

    /// set to true if the caller's log is as up to date as the recipient's
    /// - extension on raft. see Diego's phd dissertation, section 9.6
    /// - "Preventing disruptions when a server rejoins the cluster"
    reply.log_ok
      = r.prev_log_term > _meta.term
        || (r.prev_log_term == _meta.term && r.prev_log_index >= _meta.commit_index);

    // raft.pdf: reply false if term < currentTerm (§5.1)
    if (r.term < _meta.term) {
        return make_ready_future<vote_reply>(std::move(reply));
    }

    if (r.term > _meta.term) {
        raftlog().info(
          "{}-group recevied vote with larger term:{} than ours:{}",
          _meta.group,
          r.term,
          _meta.term);
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
    reply.term = _meta.term;
    reply.last_log_index = _meta.commit_index;
    reply.success = false;
    // raft.pdf: Reply false if term < currentTerm (§5.1)
    if (r.meta.term < _meta.term) {
        reply.success = false;
        return make_ready_future<append_entries_reply>(std::move(reply));
    }
    if (r.meta.term > _meta.term) {
        raftlog().debug(
          "append_entries request::term:{}  > ours: {}. Setting new term",
          r.meta.term,
          _meta.term);

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
        reply.success = false;
        return make_ready_future<append_entries_reply>(std::move(reply));
    }

    // section 2
    // must come from the same term
    if (r.meta.prev_log_term != _meta.prev_log_term) {
        raftlog().debug("rejecting append_entries missmatching prev_log_term");
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
        return make_ready_future<append_entries_reply>(std::move(reply));
    }

    // call custom serializers/hooks
    for (auto h : _hooks) {
        h->pre_commit(model::offset(_meta.commit_index), r.entries);
    }

    // move the vector and copy the header metadata
    return disk_append(std::move(r.entries))
      .then([this,
             r = std::move(r)](std::vector<storage::log::append_result> ret) {
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
              throw std::runtime_error(fmt::format(
                "Log is now in an inconsistent state. Leader commit_index:{} "
                "vs. our commit_index:{}, for term:{}",
                r.meta.commit_index,
                _meta.commit_index,
                _meta.prev_log_term));
          }
          append_entries_reply reply;
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
consensus::disk_append(std::vector<std::unique_ptr<entry>> entries) {
    using ret_t = std::vector<storage::log::append_result>;
    return do_with(
             std::move(entries),
             [this](std::vector<std::unique_ptr<entry>>& in) {
                 // needs a ref to the range
                 return copy_range<ret_t>(
                   in, [this](std::unique_ptr<entry>& ptr) {
                       return _log.append(
                         std::move(ptr->reader()),
                         storage::log_append_config{
                           // explicit here
                           storage::log_append_config::fsync::no,
                           _io_priority,
                           model::timeout_clock::now() + _disk_timeout});
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
future<> consensus::replicate(std::unique_ptr<entry>) {
    return make_ready_future<>();
}

} // namespace raft
