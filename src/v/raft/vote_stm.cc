#include "raft/vote_stm.h"

#include "outcome_future_utils.h"
#include "raft/consensus_utils.h"
#include "raft/errc.h"
#include "raft/logger.h"
#include "raft/raftgen_service.h"

#include <seastar/util/bool_class.hh>

namespace raft {
std::ostream& operator<<(std::ostream& o, const vote_stm::vmeta& m) {
    o << "{node: " << m.node << ", value: ";
    if (m.value) {
        if (m.value->has_error()) {
            auto& e = m.value->error();
            o << "{" << *m.value << ", message: " << e.message() << "}";
        } else {
            o << "[" << *m.value << "]";
        }
    } else {
        o << "nullptr";
    }
    return o << "}";
}
vote_stm::vote_stm(consensus* p)
  : _ptr(p)
  , _sem(_ptr->_conf.nodes.size())
  , _ctxlog(_ptr->_self, _ptr->group()) {}

vote_stm::~vote_stm() {
    if (_vote_bg.get_count() > 0 && !_vote_bg.is_closed()) {
        vlog(_ctxlog.error, "Must call vote_stm::wait()");
        std::terminate();
    }
}
ss::future<result<vote_reply>> vote_stm::do_dispatch_one(model::node_id n) {
    vlog(_ctxlog.trace, "Sending vote request to {}", n);
    auto tout = clock_type::now() + _ptr->_jit.base_duration();

    auto r = _req;
    _ptr->_probe.vote_request_sent();
    return _ptr->_client_protocol.vote(n, std::move(r), rpc::client_opts(tout));
}

ss::future<> vote_stm::dispatch_one(model::node_id n) {
    return with_gate(_vote_bg, [this, n] {
        return with_semaphore(_sem, 1, [this, n] {
            if (n == _ptr->_self) {
                // skip self vote
                return ss::make_ready_future<>();
            }
            return do_dispatch_one(n).then_wrapped(
              [this, n](ss::future<result<vote_reply>> f) {
                  auto voter_reply = std::find_if(
                    _replies.begin(), _replies.end(), [n](vmeta& mi) {
                        return mi.node == n;
                    });
                  try {
                      auto r = f.get0();
                      voter_reply->set_value(std::move(r));
                  } catch (...) {
                      voter_reply->set_value(errc::vote_dispatch_error);
                  }
                  if (!voter_reply->value) {
                      vlog(_ctxlog.info, "error voting: {}", *voter_reply);
                  }
              });
        });
    });
}

std::pair<uint32_t, uint32_t> vote_stm::partition_count() const {
    uint32_t success = 0;
    uint32_t failure = 0;
    for (auto& m : _replies) {
        auto voting_state = m.get_state();
        if (voting_state == vmeta::state::vote_granted) {
            ++success;
        } else if (voting_state != vmeta::state::in_progress) {
            ++failure;
        }
    }
    return {success, failure};
}
ss::future<> vote_stm::vote() {
    using skip_vote = ss::bool_class<struct skip_vote_tag>;
    return _ptr->_op_lock
      .with([this] {
          // check again while under op_sem
          if (_ptr->should_skip_vote()) {
              return ss::make_ready_future<skip_vote>(skip_vote::yes);
          }
          // 5.2.1
          _ptr->_vstate = consensus::vote_state::candidate;
          _ptr->_leader_id = std::nullopt;
          _ptr->trigger_leadership_notification();
          // 5.2.1.2
          _ptr->_term += model::term_id(1);

          // vote is the only method under _op_sem
          for (auto& n : _ptr->_conf.nodes) {
              _replies.emplace_back(vmeta(n.id()));
          }
          auto lstats = _ptr->_log.offsets();
          _req = vote_request{_ptr->_self,
                              _ptr->group(),
                              _ptr->term(),
                              lstats.dirty_offset,
                              lstats.dirty_offset_term};
          // we have to self vote before dispatching vote request to
          // other nodes, this vote has to be done under op semaphore as
          // it changes voted_for state
          return self_vote().then([] { return skip_vote::no; });
      })
      .then([this](skip_vote skip) {
          if (skip) {
              return ss::make_ready_future<>();
          }
          return do_vote();
      });
}
ss::future<> vote_stm::do_vote() {
    auto& cfg = _ptr->_conf;

    // 5.2.1. 3
    vote_reply reply;
    reply.term = _req.term;
    reply.log_ok = true;

    for (auto& n : cfg.nodes) {
        (void)dispatch_one(n.id()); // background
    }
    // wait until majority or all
    const size_t majority = cfg.majority();
    const size_t all = cfg.nodes.size();
    return _sem.wait(majority)
      .then([this, majority, all] {
          return ss::do_until(
            [this, majority, all] {
                auto [success, failure] = partition_count();
                vlog(
                  _ctxlog.trace,
                  "Vote results [success:{}, failures:{}, majority: {}]",
                  success,
                  failure,
                  majority);
                return success >= majority || failure >= majority
                       || (success + failure) >= all;
            },
            [this] {
                return with_gate(_vote_bg, [this] { return _sem.wait(1); });
            });
      })
      // porcess results
      .then([this]() {
          return _ptr->_op_lock.get_units().then(
            [this](ss::semaphore_units<> u) {
                return process_replies(std::move(u));
            });
      });
}
ss::future<> vote_stm::wait() { return _vote_bg.close(); }

ss::future<> vote_stm::process_replies(ss::semaphore_units<> u) {
    const size_t majority = _ptr->_conf.majority();
    auto [success, failure] = partition_count();
    if (_ptr->_vstate != consensus::vote_state::candidate) {
        vlog(
          _ctxlog.info,
          "No longer a candidate, ignoring vote replies: {}/{}",
          success,
          _ptr->_conf.nodes.size());
        return ss::make_ready_future<>();
    }
    if (success < majority) {
        vlog(
          _ctxlog.info,
          "Majority vote failed. {}/{} votes, need:{}",
          success,
          _ptr->_conf.nodes.size(),
          majority);
        _ptr->_vstate = consensus::vote_state::follower;
        return ss::make_ready_future<>();
    }
    std::vector<model::node_id> acks;
    acks.reserve(success);
    for (auto& r : _replies) {
        if (r.get_state() == vmeta::state::vote_granted) {
            acks.emplace_back(r.node);
        }
    }
    vlog(_ctxlog.trace, "vote acks in term {} from: {}", _ptr->term(), acks);
    // section vote:5.2.2
    _ptr->_vstate = consensus::vote_state::leader;
    _ptr->_leader_id = _ptr->self();
    // Set last heartbeat timestamp to max as we are the leader
    _ptr->_hbeat = clock_type::time_point::max();
    vlog(_ctxlog.info, "became the leader term:{}", _ptr->term());

    _ptr->trigger_leadership_notification();
    replicate_config_as_new_leader(std::move(u));
    return ss::make_ready_future<>();
}

void vote_stm::replicate_config_as_new_leader(ss::semaphore_units<> u) {
    (void)ss::with_gate(_ptr->_bg, [this, u = std::move(u)]() mutable {
        return _ptr->replicate_configuration(std::move(u), _ptr->_conf);
    });
}

ss::future<> vote_stm::self_vote() {
    vote_reply reply;
    reply.term = _req.term;
    reply.log_ok = true;
    reply.granted = true;

    vlog(_ctxlog.trace, "Voting for self in term {}", _req.term);
    _ptr->_voted_for = _ptr->_self;
    return _ptr->write_voted_for({_ptr->_self, model::term_id(_req.term)})
      .then([this, reply = std::move(reply)] {
          auto m = std::find_if(
            _replies.begin(), _replies.end(), [this](vmeta& mi) {
                return mi.node == _ptr->_self;
            });
          m->set_value(std::move(reply));
      });
}
} // namespace raft
