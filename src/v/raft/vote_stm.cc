#include "raft/vote_stm.h"

#include "outcome_future_utils.h"
#include "raft/consensus_utils.h"
#include "raft/errc.h"
#include "raft/logger.h"
#include "raft/raftgen_service.h"

#include <seastar/util/bool_class.hh>

namespace raft {
std::ostream& operator<<(std::ostream& o, const vote_stm::vmeta& m) {
    o << "{value: ";
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
  , _sem(_ptr->config().unique_voter_count())
  , _ctxlog(_ptr->group(), _ptr->ntp()) {}

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
                  auto voter_reply = _replies.find(n);
                  try {
                      auto r = f.get0();
                      vlog(
                        _ctxlog.info, "vote reply from {} - {}", n, r.value());
                      voter_reply->second.set_value(r);
                  } catch (...) {
                      voter_reply->second.set_value(errc::vote_dispatch_error);
                  }
                  if (!voter_reply->second.value) {
                      vlog(
                        _ctxlog.info,
                        "error voting: {}",
                        voter_reply->second.value->error());
                  }
              });
        });
    });
}

std::pair<uint32_t, uint32_t> vote_stm::partition_count() const {
    uint32_t success = 0;
    uint32_t failure = 0;
    for (auto& [id, m] : _replies) {
        auto voting_state = m.get_state();
        if (voting_state == vmeta::state::vote_granted) {
            ++success;
        } else if (voting_state != vmeta::state::in_progress) {
            ++failure;
        }
    }
    return {success, failure};
}
ss::future<> vote_stm::vote(bool leadership_transfer) {
    using skip_vote = ss::bool_class<struct skip_vote_tag>;
    return _ptr->_op_lock
      .with([this, leadership_transfer] {
          // check again while under op_sem
          if (_ptr->should_skip_vote(leadership_transfer)) {
              return ss::make_ready_future<skip_vote>(skip_vote::yes);
          }
          // 5.2.1
          _ptr->_vstate = consensus::vote_state::candidate;
          _ptr->_leader_id = std::nullopt;
          _ptr->trigger_leadership_notification();
          // 5.2.1.2
          _ptr->_term += model::term_id(1);

          // vote is the only method under _op_sem
          _ptr->config().for_each_voter(
            [this](model::node_id id) { _replies.emplace(id, vmeta{}); });
          auto lstats = _ptr->_log.offsets();
          auto last_entry_term = _ptr->get_last_entry_term(lstats);

          _req = vote_request{
            _ptr->_self,
            _ptr->group(),
            _ptr->term(),
            lstats.dirty_offset,
            last_entry_term,
            leadership_transfer};
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
    auto cfg = _ptr->config();

    // TODO: remove dead code
    // 5.2.1. 3
    vote_reply reply;
    reply.term = _req.term;
    reply.log_ok = true;
    // dispatch requests to all voters
    cfg.for_each_voter([this](model::node_id id) { (void)dispatch_one(id); });

    // wait until majority
    const size_t majority = (_ptr->config().unique_voter_count() / 2) + 1;

    return _sem.wait(majority)
      .then([this, cfg = std::move(cfg)]() mutable {
          return process_replies(std::move(cfg));
      })
      // porcess results
      .then([this]() {
          return _ptr->_op_lock.get_units().then(
            [this](ss::semaphore_units<> u) {
                update_vote_state(std::move(u));
            });
      });
}

ss::future<> vote_stm::process_replies(group_configuration cfg) {
    return ss::repeat([this, cfg = std::move(cfg)] {
        // majority votes granted
        bool majority_granted = cfg.majority([this](model::node_id id) {
            return _replies.find(id)->second.get_state()
                   == vmeta::state::vote_granted;
        });

        if (majority_granted) {
            _success = true;
            return ss::make_ready_future<ss::stop_iteration>(
              ss::stop_iteration::yes);
        }

        // majority votes not granted, election not successfull
        bool majority_failed = cfg.majority([this](model::node_id id) {
            auto state = _replies.find(id)->second.get_state();
            // vote not granted and not in progress, it is failed
            return state != vmeta::state::vote_granted
                   && state != vmeta::state::in_progress;
        });

        if (majority_failed) {
            _success = false;
            return ss::make_ready_future<ss::stop_iteration>(
              ss::stop_iteration::yes);
        }
        // neither majority votes granted nor failed, check if we have all
        // replies (is there any vote request in progress)

        auto has_request_in_progress = std::any_of(
          std::cbegin(_replies), std::cend(_replies), [](const auto& p) {
              return p.second.get_state() == vmeta::state::in_progress;
          });

        if (!has_request_in_progress) {
            _success = false;
            return ss::make_ready_future<ss::stop_iteration>(
              ss::stop_iteration::yes);
        }

        return with_gate(_vote_bg, [this] { return _sem.wait(1); }).then([] {
            return ss::stop_iteration::no;
        });
    });
}

ss::future<> vote_stm::wait() { return _vote_bg.close(); }

void vote_stm::update_vote_state(ss::semaphore_units<> u) {
    // use reply term to update voter term
    for (auto& [_, r] : _replies) {
        if (r.value && r.value->has_value()) {
            auto term = r.value->value().term;
            if (term > _ptr->_term) {
                _ptr->_term = term;
            }
        }
    }

    if (_ptr->_vstate != consensus::vote_state::candidate) {
        vlog(_ctxlog.info, "No longer a candidate, ignoring vote replies");
        return;
    }

    if (!_success) {
        vlog(_ctxlog.info, "Vote failed");
        _ptr->_vstate = consensus::vote_state::follower;
        return;
    }

    std::vector<model::node_id> acks;
    for (auto& [id, r] : _replies) {
        if (r.get_state() == vmeta::state::vote_granted) {
            acks.emplace_back(id);
        }
    }
    vlog(_ctxlog.trace, "vote acks in term {} from: {}", _ptr->term(), acks);
    // section vote:5.2.2
    _ptr->_vstate = consensus::vote_state::leader;
    _ptr->_leader_id = _ptr->self();
    _ptr->_became_leader_at = clock_type::now();
    // Set last heartbeat timestamp to max as we are the leader
    _ptr->_hbeat = clock_type::time_point::max();
    vlog(_ctxlog.info, "became the leader term:{}", _ptr->term());

    _ptr->trigger_leadership_notification();
    replicate_config_as_new_leader(std::move(u));
}

void vote_stm::replicate_config_as_new_leader(ss::semaphore_units<> u) {
    (void)ss::with_gate(_ptr->_bg, [this, u = std::move(u)]() mutable {
        return _ptr->replicate_configuration(std::move(u), _ptr->config());
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
      .then([this, reply] {
          auto m = _replies.find(_ptr->self());
          m->second.set_value(reply);
      });
}
} // namespace raft
