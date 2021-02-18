// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/vote_stm.h"

#include "outcome_future_utils.h"
#include "raft/consensus.h"
#include "raft/consensus_utils.h"
#include "raft/errc.h"
#include "raft/logger.h"
#include "raft/raftgen_service.h"
#include "vassert.h"

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
  , _sem(0)
  , _ctxlog(_ptr->_ctxlog) {}

vote_stm::~vote_stm() {
    vassert(
      _vote_bg.get_count() <= 0 || _vote_bg.is_closed(),
      "Must call vote_stm::wait()");
}
ss::future<result<vote_reply>> vote_stm::do_dispatch_one(vnode n) {
    vlog(_ctxlog.trace, "Sending vote request to {}", n);
    auto tout = clock_type::now() + _ptr->_jit.base_duration();

    auto r = _req;
    _ptr->_probe.vote_request_sent();
    r.target_node_id = n;
    return _ptr->_client_protocol
      .vote(n.id(), std::move(r), rpc::client_opts(tout))
      .then([this](result<vote_reply> reply) {
          return _ptr->validate_reply_target_node(
            "vote_request", std::move(reply));
      });
}

ss::future<> vote_stm::dispatch_one(vnode n) {
    return with_gate(_vote_bg, [this, n] {
        if (n == _ptr->_self) {
            // skip self vote
            _sem.signal(1);
            return ss::make_ready_future<>();
        }
        return do_dispatch_one(n).then_wrapped(
          [this, n](ss::future<result<vote_reply>> f) {
              auto voter_reply = _replies.find(n);
              try {
                  auto r = f.get0();
                  vlog(_ctxlog.info, "vote reply from {} - {}", n, r.value());
                  voter_reply->second.set_value(r);
              } catch (...) {
                  voter_reply->second.set_value(errc::vote_dispatch_error);
              }
              _sem.signal(1);
              if (!voter_reply->second.value) {
                  vlog(
                    _ctxlog.info,
                    "error voting: {}",
                    voter_reply->second.value->error());
              }
          });
    });
}

ss::future<> vote_stm::vote(bool leadership_transfer) {
    using skip_vote = ss::bool_class<struct skip_vote_tag>;
    return _ptr->_op_lock
      .with([this, leadership_transfer] {
          _config = _ptr->config();
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
          _ptr->_voted_for = {};

          // vote is the only method under _op_sem
          _config->for_each_voter(
            [this](vnode id) { _replies.emplace(id, vmeta{}); });
          auto lstats = _ptr->_log.offsets();
          auto last_entry_term = _ptr->get_last_entry_term(lstats);

          _req = vote_request{
            .node_id = _ptr->_self,
            .group = _ptr->group(),
            .term = _ptr->term(),
            .prev_log_index = lstats.dirty_offset,
            .prev_log_term = last_entry_term,
            .leadership_transfer = leadership_transfer};
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
    // dispatch requests to all voters
    _config->for_each_voter([this](vnode id) { (void)dispatch_one(id); });

    // wait until majority
    const size_t majority = (_config->unique_voter_count() / 2) + 1;

    return _sem.wait(majority)
      .then([this] { return process_replies(); })
      // porcess results
      .then([this]() {
          return _ptr->_op_lock.get_units().then(
            [this](ss::semaphore_units<> u) {
                return update_vote_state(std::move(u));
            });
      });
}

ss::future<> vote_stm::process_replies() {
    return ss::repeat([this] {
        // majority votes granted
        bool majority_granted = _config->majority([this](vnode id) {
            return _replies.find(id)->second.get_state()
                   == vmeta::state::vote_granted;
        });

        if (majority_granted) {
            _success = true;
            return ss::make_ready_future<ss::stop_iteration>(
              ss::stop_iteration::yes);
        }

        // majority votes not granted, election not successfull
        bool majority_failed = _config->majority([this](vnode id) {
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

ss::future<> vote_stm::update_vote_state(ss::semaphore_units<> u) {
    // use reply term to update voter term
    for (auto& [_, r] : _replies) {
        if (r.value && r.value->has_value()) {
            auto term = r.value->value().term;
            if (term > _ptr->_term) {
                vlog(
                  _ctxlog.info, "Vote failed - received larger term: {}", term);
                _ptr->_term = term;
                _ptr->_voted_for = {};
                _ptr->_vstate = consensus::vote_state::follower;
                return ss::now();
            }
        }
    }

    if (_ptr->_vstate != consensus::vote_state::candidate) {
        vlog(_ctxlog.info, "No longer a candidate, ignoring vote replies");
        return ss::now();
    }

    if (!_success) {
        vlog(_ctxlog.info, "Vote failed");
        _ptr->_vstate = consensus::vote_state::follower;
        return ss::now();
    }

    std::vector<vnode> acks;
    for (auto& [id, r] : _replies) {
        if (r.get_state() == vmeta::state::vote_granted) {
            acks.emplace_back(id);
        }
    }
    vlog(_ctxlog.trace, "vote acks in term {} from: {}", _ptr->term(), acks);
    // section vote:5.2.2
    _ptr->_vstate = consensus::vote_state::leader;
    _ptr->_leader_id = _ptr->self();
    // reset target priority
    _ptr->_target_priority = voter_priority::max();
    _ptr->_became_leader_at = clock_type::now();
    // Set last heartbeat timestamp to max as we are the leader
    _ptr->_hbeat = clock_type::time_point::max();
    vlog(_ctxlog.info, "became the leader term:{}", _ptr->term());
    _ptr->_last_quorum_replicated_index = _ptr->_log.offsets().committed_offset;
    _ptr->trigger_leadership_notification();

    return replicate_config_as_new_leader(std::move(u))
      .then([this](std::error_code ec) {
          // if we didn't replicated configuration, step down
          if (ec) {
              vlog(
                _ctxlog.info,
                "unable to replicate configuration as a leader, stepping down");
              return _ptr->step_down(_ptr->_term + model::term_id(1));
          }
          return ss::now();
      });
}

ss::future<std::error_code>
vote_stm::replicate_config_as_new_leader(ss::semaphore_units<> u) {
    return _ptr->replicate_configuration(std::move(u), _config.value());
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
