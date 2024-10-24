// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/vote_stm.h"

#include "base/vassert.h"
#include "raft/consensus.h"
#include "raft/errc.h"
#include "raft/fundamental.h"
#include "raft/logger.h"
#include "rpc/types.h"
#include "ssx/semaphore.h"

#include <seastar/core/timed_out_error.hh>
#include <seastar/core/with_timeout.hh>
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
vote_stm::vote_stm(consensus* p, is_prevote prevote)
  : _ptr(p)
  , _prevote(prevote)
  , _sem(0, "raft/vote")
  , _ctxlog(_ptr->_ctxlog) {}

vote_stm::~vote_stm() {
    vassert(
      _vote_bg.get_count() <= 0 || _vote_bg.is_closed(),
      "Must call vote_stm::wait()");
}
ss::future<result<vote_reply>> vote_stm::do_dispatch_one(vnode n) {
    auto tout = _ptr->_jit.base_duration() * 3;
    vlog(
      _ctxlog.info,
      "[pre-vote: {}] sending vote request to {} with timeout of {} ms",
      _prevote,
      n,
      tout / 1ms);

    auto r = _req;
    /**
     * Do not increment vote request probe when pre-voting to keep the same
     * semantics of metric
     */
    if (!_prevote) {
        _ptr->_probe->vote_request_sent();
    }
    r.target_node_id = n;
    return _ptr->_client_protocol
      .vote(
        n.id(),
        std::move(r),
        rpc::client_opts(rpc::timeout_spec::from_now(tout)))
      .then([this, target_node_id = n.id()](result<vote_reply> reply) {
          return _ptr->validate_reply_target_node(
            "vote_request", reply, target_node_id);
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
                  auto r = f.get();
                  if (r.has_value()) {
                      vlog(
                        _ctxlog.info,
                        "[pre-vote: {}] vote reply from {} - {}",
                        _prevote,
                        n,
                        r.value());

                      _ptr->maybe_update_node_reply_timestamp(n);
                  } else {
                      vlog(
                        _ctxlog.info,
                        "[pre-vote: {}] vote error from {} - {}",
                        _prevote,
                        n,
                        r.error().message());
                  }
                  voter_reply->second.set_value(r);
              } catch (...) {
                  vlog(
                    _ctxlog.info,
                    "[pre-vote: {}] vote dispatch to node {} error - {}",
                    _prevote,
                    n,
                    std::current_exception());
                  voter_reply->second.set_value(errc::vote_dispatch_error);
              }
              _sem.signal(1);
          });
    });
}

ss::future<election_success> vote_stm::vote(bool leadership_transfer) {
    enum class prepare_election_result {
        skip_election,
        proceed_with_election,
        immediate_success,
    };
    return _ptr->_election_lock.with([this, leadership_transfer] {
        return _ptr->_op_lock
          .with([this, leadership_transfer] {
              _config = _ptr->config();
              // check again while under op_sem
              if (_ptr->should_skip_vote(leadership_transfer)) {
                  return ss::make_ready_future<prepare_election_result>(
                    prepare_election_result::skip_election);
              }
              // 5.2.1 mark node as candidate, and update leader id
              _ptr->_vstate = consensus::vote_state::candidate;
              //  only trigger notification when we had a leader previously
              if (_ptr->_leader_id) {
                  _ptr->_leader_id = std::nullopt;
                  _ptr->trigger_leadership_notification();
              }

              if (_prevote && leadership_transfer) {
                  return ssx::now(prepare_election_result::immediate_success);
              }

              // 5.2.1.2
              /**
               * Pre-voting doesn't increase the term
               */
              if (!_prevote) {
                  _ptr->_term += model::term_id(1);
                  _ptr->_voted_for = {};
              }

              // special case, it may happen that node requesting votes is not a
              // voter, it may happen if it is a learner in previous
              // configuration
              _replies.emplace(_ptr->_self, *this);

              // vote is the only method under _op_sem
              _config->for_each_voter(
                [this](vnode id) { _replies.emplace(id, *this); });

              auto lstats = _ptr->_log->offsets();
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
              return self_vote().then(
                [] { return prepare_election_result::proceed_with_election; });
          })
          .then([this](prepare_election_result result) {
              switch (result) {
              case prepare_election_result::skip_election:
                  return ss::make_ready_future<election_success>(
                    election_success::no);
              case prepare_election_result::proceed_with_election:
                  return do_vote();
              case prepare_election_result::immediate_success:
                  return ss::make_ready_future<election_success>(
                    election_success::yes);
              }
          });
    });
}

ss::future<election_success> vote_stm::do_vote() {
    // dispatch requests to all voters
    _config->for_each_voter([this](vnode id) { (void)dispatch_one(id); });
    _requests_dispatched_ts = clock_type::now();
    co_await process_replies();

    auto u = co_await _ptr->_op_lock.get_units();
    co_await update_vote_state(std::move(u));

    co_return election_success(_success);
}

bool vote_stm::has_request_in_progress() const {
    return std::any_of(
      _replies.begin(),
      _replies.end(),
      [](const absl::flat_hash_map<vnode, vmeta>::value_type& p) {
          return p.second.get_state() == vmeta::state::in_progress;
      });
}

bool vote_stm::can_wait_for_all() const {
    return clock_type::now()
           < _requests_dispatched_ts + _ptr->_jit.base_duration();
}

ss::future<> vote_stm::process_replies() {
    return ss::repeat([this] {
        const bool request_in_progress = has_request_in_progress();

        /**
         * Try to wait for vote replies from all of the nodes or use information
         * from all replies if it is already available. Waiting for all the
         * replies allow candidate to verify if any other protocol participant
         * has log which is longer.
         *
         * If any of the replicas log is longer than current candidate the
         * election is failed allowing the other node to become a leader.
         */
        if (
          (!request_in_progress || can_wait_for_all())
          && _ptr->_enable_longest_log_detection()) {
            if (request_in_progress) {
                return wait_for_next_reply().then(
                  [] { return ss::stop_iteration::no; });
            }
            auto is_more_up_to_date_it = std::find_if(
              _replies.begin(),
              _replies.end(),
              [](const absl::flat_hash_map<vnode, vmeta>::value_type& p) {
                  return p.second.has_more_up_to_date_log();
              });

            if (is_more_up_to_date_it != _replies.end()) {
                vlog(
                  _ctxlog.info,
                  "[pre-vote {}] vote failed - node {} has log which is more "
                  "up to date than the current candidate",
                  _prevote,
                  is_more_up_to_date_it->first);
                _success = false;
                return ss::make_ready_future<ss::stop_iteration>(
                  ss::stop_iteration::yes);
            }
        }
        /**
         * The check if the election is successful is different for prevote, for
         * prevote phase to be successful it is enough that followers reported
         * log_ok flag. For the actual election we require that vote is granted.
         */
        auto majority_granted = _config->majority([this](const vnode& id) {
            const auto state = _replies.find(id)->second.get_state();
            if (_prevote) {
                return state == vmeta::state::log_ok
                       || state == vmeta::state::vote_granted;
            }
            return state == vmeta::state::vote_granted;
        });

        if (majority_granted) {
            _success = true;
            return ss::make_ready_future<ss::stop_iteration>(
              ss::stop_iteration::yes);
        }

        // majority votes not granted, election not successful
        auto majority_failed = _config->majority([this](const vnode& id) {
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

        if (!request_in_progress) {
            _success = false;
            return ss::make_ready_future<ss::stop_iteration>(
              ss::stop_iteration::yes);
        }

        return with_gate(_vote_bg, [this] { return _sem.wait(1); }).then([] {
            return ss::stop_iteration::no;
        });
    });
}

ss::future<> vote_stm::wait_for_next_reply() {
    vlog(
      _ctxlog.debug,
      "[pre-vote {}] trying to wait for vote replies from all of "
      "the nodes",
      _prevote);
    auto h = _vote_bg.hold();
    const auto timeout = std::max(
      clock_type::duration(0),
      _ptr->_jit.base_duration()
        - (clock_type::now() - _requests_dispatched_ts));

    /**
     * Return early if we are not allowed to wait
     */
    if (timeout == clock_type::duration(0)) {
        co_return;
    }
    /**
     * The wait for all replies loop will exit if it can not longer wait for
     * replies regardless of the result of this particular response semaphore
     * wait.
     */
    try {
        co_await _sem.wait(timeout, 1);
    } catch (const ss::semaphore_timed_out&) {
        vlog(
          _ctxlog.debug,
          "[pre-vote {}] timed out waiting for all the replies",
          _prevote);
    }
}

ss::future<> vote_stm::wait() { return _vote_bg.close(); }

ss::future<> vote_stm::update_vote_state(ssx::semaphore_units u) {
    // use reply term to update our term
    for (auto& [_, r] : _replies) {
        if (r.value && r.value->has_value()) {
            auto term = r.value->value().term;
            if (term > _ptr->_term) {
                vlog(
                  _ctxlog.info,
                  "[pre-vote {}] vote failed - received larger term: {}",
                  _prevote,
                  term);
                _ptr->_term = term;
                _ptr->_voted_for = {};
                fail_election();
                co_return;
            }
        }
    }
    /**
     * Use cached term value stored while the vote_stm owned an oplock in first
     * voting phase. (the term might have changed if a node received request
     * from other leader)
     */
    auto term = request_term();
    if (
      _ptr->_vstate != consensus::vote_state::candidate
      || _ptr->_term != term) {
        vlog(
          _ctxlog.info,
          "[prev-vote {}] no longer a candidate, ignoring vote replies",
          _prevote);
        co_return;
    }
    if (!_success) {
        vlog(_ctxlog.info, "[pre-vote: {}] vote failed", _prevote);
        fail_election();
        co_return;
    }
    /**
     * Pre-voting process finishes before updating Raft state.
     */
    if (_prevote) {
        co_return;
    }

    const auto only_voter = _config->unique_voter_count() == 1
                            && _config->is_voter(_ptr->self());
    if (!only_voter && _ptr->_node_priority_override == zero_voter_priority) {
        vlog(
          _ctxlog.debug,
          "[pre-vote: false] Ignoring successful vote. Node priority too low: "
          "{}",
          _ptr->_node_priority_override.value());
        fail_election();
        co_return;
    }

    std::vector<vnode> acks;
    for (auto& [id, r] : _replies) {
        if (r.get_state() == vmeta::state::vote_granted) {
            acks.emplace_back(id);
        }
    }

    vlog(_ctxlog.trace, "vote acks in term {} from: {}", term, acks);
    // section vote:5.2.2
    _ptr->_vstate = consensus::vote_state::leader;
    _ptr->_follower_recovery_state.reset();
    _ptr->_leader_id = _ptr->self();
    // reset target priority
    _ptr->_target_priority = voter_priority::max();
    _ptr->_became_leader_at = clock_type::now();
    // Set last heartbeat timestamp to max as we are the leader
    _ptr->_hbeat = clock_type::time_point::max();
    vlog(_ctxlog.info, "becoming the leader term:{}", term);
    _ptr->_last_quorum_replicated_index_with_flush = _ptr->_flushed_offset;

    auto ec = co_await replicate_config_as_new_leader(std::move(u));

    // even if failed to replicate, don't step down: followers may be behind
    if (ec) {
        vlog(
          _ctxlog.info,
          "unable to replicate configuration as a leader - error code: "
          "{} - {} ",
          ec.value(),
          ec.message());
    } else {
        if (term == _ptr->_term) {
            vlog(_ctxlog.info, "became the leader term: {}", term);
            vassert(
              _ptr->_confirmed_term == _ptr->_term,
              "successfully replicated configuration should update "
              "_confirmed_term={} to be equal to _term={}",
              _ptr->_confirmed_term,
              _ptr->_term);
        }
    }
}

ss::future<std::error_code>
vote_stm::replicate_config_as_new_leader(ssx::semaphore_units u) {
    // we use long timeout of 5 * base election timeout to trigger it only when
    // raft group is stuck.
    const auto deadline = clock_type::now() + 5 * _ptr->_jit.base_duration();
    return ss::with_timeout(
             deadline,
             _ptr->replicate_configuration(std::move(u), _config.value()))
      .handle_exception_type([](const ss::timed_out_error&) {
          return make_error_code(errc::timeout);
      });
}

ss::future<> vote_stm::self_vote() {
    vote_reply reply;
    reply.term = request_term();
    reply.log_ok = true;
    reply.granted = true;

    vlog(
      _ctxlog.trace,
      "[pre-vote: {}] voting for self in term {}",
      _prevote,
      request_term());
    /**
     * If this is the actual vote phase, write voted_for
     */
    if (!_prevote) {
        _ptr->_voted_for = _ptr->_self;
        co_await _ptr->write_voted_for({_ptr->_self, request_term()});
    }

    auto m = _replies.find(_ptr->self());
    m->second.set_value(reply);
}

void vote_stm::fail_election() {
    vassert(
      _ptr->_vstate != consensus::vote_state::leader
        && _ptr->_hbeat != clock_type::time_point::max(),
      "Became a leader outside current election");
    _ptr->_vstate = consensus::vote_state::follower;
}

} // namespace raft
