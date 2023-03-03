// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/prevote_stm.h"

#include "config/configuration.h"
#include "model/metadata.h"
#include "outcome_future_utils.h"
#include "raft/consensus.h"
#include "raft/consensus_utils.h"
#include "raft/errc.h"
#include "raft/logger.h"
#include "raft/raftgen_service.h"
#include "raft/types.h"
#include "rpc/types.h"
#include "ssx/semaphore.h"

#include <seastar/util/bool_class.hh>

#include <chrono>

namespace raft {

prevote_stm::prevote_stm(consensus* p)
  : _ptr(p)
  , _sem{0, "raft/prevote"}
  , _prevote_timeout(rpc::timeout_spec::none)
  , _ctxlog(_ptr->group(), _ptr->ntp()) {}

prevote_stm::~prevote_stm() {
    vassert(
      _vote_bg.get_count() <= 0 || _vote_bg.is_closed(),
      "Must call vote_stm::wait()");
}

ss::future<result<vote_reply>> prevote_stm::do_dispatch_prevote(vnode n) {
    auto tout_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      _prevote_timeout.timeout_at().time_since_epoch());
    vlog(
      _ctxlog.trace,
      "Sending prevote request to {} from {} with tout: {}",
      n,
      _ptr->_self,
      tout_ms);
    auto r = _req;
    r.target_node_id = n;
    return _ptr->_client_protocol
      .vote(n.id(), std::move(r), rpc::client_opts(_prevote_timeout))
      .then([this, target_node_id = n.id()](result<vote_reply> reply) {
          return _ptr->validate_reply_target_node(
            "prevote", reply, target_node_id);
      });
}

ss::future<>
prevote_stm::process_reply(vnode n, ss::future<result<vote_reply>> f) {
    auto voter_reply = _replies.find(n);

    try {
        if (_prevote_timeout.has_timed_out()) {
            vlog(_ctxlog.trace, "prevote ack from {} timed out", n);
            voter_reply->second._is_failed = true;
            voter_reply->second._is_pending = false;
        } else {
            if (f.failed()) {
                // This can be e.g. a semaphore_timed_out
                vlog(
                  _ctxlog.trace,
                  "prevote ack: node {} threw exception {}",
                  f.get_exception(),
                  n);
                voter_reply->second._is_failed = true;
                voter_reply->second._is_pending = false;
            } else {
                auto r = f.get0();
                if (r.has_value()) {
                    auto v = r.value();
                    _ptr->maybe_update_node_reply_timestamp(n);
                    if (v.log_ok) {
                        vlog(
                          _ctxlog.trace,
                          "prevote ack: node {} caught up with a candidate",
                          n);
                        voter_reply->second._is_ok = true;
                        voter_reply->second._is_pending = false;
                    } else {
                        vlog(
                          _ctxlog.trace,
                          "prevote ack: node {} is ahead of a candidate",
                          n);
                        voter_reply->second._is_failed = true;
                        voter_reply->second._is_pending = false;
                    }
                } else {
                    vlog(
                      _ctxlog.trace,
                      "prevote ack from {} doesn't have value, error: {}",
                      n,
                      r.error().message());
                    voter_reply->second._is_failed = true;
                    voter_reply->second._is_pending = false;
                }
            }
        }
    } catch (...) {
        vlog(
          _ctxlog.trace,
          "error on sending prevote to {} exception: {}",
          n,
          std::current_exception());
        voter_reply->second._is_failed = true;
        voter_reply->second._is_pending = false;
    }
    _sem.signal(1);
    return ss::make_ready_future<>();
}

ss::future<> prevote_stm::dispatch_prevote(vnode n) {
    return with_gate(_vote_bg, [this, n] {
        if (n == _ptr->_self) {
            // skip self prevote
            _sem.signal(1);
            return ss::make_ready_future<>();
        }
        return do_dispatch_prevote(n).then_wrapped(
          [this, n](ss::future<result<vote_reply>> f) {
              return process_reply(n, std::move(f));
          });
    });
}

ss::future<bool> prevote_stm::prevote(bool leadership_transfer) {
    return _ptr->_op_lock
      .with([this, leadership_transfer] {
          // 5.2.1 mark node as candidate, and update leader id
          _ptr->_vstate = consensus::vote_state::candidate;
          //  only trigger notification when we had a leader previosly
          if (_ptr->_leader_id) {
              _ptr->_leader_id = std::nullopt;
              _ptr->trigger_leadership_notification();
          }

          _config = _ptr->config();

          // special case, it may happen that node requesting votes is not a
          // voter, it may happen if it is a learner in previous configuration
          _replies.emplace(_ptr->_self, vmeta{});

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

          _prevote_timeout = rpc::timeout_spec::from_now(
            _ptr->_jit.base_duration());

          auto m = _replies.find(_ptr->self());
          m->second._is_ok = true;
          m->second._is_pending = false;
      })
      .then([this] { return do_prevote(); });
}

ss::future<bool> prevote_stm::do_prevote() {
    // dispatch requests to all voters
    _config->for_each_voter(
      [this](vnode id) { ssx::background = dispatch_prevote(id); });

    // process results
    return process_replies().then([this]() {
        if (_success && _ptr->_node_priority_override == zero_voter_priority) {
            vlog(
              _ctxlog.debug,
              "Ignoring successful pre-vote. Node priority too low: {}",
              _ptr->_node_priority_override.value());
            _success = false;
        }
        return _success;
    });
}

ss::future<> prevote_stm::process_replies() {
    return ss::repeat([this] {
        // majority votes granted
        bool majority_granted = _config->majority(
          [this](vnode id) { return _replies.find(id)->second._is_ok; });

        if (majority_granted) {
            _success = true;
            return ss::make_ready_future<ss::stop_iteration>(
              ss::stop_iteration::yes);
        }

        // majority votes not granted, pre-election not successfull
        bool majority_failed = _config->majority(
          [this](vnode id) { return _replies.find(id)->second._is_failed; });

        if (majority_failed) {
            _success = false;
            return ss::make_ready_future<ss::stop_iteration>(
              ss::stop_iteration::yes);
        }
        // neither majority votes granted nor failed, check if we have all
        // replies (is there any vote request in progress)

        auto has_request_in_progress = std::any_of(
          std::cbegin(_replies), std::cend(_replies), [](const auto& p) {
              return p.second._is_pending;
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

ss::future<> prevote_stm::wait() { return _vote_bg.close(); }

} // namespace raft
