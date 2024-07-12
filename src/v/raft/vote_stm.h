/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/outcome.h"
#include "model/fundamental.h"
#include "raft/fwd.h"
#include "raft/group_configuration.h"
#include "raft/logger.h"
#include "raft/types.h"
#include "ssx/semaphore.h"

#include <seastar/core/gate.hh>
#include <seastar/util/bool_class.hh>

#include <absl/container/flat_hash_map.h>

namespace raft {
using is_prevote = ss::bool_class<struct is_prevote_tag>;
/// Section 5.2
/// 1 start election
/// 1.2 increment term
/// 1.3 vote for self
/// 1.4 reset election timer
/// 1.5 send all votes
/// 2 if votes from majority become leader
/// 3 if got append_entries() from new leader, become follower
/// 4 if election timeout elapses, start new lection
///
class vote_stm {
public:
    explicit vote_stm(consensus*, is_prevote = is_prevote::no);
    ~vote_stm();

    /// sends the vote and mutates consensus pointer internal state
    /// it uses the conensus::_op_sem  in 2 places
    /// (1) while writing our configuration
    /// (2) while processing leadership replies
    /// it _does not_ hold the semaphore for the full vote to allow for
    /// staggering processing/vote interruption
    ss::future<election_success> vote(bool leadership_transfer);
    ss::future<> wait();

private:
    struct vmeta {
        /**
         * State represents the response from the voter.
         *
         * in_progress - request pending, waiting for response
         *
         * log_ok - voter log is not longer than candidate log, but vote hasn't
         * been granted
         *
         * vote_granted - vote granted, this implies `log_ok`
         *
         * vote_not_granted - voter didn't cast vote on current candidate
         *
         * error - vote request error
         */
        enum class state {
            in_progress,
            log_ok,
            vote_granted,
            vote_not_granted,
            error,
        };

        explicit vmeta(vote_stm& stm)
          : _vote_stm(stm) {}

        void set_value(result<vote_reply> r) {
            value = std::make_unique<result<vote_reply>>(r);
        }

        state get_state() const {
            // there is no value yet, request is not completed
            if (!value) {
                return state::in_progress;
            }

            if (value->has_value()) {
                if (value->value().term != _vote_stm.request_term()) {
                    return state::vote_not_granted;
                }

                if (value->value().granted) {
                    return state::vote_granted;
                }

                if (value->value().log_ok) {
                    return state::log_ok;
                }
                return state::vote_not_granted;
            }
            // it is an error
            return state::error;
        }

        bool has_more_up_to_date_log() const {
            return value && value->has_value() && !value->value().log_ok;
        }
        std::unique_ptr<result<vote_reply>> value;
        vote_stm& _vote_stm;
    };

    bool has_request_in_progress() const;

    bool can_wait_for_all() const;

    model::term_id request_term() const { return _req.term; }

    ss::future<> wait_for_next_reply();

    void fail_election();

    friend std::ostream& operator<<(std::ostream&, const vmeta&);

    ss::future<election_success> do_vote();
    ss::future<> self_vote();
    ss::future<> dispatch_one(vnode);
    ss::future<result<vote_reply>> do_dispatch_one(vnode);
    ss::future<> update_vote_state(ssx::semaphore_units);
    ss::future<> process_replies();
    ss::future<std::error_code>
      replicate_config_as_new_leader(ssx::semaphore_units);
    // args
    consensus* _ptr;
    is_prevote _prevote = is_prevote::no;
    // make sure to always make a copy; never move() this struct
    vote_request _req;
    bool _success = false;
    // for sequentiality/progress
    ssx::semaphore _sem;
    std::optional<raft::group_configuration> _config;
    // for safety to wait for all bg ops
    ss::gate _vote_bg;
    absl::flat_hash_map<vnode, vmeta> _replies;
    ctx_log _ctxlog;
    clock_type::time_point _requests_dispatched_ts;
};

} // namespace raft
