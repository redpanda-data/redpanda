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

#include "outcome.h"
#include "raft/logger.h"
#include "raft/types.h"
#include "ssx/semaphore.h"

#include <seastar/core/gate.hh>

#include <absl/container/flat_hash_map.h>

#include <variant>
#include <vector>

namespace raft {
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
    explicit vote_stm(consensus*);
    ~vote_stm();

    /// sends the vote and mutates consensus pointer internal state
    /// it uses the conensus::_op_sem  in 2 places
    /// (1) while writing our configuration
    /// (2) while processing leadership replies
    /// it _does not_ hold the semaphore for the full vote to allow for
    /// staggering processing/vote interruption
    ss::future<> vote(bool leadership_transfer);
    ss::future<> wait();

private:
    struct vmeta {
        enum class state {
            in_progress,
            vote_granted,
            vote_not_granted,
            error,
        };

        void set_value(result<vote_reply> r) {
            value = std::make_unique<result<vote_reply>>(std::move(r));
        }

        state get_state() const {
            // there is no value yet, request is not completed
            if (!value) {
                return state::in_progress;
            }

            // we have value, vote is either granted or not
            if (value->has_value()) {
                return value->value().granted ? state::vote_granted
                                              : state::vote_not_granted;
            }
            // it is an error
            return state::error;
        }
        std::unique_ptr<result<vote_reply>> value;
    };

    friend std::ostream& operator<<(std::ostream&, const vmeta&);

    ss::future<> do_vote();
    ss::future<> self_vote();
    ss::future<> dispatch_one(vnode);
    ss::future<result<vote_reply>> do_dispatch_one(vnode);
    ss::future<> update_vote_state(ssx::semaphore_units);
    ss::future<> process_replies();
    ss::future<std::error_code>
      replicate_config_as_new_leader(ssx::semaphore_units);
    // args
    consensus* _ptr;
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
};

} // namespace raft
