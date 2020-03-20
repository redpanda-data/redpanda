#pragma once

#include "outcome.h"
#include "raft/consensus.h"
#include "raft/logger.h"

#include <seastar/core/semaphore.hh>

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
    vote_stm(consensus*);
    ~vote_stm();

    /// sends the vote and mutates consensus pointer internal state
    /// it uses the conensus::_op_sem  in 2 places
    /// (1) while writing our configuration
    /// (2) while processing leadership replies
    /// it _does not_ hold the semaphore for the full vote to allow for
    /// staggering processing/vote interruption
    ss::future<> vote();
    ss::future<> wait();

private:
    struct vmeta {
        vmeta(model::node_id n)
          : node(n) {}
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

        model::node_id node;
        std::unique_ptr<result<vote_reply>> value;
    };

    friend std::ostream& operator<<(std::ostream&, const vmeta&);

    ss::future<> do_vote();
    ss::future<> self_vote();
    ss::future<> dispatch_one(model::node_id);
    ss::future<result<vote_reply>> do_dispatch_one(model::node_id);
    std::pair<int32_t, int32_t> partition_count() const;
    ss::future<> process_replies(ss::semaphore_units<>);
    ss::future<> replicate_config_as_new_leader(ss::semaphore_units<>);
    // args
    consensus* _ptr;
    // make sure to always make a copy; never move() this struct
    vote_request _req;

    // for sequentiality/progress
    ss::semaphore _sem;
    // for safety to wait for all bg ops
    ss::gate _vote_bg;
    std::vector<vmeta> _replies;
    raft_ctx_log _ctxlog;
};

} // namespace raft
