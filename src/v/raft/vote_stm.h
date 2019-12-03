#pragma once

#include "raft/consensus.h"

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
    future<> vote();
    future<> wait();

private:
    struct vmeta {
        vmeta(model::node_id n)
          : node(n) {}
        vmeta(vmeta&&) noexcept = default;
        vmeta& operator=(vmeta&&) noexcept = default;

        bool is_set() const { return value.index() != std::variant_npos; }
        bool is_error() const { return std::holds_alternative<sstring>(value); }
        bool is_reply() const {
            return std::holds_alternative<vote_reply>(value);
        }
        bool is_vote_granted_reply() const {
            return is_reply() && std::get<vote_reply>(value).granted;
        }

        model::node_id node;
        std::variant<vote_reply, sstring> value;
    };
    friend std::ostream& operator<<(std::ostream&, const vmeta&);

    future<> do_vote();
    future<> self_vote();
    future<> dispatch_one(model::node_id);
    future<vote_reply> do_dispatch_one(model::node_id);
    std::pair<int32_t, int32_t> partition_count() const;
    future<> process_replies();
    future<> replicate_config_as_new_leader();
    // args
    consensus* _ptr;
    // make sure to always make a copy; never move() this struct
    vote_request _req;

    // for sequentiality/progress
    seastar::semaphore _sem;
    // for safety to wait for all bg ops
    seastar::gate _vote_bg;
    std::vector<vmeta> _replies;
};

} // namespace raft
