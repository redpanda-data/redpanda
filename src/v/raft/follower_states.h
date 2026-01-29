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

#include "absl/container/node_hash_map.h"
#include "base/vassert.h"
#include "raft/types.h"
#include "ssx/single_fiber_executor.h"

namespace raft {
struct follower_index_metadata {
    explicit follower_index_metadata(vnode node)
      : node_id(node)
      , coordinated_compaction_offsets_getter{std::make_unique<void_executor>()}
      , coordinated_compaction_offsets_sender{
          std::make_unique<void_executor>()} {}

    follower_index_metadata(const follower_index_metadata&) = delete;
    follower_index_metadata& operator=(const follower_index_metadata&) = delete;
    follower_index_metadata(follower_index_metadata&&) = default;
    follower_index_metadata& operator=(follower_index_metadata&&) = delete;
    // resets the follower state i.e. all indices and sequence numbers
    void reset();

    bool has_inflight_appends() const {
        return inflight_append_request_count > 0;
    }

    follower_req_seq next_follower_sequence() { return ++last_sent_seq; }

    static bool is_first_request(follower_req_seq seq) { return seq() == 1; }

    vnode node_id;
    // index of last known log for this follower
    model::offset last_flushed_log_index;
    // index of last not flushed offset
    model::offset last_dirty_log_index;
    // index of log for which leader and follower logs matches
    model::offset match_index;
    // Used to establish index persistently replicated by majority
    constexpr model::offset match_committed_index() const {
        return std::min(last_flushed_log_index, match_index);
    }
    // next index to send to this follower
    model::offset next_index;
    // field indicating end offset of follower log after current pending
    // append_entries_requests are successfully delivered and processed by the
    // follower.
    model::offset expected_log_end_offset;
    // timestamp of last append_entries_rpc call
    clock_type::time_point last_sent_append_entries_req_timestamp;
    clock_type::time_point last_received_reply_timestamp;
    uint32_t heartbeats_failed{0};
    // The pair of sequences used to track append entries requests sent and
    // received by the follower. Every time append entries request is created
    // the `last_sent_seq` is incremented before accessing raft protocol state
    // and dispatching an RPC and its value is passed to the response
    // processing continuation. When follower append entries replies are
    // received if the sequence bound with reply is greater than or equal to
    // `last_received_seq` the `last_received_seq` field is updated with
    // received sequence and reply is treated as valid. If received sequence is
    // smaller than `last_received_seq` requests were reordered.

    /// Using `follower_req_seq` argument to track the follower replies
    /// reordering
    ///
    ///                                                                    Time
    ///                                                        Follower     +
    ///                                                           +         |
    ///                      +--------------+                     |         |
    ///                      | Req [seq: 1] +-------------------->+         |
    ///                      +--------------+                     |         |
    ///                           +--------------+                |         |
    ///                           | Req [seq: 2] +--------------->+         |
    ///                           +--------------+                |         |
    ///                                +--------------+           |         |
    ///                                | Req [seq: 3] +---------->+         |
    ///                                +--------------+           |         |
    ///                                                           |         |
    ///                                                           |         |
    ///                                                           |         |
    ///                                        Reply [seq: 1]     |         |
    /// last_received_seq = 1;    <-------------------------------+         |
    ///                                                           |         |
    ///                                                           |         |
    ///                                        Reply [seq: 3]     |         |
    /// last_received_seq = 3;    <-------------------------------+         |
    ///                                                           |         |
    ///                                                           |         |
    ///                                        Reply [seq: 2]     |         |
    /// reordered 2 < last_rec    <-------------------------------+         |
    ///                                                           |         |
    ///                                                           |         |
    ///                                                           |         |
    ///                                                           |         |
    ///                                                           |         |
    ///                                                           +         |
    ///                                                                     v

    follower_req_seq last_sent_seq{0};
    follower_req_seq last_received_seq{0};
    // sequence number of last received successful append entries request
    follower_req_seq last_successful_received_seq{0};
    bool is_learner = true;
    bool is_recovering = false;

    /*
     * When is_recovering is true a fiber may wait for recovery to be signaled
     * on the recovery_finished condition variable.
     */
    ss::condition_variable recovery_finished;
    /**
     * When recovering, the recovery state machine will wait on this condition
     * variable to wait for changes that may alter recovery state when all
     * necessary requests were already dispatched to the follower. This
     * condition variable is used not to make the recovery loop tight when
     * checking if recovery may be finished
     */
    ss::condition_variable follower_state_change;

    /**
     * We prevent race conditions by counting the number of suppressing requests
     * in flight.
     */
    size_t inflight_append_request_count = 0;

    std::optional<protocol_metadata> last_sent_protocol_meta;

    /**
     * Coordinated compaction details
     */
    // in the follower log; model::offset{} if follower never reported its MCCO
    model::offset max_cleanly_compacted_offset;
    // in the follower log; model::offset{} if follower never reported its MXFO
    model::offset max_transaction_free_offset;
    using void_executor = ssx::single_fiber_executor<
      ss::noncopyable_function<ss::future<>(ss::abort_source&)>>;
    using void_executor_ptr = std::unique_ptr<void_executor>;
    // state for requesting latest MCCO and MXFO calculated on the follower
    void_executor_ptr coordinated_compaction_offsets_getter;
    // state for sending latest MTRO and MXRO to the follower
    void_executor_ptr coordinated_compaction_offsets_sender;

    friend std::ostream&
    operator<<(std::ostream& o, const follower_index_metadata& i);
};
class follower_states {
public:
    using container_t = absl::node_hash_map<vnode, follower_index_metadata>;
    using iterator = container_t::iterator;
    using const_iterator = container_t::const_iterator;
    using value_type = container_t::value_type;

    explicit follower_states(vnode self)
      : _self(self) {}

    const follower_index_metadata& get(vnode n) const {
        auto it = _followers.find(n);
        vassert(
          it != _followers.end(),
          "Cannot get non-existent follower index {}, key:{}",
          *this,
          n);
        return it->second;
    }
    follower_index_metadata& get(vnode n) {
        auto it = _followers.find(n);
        vassert(
          it != _followers.end(),
          "Cannot get non-existent follower index {}, key:{}",
          *this,
          n);
        return it->second;
    }

    bool contains(vnode n) const {
        return _followers.find(n) != _followers.end();
    }

    iterator emplace(vnode n, follower_index_metadata m) {
        _followers.erase(n);
        auto [it, success] = _followers.emplace(n, std::move(m));
        vassert(success, "could not insert node:{}", n);
        return it;
    }

    iterator find(vnode n) { return _followers.find(n); }
    const_iterator find(vnode n) const { return _followers.find(n); }

    iterator begin() { return _followers.begin(); }
    iterator end() { return _followers.end(); }
    const_iterator begin() const { return _followers.begin(); }
    const_iterator end() const { return _followers.end(); }

    size_t size() const { return _followers.size(); }

    void return_append_entries_units(vnode);

    void update_with_configuration(const group_configuration&);

    void reset() {
        for (auto& [_, meta] : _followers) {
            meta.reset();
        }
    }

private:
    friend std::ostream& operator<<(std::ostream&, const follower_states&);
    vnode _self;
    container_t _followers;
};

} // namespace raft
