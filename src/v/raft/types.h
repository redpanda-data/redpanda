/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "model/adl_serde.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "raft/fwd.h"
#include "raft/group_configuration.h"
#include "reflection/async_adl.h"
#include "utils/named_type.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/util/bool_class.hh>

#include <boost/range/irange.hpp>
#include <boost/range/join.hpp>

#include <cstdint>
#include <exception>

namespace raft {
using clock_type = ss::lowres_clock;
using duration_type = typename clock_type::duration;
using timer_type = ss::timer<clock_type>;
static constexpr clock_type::time_point no_timeout
  = clock_type::time_point::max();

using group_id = named_type<int64_t, struct raft_group_id_type>;

static constexpr const model::record_batch_type configuration_batch_type{2};
static constexpr const model::record_batch_type data_batch_type{1};

struct protocol_metadata {
    group_id group;
    model::offset commit_index;
    model::term_id term;
    model::offset prev_log_index;
    model::term_id prev_log_term;
    model::offset last_visible_index;
};

// The sequence used to track the order of follower append entries request
using follower_req_seq = named_type<uint64_t, struct follower_req_seq_tag>;

struct follower_index_metadata {
    explicit follower_index_metadata(vnode node)
      : node_id(node) {}

    follower_index_metadata(const follower_index_metadata&) = delete;
    follower_index_metadata& operator=(const follower_index_metadata&) = delete;
    follower_index_metadata(follower_index_metadata&&) = default;
    follower_index_metadata& operator=(follower_index_metadata&&) = default;

    vnode node_id;
    // index of last known log for this follower
    model::offset last_committed_log_index;
    // index of last not flushed offset
    model::offset last_dirty_log_index;
    // index of log for which leader and follower logs matches
    model::offset match_index;
    // Used to establish index persistently replicated by majority
    constexpr model::offset match_committed_index() const {
        return std::min(last_committed_log_index, match_index);
    }
    // next index to send to this follower
    model::offset next_index;
    // timestamp of last append_entries_rpc call
    clock_type::time_point last_append_timestamp;
    clock_type::time_point last_hbeat_timestamp;
    uint64_t failed_appends{0};
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
    bool is_learner = false;
    bool is_recovering = false;

    /*
     * When is_recovering is true a fiber may wait for recovery to be signaled
     * on the recovery_finished condition variable.
     */
    ss::condition_variable recovery_finished;
    /**
     * When recovering, the recovery state machine will wait on this condition
     * variable to wait for any changes that may require triggering recovery.
     *
     * This is:
     * - follower log indices moved backward
     * - disk append happened on the leader
     * - follower is going to be removed
     */
    ss::condition_variable follower_state_change;
    /**
     * We prevent race conditions accessing suppress_heartbeats flag using the
     * `last_sent_seq` value for version control.
     */
    bool suppress_heartbeats = false;
};

struct append_entries_request {
    using flush_after_append = ss::bool_class<struct flush_after_append_tag>;

    // required for the cases where we will set the target node id before
    // sending request to the node
    append_entries_request(
      vnode src,
      protocol_metadata m,
      model::record_batch_reader r,
      flush_after_append f = flush_after_append::yes) noexcept
      : node_id(src)
      , meta(m)
      , batches(std::move(r))
      , flush(f){};

    append_entries_request(
      vnode src,
      vnode target,
      protocol_metadata m,
      model::record_batch_reader r,
      flush_after_append f = flush_after_append::yes) noexcept
      : node_id(src)
      , target_node_id(target)
      , meta(m)
      , batches(std::move(r))
      , flush(f){};
    ~append_entries_request() noexcept = default;
    append_entries_request(const append_entries_request&) = delete;
    append_entries_request& operator=(const append_entries_request&) = delete;
    append_entries_request(append_entries_request&&) noexcept = default;
    append_entries_request&
    operator=(append_entries_request&&) noexcept = default;

    raft::group_id target_group() const { return meta.group; }
    vnode target_node() const { return target_node_id; }
    vnode node_id;
    vnode target_node_id;
    protocol_metadata meta;
    model::record_batch_reader batches;
    flush_after_append flush;
    static append_entries_request make_foreign(append_entries_request&& req) {
        return append_entries_request(
          req.node_id,
          req.target_node_id,
          std::move(req.meta),
          model::make_foreign_record_batch_reader(std::move(req.batches)),
          req.flush);
    }
};

struct append_entries_reply {
    enum class status : uint8_t { success, failure, group_unavailable };
    // node id to validate on receiver
    vnode target_node_id;
    /// \brief callee's node_id; work-around for batched heartbeats
    vnode node_id;
    group_id group;
    /// \brief callee's term, for the caller to upate itself
    model::term_id term;
    /// \brief The recipient's last log index after it applied changes to
    /// the log. This is used to speed up finding the correct value for the
    /// nextIndex with a follower that is far behind a leader
    model::offset last_committed_log_index;
    model::offset last_dirty_log_index;
    // the last entry base offset used for the recovery speed up, the value is
    // only valid for not successfull append_entries reply
    model::offset last_term_base_offset;
    /// \brief did the rpc succeed or not
    status result = status::failure;
};

struct heartbeat_metadata {
    protocol_metadata meta;
    vnode node_id;
    vnode target_node_id;
};

/// \brief this is our _biggest_ modification to how raft works
/// to accomodate for millions of raft groups in a cluster.
/// internally, the receiving side will simply iterate and dispatch one
/// at a time, as well as the receiving side will trigger the
/// individual raft responses one at a time - for example to start replaying the
/// log at some offset
struct heartbeat_request {
    std::vector<heartbeat_metadata> heartbeats;
};
struct heartbeat_reply {
    std::vector<append_entries_reply> meta;
};

struct vote_request {
    vnode node_id;
    // node id to validate on receiver
    vnode target_node_id;

    group_id group;
    /// \brief current term
    model::term_id term;
    /// \brief used to compare completeness
    model::offset prev_log_index;
    model::term_id prev_log_term;
    /// \brief true if vote triggered by leadership transfer
    bool leadership_transfer;
    raft::group_id target_group() const { return group; }
    vnode target_node() const { return target_node_id; }
};

struct vote_reply {
    // node id to validate on receiver
    vnode target_node_id;
    /// \brief callee's term, for the caller to upate itself
    model::term_id term;

    /// True if the follower granted the candidate it's vote, false otherwise
    bool granted = false;

    /// set to true if the caller's log is as up to date as the recipient's
    /// - extension on raft. see Diego's phd dissertation, section 9.6
    /// - "Preventing disruptions when a server rejoins the cluster"
    bool log_ok = false;
};

/// This structure is used by consensus to notify other systems about group
/// leadership changes.
struct leadership_status {
    // Current term
    model::term_id term;
    // Group for which leader have changed
    group_id group;
    // Empty when there is no known leader in the group
    std::optional<vnode> current_leader;
};

struct replicate_result {
    /// used by the kafka API to produce a kafka reply to produce request.
    /// see produce_request.cc
    model::offset last_offset;
};

enum class consistency_level { quorum_ack, leader_ack, no_ack };

struct replicate_options {
    explicit replicate_options(consistency_level l)
      : consistency(l) {}

    consistency_level consistency;
};

struct snapshot_metadata {
    model::offset last_included_index;
    model::term_id last_included_term;
    group_configuration latest_configuration;
    ss::lowres_clock::time_point cluster_time;
};

struct install_snapshot_request {
    // node id to validate on receiver
    vnode target_node_id;
    // leader’s term
    model::term_id term;
    // target group
    raft::group_id group;
    // leader id so follower can redirect clients
    vnode node_id;
    // the snapshot replaces all entries up through and including this index
    model::offset last_included_index;
    // byte offset where chunk is positioned in the snapshot file
    uint64_t file_offset;
    // snapshot chunk, raw bytes
    iobuf chunk;
    // true if this is the last chunk
    bool done;

    raft::group_id target_group() const { return group; }
    vnode target_node() const { return target_node_id; }
    friend std::ostream&
    operator<<(std::ostream&, const install_snapshot_request&);
};

class install_snapshot_request_foreign_wrapper {
public:
    using ptr_t = ss::foreign_ptr<std::unique_ptr<install_snapshot_request>>;

    explicit install_snapshot_request_foreign_wrapper(
      install_snapshot_request&& req)
      : _ptr(ss::make_foreign(
        std::make_unique<install_snapshot_request>(std::move(req)))) {}

    install_snapshot_request copy() const {
        // make copy on target core
        return install_snapshot_request{
          .target_node_id = _ptr->target_node_id,
          .term = _ptr->term,
          .group = _ptr->group,
          .node_id = _ptr->node_id,
          .last_included_index = _ptr->last_included_index,
          .file_offset = _ptr->file_offset,
          .chunk = _ptr->chunk.copy(),
          .done = _ptr->done};
    }
    raft::group_id target_group() const { return _ptr->target_group(); }
    vnode target_node() const { return _ptr->target_node_id; }

private:
    ptr_t _ptr;
};

struct install_snapshot_reply {
    // node id to validate on receiver
    vnode target_node_id;
    // current term, for leader to update itself
    model::term_id term;

    //  The total number of bytes in the snapshot that the follower has
    //  stored (after applying the request).
    //  The leader considers the snapshot transfer complete when bytes_stored
    //  equals the full size of the snapshot. The leader should use bytes_stored
    //  as the value for byte_offset in the next request (most importantly,
    //  when a follower reboots, it returns 0 here and the leader starts at
    //  offset 0 in the next request).
    uint64_t bytes_stored;
    // indicates if the request was successfull
    bool success = false;

    friend std::ostream&
    operator<<(std::ostream&, const install_snapshot_reply&);
};

/**
 * Configuration describing snapshot that is going to be taken at current node.
 */
struct write_snapshot_cfg {
    using should_prefix_truncate = ss::bool_class<struct prefix_truncate_tag>;

    write_snapshot_cfg(model::offset last_included_index, iobuf data)
      : write_snapshot_cfg(
        last_included_index, std::move(data), should_prefix_truncate::yes) {}

    write_snapshot_cfg(
      model::offset last_included_index,
      iobuf data,
      should_prefix_truncate truncate)
      : last_included_index(last_included_index)
      , data(std::move(data))
      , should_truncate(truncate) {}

    // last applied offset
    model::offset last_included_index;
    // snapshot content
    iobuf data;
    // are we going to prefix truncate the log right after persisting the
    // snapshot
    should_prefix_truncate should_truncate;
};

struct timeout_now_request {
    // node id to validate on receiver
    vnode target_node_id;

    vnode node_id;
    group_id group;
    model::term_id term;

    raft::group_id target_group() const { return group; }
    vnode target_node() const { return target_node_id; }
};

struct timeout_now_reply {
    enum class status : uint8_t { success, failure };
    // node id to validate on receiver
    vnode target_node_id;

    model::term_id term;
    status result;
};

// key types used to store data in key-value store
enum class metadata_key : int8_t {
    voted_for = 0,
    config_map = 1,
    config_latest_known_offset = 2,
    last_applied_offset = 3,
    unique_local_id = 4,
    last
};

// priority used to implement semi-deterministic leader election
using voter_priority = named_type<uint32_t, struct voter_priority_tag>;

// zero priority doesn't allow node to become a leader
static constexpr voter_priority zero_voter_priority = voter_priority{0};
// 1 is smallest possible priority allowing node to become a leader
static constexpr voter_priority min_voter_priority = voter_priority{1};

std::ostream& operator<<(std::ostream& o, const vnode& r);
std::ostream& operator<<(std::ostream& o, const consistency_level& l);
std::ostream& operator<<(std::ostream& o, const protocol_metadata& m);
std::ostream& operator<<(std::ostream& o, const vote_reply& r);
std::ostream& operator<<(std::ostream& o, const append_entries_reply::status&);
std::ostream& operator<<(std::ostream& o, const append_entries_reply& r);
std::ostream& operator<<(std::ostream& o, const vote_request& r);
std::ostream& operator<<(std::ostream& o, const follower_index_metadata& i);
std::ostream& operator<<(std::ostream& o, const heartbeat_request& r);
std::ostream& operator<<(std::ostream& o, const heartbeat_reply& r);
} // namespace raft

namespace reflection {
template<>
struct async_adl<raft::append_entries_request> {
    ss::future<> to(iobuf& out, raft::append_entries_request&& request);
    ss::future<raft::append_entries_request> from(iobuf_parser& in);
};
template<>
struct adl<raft::protocol_metadata> {
    void to(iobuf& out, raft::protocol_metadata request);
    raft::protocol_metadata from(iobuf_parser& in);
};
template<>
struct async_adl<raft::heartbeat_request> {
    ss::future<> to(iobuf& out, raft::heartbeat_request&& request);
    ss::future<raft::heartbeat_request> from(iobuf_parser& in);
};

template<>
struct async_adl<raft::heartbeat_reply> {
    ss::future<> to(iobuf& out, raft::heartbeat_reply&& request);
    ss::future<raft::heartbeat_reply> from(iobuf_parser& in);
};

template<>
struct adl<raft::snapshot_metadata> {
    void to(iobuf& out, raft::snapshot_metadata&& request);
    raft::snapshot_metadata from(iobuf_parser& in);
};

} // namespace reflection
