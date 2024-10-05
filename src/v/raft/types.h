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

#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "raft/fundamental.h"
#include "raft/group_configuration.h"
#include "raft/replicate.h"
#include "reflection/adl.h"
#include "serde/rw/bool_class.h"
#include "serde/rw/envelope.h"
#include "serde/rw/scalar.h"
#include "utils/named_type.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/util/bool_class.hh>

#include <boost/range/irange.hpp>
#include <boost/range/join.hpp>

#include <cstdint>

namespace raft {

using election_success = ss::bool_class<struct election_success_tag>;

struct protocol_metadata
  : serde::
      envelope<protocol_metadata, serde::version<1>, serde::compat_version<0>> {
    group_id group;
    model::offset commit_index;
    model::term_id term;
    model::offset prev_log_index;
    model::term_id prev_log_term;
    model::offset last_visible_index;
    model::offset dirty_offset;

    friend std::ostream&
    operator<<(std::ostream& o, const protocol_metadata& m);

    friend bool operator==(const protocol_metadata&, const protocol_metadata&)
      = default;

    auto serde_fields() {
        return std::tie(
          group,
          commit_index,
          term,
          prev_log_index,
          prev_log_term,
          last_visible_index,
          dirty_offset);
    }
};

// The sequence used to track the order of follower append entries request
using follower_req_seq = named_type<uint64_t, struct follower_req_seq_tag>;
struct follower_index_metadata {
    explicit follower_index_metadata(vnode node)
      : node_id(node) {}

    follower_index_metadata(const follower_index_metadata&) = delete;
    follower_index_metadata& operator=(const follower_index_metadata&) = delete;
    follower_index_metadata(follower_index_metadata&&) = default;
    follower_index_metadata& operator=(follower_index_metadata&&) = delete;
    // resets the follower state i.e. all indicies and sequence numbers
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
    // sequence number of last received successfull append entries request
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

    friend std::ostream&
    operator<<(std::ostream& o, const follower_index_metadata& i);
};

/**
 * class containing follower statistics, this may be helpful for debugging,
 * metrics and querying for follower status
 */
struct follower_metrics {
    model::node_id id;
    bool is_learner;
    model::offset committed_log_index;
    model::offset dirty_log_index;
    model::offset match_index;
    clock_type::time_point last_heartbeat;
    bool is_live;
    bool under_replicated;
};
using flush_after_append = ss::bool_class<struct flush_after_append_tag>;

using flush_after_append = ss::bool_class<struct flush_after_append_tag>;

struct append_entries_request
  : serde::envelope<
      append_entries_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    // required for the cases where we will set the target node id before
    // sending request to the node
    append_entries_request(
      vnode src,
      protocol_metadata m,
      model::record_batch_reader r,
      flush_after_append f = flush_after_append::yes) noexcept
      : _source_node(src)
      , _meta(m)
      , _flush(f)
      , _batches(std::move(r)) {}

    append_entries_request(
      vnode src,
      vnode target,
      protocol_metadata m,
      model::record_batch_reader r,
      flush_after_append f = flush_after_append::yes) noexcept
      : _source_node(src)
      , _target_node_id(target)
      , _meta(m)
      , _flush(f)
      , _batches(std::move(r)) {}

    ~append_entries_request() noexcept = default;
    append_entries_request(const append_entries_request&) = delete;
    append_entries_request& operator=(const append_entries_request&) = delete;
    append_entries_request(append_entries_request&&) noexcept = default;
    append_entries_request& operator=(append_entries_request&&) noexcept
      = default;

    raft::group_id target_group() const { return _meta.group; }
    vnode source_node() const { return _source_node; }
    vnode target_node() const { return _target_node_id; }
    void set_target_node(vnode target) { _target_node_id = target; }

    const protocol_metadata& metadata() const { return _meta; }
    flush_after_append is_flush_required() const { return _flush; }

    model::record_batch_reader release_batches() && {
        return std::move(_batches);
    }

    model::record_batch_reader& batches() { return _batches; }
    const model::record_batch_reader& batches() const { return _batches; }

    static append_entries_request make_foreign(append_entries_request&& req);

    friend std::ostream&
    operator<<(std::ostream& o, const append_entries_request& r);

    ss::future<> serde_async_write(iobuf& out);

    static ss::future<append_entries_request>
    serde_async_direct_read(iobuf_parser&, serde::header);

private:
    vnode _source_node;
    vnode _target_node_id;
    protocol_metadata _meta;
    flush_after_append _flush;
    model::record_batch_reader _batches;
};

class append_entries_request_serde_wrapper
  : public serde::envelope<
      append_entries_request_serde_wrapper,
      serde::version<0>,
      serde::compat_version<0>> {
public:
    using rpc_adl_exempt = std::true_type;
    explicit append_entries_request_serde_wrapper(append_entries_request req)
      : _request(std::move(req)) {}

    append_entries_request release() && { return std::move(_request); }

    ss::future<> serde_async_write(iobuf& out);

    static ss::future<append_entries_request_serde_wrapper>
    serde_async_direct_read(iobuf_parser&, serde::header);

private:
    append_entries_request _request;
};

/*
 * append_entries_reply uses two different types of serialization: when
 * encoding/decoding directly normal adl/serde per-field serialization is used.
 * the second type is a custom encoding used by heartbeat_reply for more
 * efficient encoding of a vectory of append_entries_reply.
 */
struct append_entries_reply
  : serde::envelope<
      append_entries_reply,
      serde::version<1>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

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
    model::offset last_flushed_log_index;
    model::offset last_dirty_log_index;
    // the last entry base offset used for the recovery speed up, the value is
    // only valid for not successfull append_entries reply
    model::offset last_term_base_offset;
    /// \brief did the rpc succeed or not
    reply_result result = reply_result::failure;

    // Hint from follower to leader, indicating they are ready for
    // recovery traffic (false if they may not have enough resources to
    // handle recovery).
    // This is true by default to reflect the legacy case, where
    // older nodes are always ready for recovery.
    bool may_recover = true;

    friend std::ostream&
    operator<<(std::ostream& o, const append_entries_reply& r);

    friend bool
    operator==(const append_entries_reply&, const append_entries_reply&)
      = default;

    auto serde_fields() {
        return std::tie(
          target_node_id,
          node_id,
          group,
          term,
          last_flushed_log_index,
          last_dirty_log_index,
          last_term_base_offset,
          result,
          may_recover);
    }
};

struct heartbeat_metadata {
    protocol_metadata meta;
    vnode node_id;
    vnode target_node_id;

    friend bool operator==(const heartbeat_metadata&, const heartbeat_metadata&)
      = default;
    friend std::ostream&
    operator<<(std::ostream& o, const heartbeat_metadata& r);
};

/// \brief this is our _biggest_ modification to how raft works
/// to accomodate for millions of raft groups in a cluster.
/// internally, the receiving side will simply iterate and dispatch one
/// at a time, as well as the receiving side will trigger the
/// individual raft responses one at a time - for example to start replaying the
/// log at some offset
struct heartbeat_request
  : serde::
      envelope<heartbeat_request, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    std::vector<heartbeat_metadata> heartbeats;

    heartbeat_request() noexcept = default;
    explicit heartbeat_request(std::vector<heartbeat_metadata> heartbeats)
      : heartbeats(std::move(heartbeats)) {}

    friend std::ostream&
    operator<<(std::ostream& o, const heartbeat_request& r);

    friend bool operator==(const heartbeat_request&, const heartbeat_request&)
      = default;

    ss::future<> serde_async_write(iobuf& out);
    void serde_read(iobuf_parser&, const serde::header&);
};

struct heartbeat_reply
  : serde::
      envelope<heartbeat_reply, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    std::vector<append_entries_reply> meta;

    heartbeat_reply() noexcept = default;
    explicit heartbeat_reply(std::vector<append_entries_reply> meta)
      : meta(std::move(meta)) {}

    friend std::ostream& operator<<(std::ostream& o, const heartbeat_reply& r);

    friend bool operator==(const heartbeat_reply&, const heartbeat_reply&)
      = default;

    void serde_write(iobuf& out);
    void serde_read(iobuf_parser&, const serde::header&);
};

struct vote_request
  : serde::envelope<vote_request, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
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
    vnode source_node() const { return node_id; }
    vnode target_node() const { return target_node_id; }

    friend std::ostream& operator<<(std::ostream& o, const vote_request& r);

    friend bool operator==(const vote_request&, const vote_request&) = default;

    auto serde_fields() {
        return std::tie(
          node_id,
          target_node_id,
          group,
          term,
          prev_log_index,
          prev_log_term,
          leadership_transfer);
    }
};

struct vote_reply
  : serde::envelope<vote_reply, serde::version<1>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
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

    // replying node
    vnode node_id;

    friend std::ostream& operator<<(std::ostream& o, const vote_reply& r);

    friend bool operator==(const vote_reply&, const vote_reply&) = default;

    auto serde_fields() {
        return std::tie(target_node_id, term, granted, log_ok, node_id);
    }
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

struct transfer_leadership_options {
    std::chrono::milliseconds recovery_timeout;
};

using offset_translator_delta = named_type<int64_t, struct ot_delta_tag>;
struct snapshot_metadata {
    // we start snasphot metadata version at 64 to leave room for configuration
    // version updates
    static constexpr int8_t initial_version = 64;
    static constexpr int8_t current_version = initial_version;

    model::offset last_included_index;
    model::term_id last_included_term;
    int8_t version = current_version;
    group_configuration latest_configuration;
    ss::lowres_clock::time_point cluster_time;
    offset_translator_delta log_start_delta;
    /**
     * Since snapshot metadata did not include a version field we are going to
     * use group configuration version field as a indicator of snapshot metadata
     * version.
     */
    static_assert(
      group_configuration::current_version <= initial_version,
      "snapshot metadata is based on the assumption that group configuration "
      "version is equal to 3, please change it accordignly");
};

struct install_snapshot_request
  : serde::envelope<
      install_snapshot_request,
      serde::version<1>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
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
    // leader dirty offset
    model::offset dirty_offset;

    raft::group_id target_group() const { return group; }
    vnode source_node() const { return node_id; }
    vnode target_node() const { return target_node_id; }
    friend std::ostream&
    operator<<(std::ostream&, const install_snapshot_request&);

    friend bool
    operator==(const install_snapshot_request&, const install_snapshot_request&)
      = default;

    auto serde_fields() {
        return std::tie(
          target_node_id,
          term,
          group,
          node_id,
          last_included_index,
          file_offset,
          chunk,
          done,
          dirty_offset);
    }
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
          .done = _ptr->done,
          .dirty_offset = _ptr->dirty_offset};
    }
    raft::group_id target_group() const { return _ptr->target_group(); }
    vnode target_node() const { return _ptr->target_node_id; }

private:
    ptr_t _ptr;
};

struct install_snapshot_reply
  : serde::envelope<
      install_snapshot_reply,
      serde::version<1>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
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
    uint64_t bytes_stored = 0;
    // indicates if the request was successfull
    bool success = false;

    // replying node
    vnode node_id;

    friend std::ostream&
    operator<<(std::ostream&, const install_snapshot_reply&);

    friend bool
    operator==(const install_snapshot_reply&, const install_snapshot_reply&)
      = default;

    auto serde_fields() {
        return std::tie(target_node_id, term, bytes_stored, success, node_id);
    }
};

/**
 * Configuration describing snapshot that is going to be taken at current node.
 */
struct write_snapshot_cfg {
    write_snapshot_cfg(model::offset last_included_index, iobuf data)
      : last_included_index(last_included_index)
      , data(std::move(data)) {}

    // last applied offset
    model::offset last_included_index;
    // snapshot content
    iobuf data;
};

struct timeout_now_request
  : serde::envelope<
      timeout_now_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    // node id to validate on receiver
    vnode target_node_id;

    vnode node_id;
    group_id group;
    model::term_id term;

    raft::group_id target_group() const { return group; }
    vnode source_node() const { return node_id; }
    vnode target_node() const { return target_node_id; }

    friend bool
    operator==(const timeout_now_request&, const timeout_now_request&)
      = default;

    auto serde_fields() {
        return std::tie(target_node_id, node_id, group, term);
    }

    friend std::ostream&
    operator<<(std::ostream& o, const timeout_now_request& r) {
        fmt::print(
          o,
          "target_node_id {} node_id {} group {} term {}",
          r.target_node_id,
          r.node_id,
          r.group,
          r.term);
        return o;
    }
};

struct timeout_now_reply
  : serde::
      envelope<timeout_now_reply, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    enum class status : uint8_t { success, failure };
    // node id to validate on receiver
    vnode target_node_id;

    model::term_id term;
    status result;

    friend bool operator==(const timeout_now_reply&, const timeout_now_reply&)
      = default;

    auto serde_fields() { return std::tie(target_node_id, term, result); }

    friend std::ostream&
    operator<<(std::ostream& o, const timeout_now_reply& r) {
        fmt::print(
          o,
          "target_node_id {} term {} result {}",
          r.target_node_id,
          r.term,
          static_cast<std::underlying_type_t<status>>(r.result));
        return o;
    }
};

// key types used to store data in key-value store
enum class metadata_key : int8_t {
    voted_for = 0,
    config_map = 1,
    config_latest_known_offset = 2,
    last_applied_offset = 3,
    unique_local_id = 4,
    config_next_cfg_idx = 5,
    last
};

// priority used to implement semi-deterministic leader election
using voter_priority = named_type<uint32_t, struct voter_priority_tag>;

// zero priority doesn't allow node to become a leader
inline constexpr voter_priority zero_voter_priority = voter_priority{0};
// 1 is smallest possible priority allowing node to become a leader
inline constexpr voter_priority min_voter_priority = voter_priority{1};

/**
 * Raft scheduling_config contains Seastar scheduling and IO priority
 * controlling primitives.
 */
struct scheduling_config {
    scheduling_config(
      ss::scheduling_group default_sg,
      ss::io_priority_class default_iopc,
      ss::scheduling_group learner_recovery_sg,
      ss::io_priority_class learner_recovery_iopc)
      : default_sg(default_sg)
      , default_iopc(default_iopc)
      , learner_recovery_sg(learner_recovery_sg)
      , learner_recovery_iopc(learner_recovery_iopc) {}

    scheduling_config(
      ss::scheduling_group default_sg, ss::io_priority_class default_iopc)
      : scheduling_config(default_sg, default_iopc, default_sg, default_iopc) {}

    ss::scheduling_group default_sg;
    ss::io_priority_class default_iopc;
    ss::scheduling_group learner_recovery_sg;
    ss::io_priority_class learner_recovery_iopc;
};

std::ostream& operator<<(std::ostream& o, const consistency_level& l);
std::ostream& operator<<(std::ostream& o, const reply_result&);

using with_learner_recovery_throttle
  = ss::bool_class<struct with_recovery_throttle_tag>;

using keep_snapshotted_log = ss::bool_class<struct keep_snapshotted_log_tag>;

// Raft part of the struct that makes starting the partition
// instance on the destination shard of the x-shard transfer easier.
struct xshard_transfer_state {
    // If before the transfer, this partition was the leader, will contain the
    // corresponding term. It will be used to try to immediately regain the
    // leadership on the destination shard.
    std::optional<model::term_id> leader_term;
};

} // namespace raft

namespace reflection {

template<>
struct adl<raft::snapshot_metadata> {
    void to(iobuf& out, raft::snapshot_metadata&& request);
    raft::snapshot_metadata from(iobuf_parser& in);
};

} // namespace reflection
