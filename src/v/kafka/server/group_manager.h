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
#include "cluster/cluster_utils.h"
#include "cluster/partition_manager.h"
#include "config/configuration.h"
#include "kafka/protocol/describe_groups.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/heartbeat.h"
#include "kafka/protocol/join_group.h"
#include "kafka/protocol/leave_group.h"
#include "kafka/protocol/list_groups.h"
#include "kafka/protocol/offset_commit.h"
#include "kafka/protocol/offset_fetch.h"
#include "kafka/protocol/sync_group.h"
#include "kafka/server/group.h"
#include "kafka/server/member.h"
#include "model/namespace.h"
#include "raft/group_manager.h"
#include "seastarx.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include <absl/container/flat_hash_map.h>
#include <cluster/partition_manager.h>

namespace kafka {

struct recovery_batch_consumer;

/*
 * \brief Manages the Kafka group lifecycle.
 *
 * Dynamic partition attachment
 * ============================
 *
 * When a partition belonging to the internal group metadata topic comes under
 * management on a node it is dynamically registered with the group manager. The
 * set of partitions is managed in the group manager in an index:
 *
 *     [ntp -> attached_partition]
 *
 * Where `attached_partition` is a struct with an initial state of:
 *
 *     { loading = true, semaphore(1), abort_source, partition }
 *
 * Leadership changes
 * ==================
 *
 * The group manager handles leadership changes by transitioning the state of
 * attached partitions, either by recovering state or clearing state from cache.
 *
 * On leadership change notification
 *
 *     1. Do nothing if notification is for a non-registered partition
 *     2. While holding the attached partition's semaphore
 *     3. Recover or unload (see below)
 *
 * Leadership change notifications current occur for _all_ partitions on the
 * current {node, core} pair (not just group metadata partitions). Hence, we do
 * nothing for non-registered partitions. See dynamic partition attachment
 * discussion above regarding registration.
 *
 * In order to support (1) parallel partition recovery (important during
 * start-up) and (2) flapping leadership we grab a per-partition semaphore to
 * queue state transitions on the registered partition.
 *
 * The semaphore is used to serialize recovery/unload requests which are
 * themselves an asynchronous fiber. The controller/raft dispatches these
 * requests as leadership changes, and the requests are sync upcalls that can't
 * be handled synchronously without blocking raft.
 *
 * When a new upcall is received, we use the abort source to request that any
 * on-going recovery/unload is stopped promptly.
 *
 * Recovery (background)
 * =====================
 *
 * - Both recovery and partition unload are serialized per-partition
 * - Recovery occurs when the local node is leader, else unload (below)
 *
 * The recovery process reads the entire log and deduplicates entries into the
 * `recovery_batch_consumer` object.
 *
 * After the log is read the deduplicated state is used to re-populate the
 * in-memory cache of groups/commits through.
 *
 * Unload (background)
 * ===================
 *
 * - Both recovery and partition unload are serialized per-partition
 * - Unloading occurs when the local node loses partition leadership
 *
 * This process involves identifying the groups/commits that map to a partition
 * for which the local node is no longer a leader. The in-memory cache will be
 * cleared.
 *
 *     - This is not yet implemented.
 */
class group_manager {
public:
    group_manager(
      ss::sharded<raft::group_manager>& gm,
      ss::sharded<cluster::partition_manager>& pm,
      config::configuration& conf)
      : _gm(gm)
      , _pm(pm)
      , _conf(conf)
      , _self(cluster::make_self_broker(config::shard_local_cfg())) {}

    ss::future<> start();
    ss::future<> stop();

public:
    /// \brief Handle a JoinGroup request
    ss::future<join_group_response> join_group(join_group_request&& request);

    /// \brief Handle a SyncGroup request
    ss::future<sync_group_response> sync_group(sync_group_request&& request);

    /// \brief Handle a Heartbeat request
    ss::future<heartbeat_response> heartbeat(heartbeat_request&& request);

    /// \brief Handle a LeaveGroup request
    ss::future<leave_group_response> leave_group(leave_group_request&& request);

    /// \brief Handle a OffsetCommit request
    ss::future<offset_commit_response>
    offset_commit(offset_commit_request&& request);

    /// \brief Handle a OffsetFetch request
    ss::future<offset_fetch_response>
    offset_fetch(offset_fetch_request&& request);

    // returns the set of registered groups, and a flag indicating if any
    // partition is actively recovering, which can be used to signal to the
    // caller that the returned set of groups may be incomplete.
    std::pair<bool, std::vector<listed_group>> list_groups() const;

    described_group describe_group(const model::ntp&, const kafka::group_id&);

public:
    error_code validate_group_status(
      const model::ntp& ntp, const group_id& group, api_key api);

    static bool valid_group_id(const group_id& group, api_key api);

private:
    group_ptr get_group(const group_id& group) {
        if (auto it = _groups.find(group); it != _groups.end()) {
            return it->second;
        }
        return nullptr;
    }

    cluster::notification_id_type _manage_notify_handle;
    ss::gate _gate;

    void attach_partition(ss::lw_shared_ptr<cluster::partition>);

    struct attached_partition {
        bool loading;
        ss::semaphore sem{1};
        ss::abort_source as;
        ss::lw_shared_ptr<cluster::partition> partition;

        attached_partition(ss::lw_shared_ptr<cluster::partition> p)
          : loading(true)
          , partition(std::move(p)) {}
    };

    absl::flat_hash_map<model::ntp, ss::lw_shared_ptr<attached_partition>>
      _partitions;

    cluster::notification_id_type _leader_notify_handle;

    void handle_leader_change(
      ss::lw_shared_ptr<cluster::partition>, std::optional<model::node_id>);

    ss::future<> handle_partition_leader_change(
      ss::lw_shared_ptr<attached_partition>,
      std::optional<model::node_id> leader_id);

    ss::future<> recover_partition(
      ss ::lw_shared_ptr<cluster::partition>, recovery_batch_consumer);

    ss::future<> inject_noop(
      ss::lw_shared_ptr<cluster::partition> p,
      ss::lowres_clock::time_point timeout);

    ss::sharded<raft::group_manager>& _gm;
    ss::sharded<cluster::partition_manager>& _pm;
    config::configuration& _conf;
    absl::flat_hash_map<group_id, group_ptr> _groups;
    model::broker _self;
};

/**
 * the key type for group membership log records.
 *
 * the opaque key field is decoded based on the actual type.
 *
 * TODO: The `noop` type indicates a control structure used to synchronize raft
 * state in a transition to leader state so that a consistent read is made. this
 * is a temporary work-around until we fully address consistency semantics in
 * raft.
 */
struct group_log_record_key {
    enum class type : int8_t { group_metadata, offset_commit, noop };

    type record_type;
    iobuf key;
};

/**
 * the value type of a group metadata log record.
 */
struct group_log_group_metadata {
    kafka::protocol_type protocol_type;
    kafka::generation_id generation;
    std::optional<kafka::protocol_name> protocol;
    std::optional<kafka::member_id> leader;
    int32_t state_timestamp;
    std::vector<member_state> members;
};

/**
 * the key type for offset commit records.
 */
struct group_log_offset_key {
    kafka::group_id group;
    model::topic topic;
    model::partition_id partition;

    bool operator==(const group_log_offset_key& other) const {
        return group == other.group && topic == other.topic
               && partition == other.partition;
    }

    friend std::ostream& operator<<(std::ostream&, const group_log_offset_key&);
};

/**
 * the value type for offset commit records.
 */
struct group_log_offset_metadata {
    model::offset offset;
    int32_t leader_epoch;
    std::optional<ss::sstring> metadata;

    friend std::ostream&
    operator<<(std::ostream&, const group_log_offset_metadata&);
};

} // namespace kafka

namespace std {
template<>
struct hash<kafka::group_log_offset_key> {
    size_t operator()(const kafka::group_log_offset_key& key) const {
        size_t h = 0;
        boost::hash_combine(h, hash<ss::sstring>()(key.group));
        boost::hash_combine(h, hash<ss::sstring>()(key.topic));
        boost::hash_combine(h, hash<model::partition_id>()(key.partition));
        return h;
    }
};
} // namespace std

namespace kafka {

/*
 * This batch consumer is used during partition recovery to read, index, and
 * deduplicate both group and commit metadata snapshots.
 */
struct recovery_batch_consumer {
    recovery_batch_consumer(ss::abort_source* as)
      : as(as) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch batch);

    ss::future<> handle_record(model::record);
    ss::future<> handle_group_metadata(iobuf key_buf, iobuf val_buf);
    ss::future<> handle_offset_metadata(iobuf key_buf, iobuf val_buf);

    recovery_batch_consumer end_of_stream() { return std::move(*this); }

    model::offset batch_base_offset;

    absl::flat_hash_map<kafka::group_id, group_log_group_metadata>
      loaded_groups;

    absl::flat_hash_set<kafka::group_id> removed_groups;

    absl::flat_hash_map<
      group_log_offset_key,
      std::pair<model::offset, group_log_offset_metadata>>
      loaded_offsets;

    // this is invalid after end_of_stream() is invoked
    ss::abort_source* as;
};

} // namespace kafka
