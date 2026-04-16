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
#include "absl/container/flat_hash_set.h"
#include "absl/container/node_hash_map.h"
#include "base/seastarx.h"
#include "cluster/notification.h"
#include "cluster/offsets_snapshot.h"
#include "cluster/topic_table.h"
#include "container/chunked_vector.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/heartbeat.h"
#include "kafka/protocol/join_group.h"
#include "kafka/protocol/leave_group.h"
#include "kafka/protocol/offset_commit.h"
#include "kafka/protocol/offset_delete.h"
#include "kafka/protocol/offset_fetch.h"
#include "kafka/protocol/schemata/delete_groups_response.h"
#include "kafka/protocol/schemata/describe_producers_response.h"
#include "kafka/protocol/schemata/list_groups_response.h"
#include "kafka/protocol/sync_group.h"
#include "kafka/protocol/txn_offset_commit.h"
#include "kafka/protocol/types.h"
#include "kafka/server/fwd.h"
#include "kafka/server/group.h"
#include "kafka/server/group_recovery_consumer.h"
#include "kafka/server/group_stm.h"
#include "model/metadata.h"
#include "raft/fwd.h"
#include "raft/notification.h"
#include "ssx/mutex.h"
#include "ssx/semaphore.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/sharded.hh>

#include <system_error>

namespace kafka {

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
class group_manager : public ss::peering_sharded_service<group_manager> {
public:
    group_manager(
      model::topic_namespace,
      ss::sharded<raft::group_manager>& gm,
      ss::sharded<cluster::partition_manager>& pm,
      ss::sharded<cluster::topic_table>&,
      ss::sharded<cluster::tx_gateway_frontend>& tx_frontend,
      ss::sharded<features::feature_table>&,
      ss::sharded<cluster::health_monitor_frontend>& hm_frontend);

    ss::future<> start();
    ss::future<> stop();

    struct list_groups_filter_data {
        using states_filter_t = absl::flat_hash_set<group_state>;

        states_filter_t states_filter;
    };

public:
    /// \brief Handle a JoinGroup request
    group::join_group_stages join_group(join_group_request&& request);

    /// \brief Handle a SyncGroup request
    group::sync_group_stages sync_group(sync_group_request&& request);

    /// \brief Handle a Heartbeat request
    ss::future<heartbeat_response> heartbeat(heartbeat_request&& request);

    /// \brief Handle a LeaveGroup request
    ss::future<leave_group_response> leave_group(leave_group_request&& request);

    /// \brief Handle a OffsetCommit request
    group::offset_commit_stages offset_commit(offset_commit_request&& request);

    ss::future<txn_offset_commit_response>
    txn_offset_commit(txn_offset_commit_request&& request);

    ss::future<cluster::commit_group_tx_reply>
    commit_tx(cluster::commit_group_tx_request&& request);

    ss::future<cluster::begin_group_tx_reply>
    begin_tx(cluster::begin_group_tx_request&&);

    ss::future<cluster::abort_group_tx_reply>
    abort_tx(cluster::abort_group_tx_request&&);

    /// \brief Handle a OffsetFetch request
    ss::future<offset_fetch_response>
    offset_fetch(offset_fetch_request request);

    ss::future<offset_delete_response>
    offset_delete(offset_delete_request&& request);

    // returns the set of registered groups, and an error if one occurred while
    // retrieving the group list (e.g. coordinator_load_in_progress).
    std::pair<error_code, chunked_vector<listed_group>>
    list_groups(const list_groups_filter_data& filter_data = {}) const;

    described_group describe_group(const model::ntp&, const kafka::group_id&);

    using partition_producers = partition_response;
    partition_response describe_partition_producers(const model::ntp&);

    ss::future<std::error_code> empty_and_delete_groups(
      const model::ntp&,
      const chunked_vector<group_id>&,
      model::revision_id revision_id);

    ss::future<chunked_vector<deletable_group_result>> delete_groups(
      chunked_vector<std::pair<model::ntp, group_id>>,
      bool allow_blocked = false);

    ss::future<> reload_groups();

    ss::future<result<model::offset>> set_blocked_for_groups(
      const model::ntp& co_ntp,
      const chunked_vector<kafka::group_id>&,
      group_block_info req);

    using group_offsets_snapshot_result = result<
      std::vector<cluster::group_offsets_snapshot>,
      cluster::cloud_metadata::error_outcome>;
    // Returns the groups being managed by the attached partition of the given
    // NTP, returning an error if the partition is not serving groups on this
    // shard (e.g. not leader, still loading groups, etc).
    ss::future<group_offsets_snapshot_result> snapshot_groups_for_upload(
      const model::ntp&, size_t max_num_groups_per_snap = 1000);

    ss::future<cluster::get_group_offsets_reply>
      get_group_offsets(cluster::get_group_offsets_request);

    ss::future<kafka::error_code>
      recover_offsets(cluster::group_offsets_snapshot);

    ss::future<cluster::set_group_offsets_reply>
      set_group_offsets(cluster::set_group_offsets_request);

    size_t attached_partitions_count() const { return _partitions.size(); }

    ss::future<cluster::get_producers_reply>
      get_group_producers_locally(cluster::get_producers_request);

public:
    error_code validate_group_status(
      const model::ntp& ntp,
      const group_id& group,
      api_key api,
      bool allow_blocked) const;

    static bool valid_group_id(const group_id& group, api_key api);

private:
    model::topic_namespace _tp_ns;

    group_ptr get_group(const group_id& group) {
        if (auto it = _groups.find(group); it != _groups.end()) {
            return it->second;
        }
        return nullptr;
    }

    cluster::notification_id_type _manage_notify_handle;
    cluster::notification_id_type _unmanage_notify_handle;
    ss::gate _gate;

    void attach_partition(ss::lw_shared_ptr<cluster::partition>);
    void detach_partition(const model::ntp&);
    ss::future<> do_detach_partition(model::ntp);

    struct attached_partition {
        bool loading;
        ssx::semaphore sem{1, "k/group-mgr"};
        ss::abort_source as;
        ss::lw_shared_ptr<cluster::partition> partition;
        ss::lw_shared_ptr<ss::rwlock> catchup_lock;
        model::term_id term{-1};
        group_block_info_map group_blocks;
        ssx::mutex block_lock{"k/group-mgr::block-lock"};

        explicit attached_partition(ss::lw_shared_ptr<cluster::partition> p);
        ~attached_partition() noexcept;
    };

    raft::group_manager_notification_id _leader_notify_handle;
    cluster::notification_id_type _topic_table_notify_handle;

    void handle_leader_change(
      model::term_id,
      ss::lw_shared_ptr<cluster::partition>,
      std::optional<model::node_id>);

    void handle_topic_delta(cluster::topic_table::ntp_delta_range_t);

    ss::future<> cleanup_removed_topic_partitions(
      const chunked_vector<model::topic_partition>&);

    ss::future<> handle_partition_leader_change(
      model::term_id,
      ss::lw_shared_ptr<attached_partition>,
      std::optional<model::node_id> leader_id);

    ss::future<> write_version_fence(
      model::term_id, ss::lw_shared_ptr<attached_partition>);

    ss::future<> recover_partition(
      model::term_id,
      ss::lw_shared_ptr<attached_partition>,
      group_recovery_consumer_state);

    ss::future<size_t> delete_offsets(
      group_ptr group, const chunked_vector<model::topic_partition>& offsets);

    ss::future<> do_recover_group(
      model::term_id,
      ss::lw_shared_ptr<attached_partition>,
      group_id,
      group_stm);

    ss::future<> gc_partition_state(ss::lw_shared_ptr<attached_partition>);

    ss::future<std::error_code> inject_noop(
      ss::lw_shared_ptr<cluster::partition> p,
      ss::lowres_clock::time_point timeout);

    // If `group_filter` is provided the function will only succeed if all of
    // them have been found in the partition.
    ss::future<
      result<std::vector<cluster::group_offsets_snapshot>, cluster::errc>>
    do_snapshot_groups(
      const model::ntp&,
      size_t max_num_groups_per_snap,
      std::optional<chunked_vector<group_id>> group_filter);

    // if `merge` is true existing group offsets is saved with usual offset
    // commit semantics, otherwise snapshot data for existing groups is ignored
    ss::future<kafka::error_code>
    do_bulk_write_offsets(cluster::group_offsets_snapshot, bool merge = false);

    ss::lw_shared_ptr<attached_partition>
    get_attached_partition(const model::ntp& ntp) {
        auto it = _partitions.find(ntp);
        if (it == _partitions.end()) {
            return nullptr;
        }
        return it->second;
    }

    ss::future<> shutdown_groups(std::vector<group_ptr> groups) {
        return ss::max_concurrent_for_each(
          groups, 64, [](auto group_ptr) { return group_ptr->shutdown(); });
    }

    ss::future<> collect_consumer_lag_metrics();

    std::optional<std::chrono::seconds> offset_retention_enabled();
    std::optional<bool> _prev_offset_retention_enabled;

    ss::timer<> _expired_group_offset_timer;
    ss::timer<> _lag_metrics_timer;
    ss::future<> handle_offset_expiration();
    ss::future<size_t> delete_expired_offsets(group_ptr, std::chrono::seconds);
    ss::sharded<raft::group_manager>& _gm;
    ss::sharded<cluster::partition_manager>& _pm;
    ss::sharded<cluster::topic_table>& _topic_table;
    ss::sharded<cluster::tx_gateway_frontend>& _tx_frontend;
    ss::sharded<features::feature_table>& _feature_table;
    ss::sharded<cluster::health_monitor_frontend>& _hm_frontend;
    config::configuration& _conf;
    absl::node_hash_map<group_id, group_ptr> _groups;
    absl::node_hash_map<model::ntp, ss::lw_shared_ptr<attached_partition>>
      _partitions;

    model::broker _self;
    config::binding<std::chrono::milliseconds> _offset_retention_check;
    config::binding<std::vector<ss::sstring>> _enabled_metrics;
    config::binding<std::chrono::seconds> _lag_collection_interval;
};

} // namespace kafka
