
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
#include "cluster/fwd.h"
#include "cluster/health_monitor_types.h"
#include "cluster/node/local_monitor.h"
#include "cluster/node_status_table.h"
#include "cluster/notification.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "rpc/fwd.h"
#include "ssx/mutex.h"
#include "ssx/semaphore.h"
#include "storage/disk.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/circular_buffer_fixed_capacity.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>

#include <chrono>
#include <memory>
#include <vector>
namespace cluster {

using health_node_cb_t = ss::noncopyable_function<void(
  const node_health_report&,
  std::optional<ss::lw_shared_ptr<const node_health_report>>)>;

namespace health_monitor_backend_details {
template<class T>
concept partition_leader_status_handler = std::is_invocable_r_v<
  void,
  T,
  const followers_stats&,
  const model::topic_namespace&,
  model::partition_id>;

template<class T>
concept partition_handler = std::
  is_invocable_r_v<void, T, const model::topic_namespace&, model::partition_id>;
} // namespace health_monitor_backend_details
/**
 * Health monitor backend is responsible for collecting cluster health status
 * and caching cluster health information.
 *
 * Health monitor status collection is active only on the node which is a
 * controller partition leader. When any other node is requesting a cluster
 * report it either uses locally cached state or asks controller leader for
 * new report.
 */
class health_monitor_backend {
public:
    static constexpr ss::shard_id shard{0};

    health_monitor_backend(
      ss::lw_shared_ptr<raft::consensus>,
      ss::sharded<members_table>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<partition_manager>&,
      ss::sharded<raft::group_manager>&,
      ss::sharded<ss::abort_source>&,
      ss::sharded<node::local_monitor>&,
      ss::sharded<drain_manager>&,
      ss::sharded<features::feature_table>&,
      ss::sharded<partition_leaders_table>&,
      ss::sharded<topic_table>&,
      ss::sharded<node_status_table>&,
      model::node_boot_id self_boot_id);

    ss::future<> stop();

    ss::future<result<cluster_health_report>> get_cluster_health(
      cluster_report_filter, force_refresh, model::timeout_clock::time_point);

    ss::future<storage::disk_space_alert> get_cluster_data_disk_health(
      force_refresh refresh, model::timeout_clock::time_point deadline);

    ss::future<result<node_health_report>> collect_current_node_health_legacy();
    ss::future<result<health::node_health>> collect_current_node_health();
    /**
     * Return cached version of current node health of collects it if it is not
     * available in cache.
     */
    ss::future<result<node_health_report_ptr>> get_current_node_health();

    cluster::notification_id_type register_node_callback(health_node_cb_t cb);
    void unregister_node_callback(cluster::notification_id_type id);

    ss::future<result<std::optional<cluster::drain_status>>>
      get_node_drain_status(model::node_id, model::timeout_clock::time_point);

    ss::future<cluster_health_overview>
      get_cluster_health_overview(model::timeout_clock::time_point);

    ss::future<result<restart_risk_report>> get_current_node_restart_risks(
      size_t limit, model::timeout_clock::time_point deadline);

    ss::future<result<double>> get_current_node_in_sync_replicas_share(
      model::timeout_clock::time_point deadline);

    bool does_raft0_have_leader();

    bool contains_node_health_report(model::node_id) const;
    /**
     * Returns maximum high watermark for a given partition across the cluster.
     * It returns the high watermark for the partition replica with highest
     * revision.
     *
     * NOTE: why not returning the high watermark from the leader replica ?
     *
     * The leader replica high watermark may be stale if leader health report is
     * older than followers one. High watermark is monotonically increasing
     * therefore it is always safe to return the highest value.
     */
    ss::future<result<std::optional<kafka::offset>>>
      get_partition_high_watermark(
        model::topic_namespace_view, model::partition_id);

    ~health_monitor_backend(); // defined in .cc (incomplete PIMPL type)

private:
    /**
     * Struct used to track pending refresh request, it gives ability
     */
    struct abortable_refresh_request
      : ss::enable_lw_shared_from_this<abortable_refresh_request> {
        abortable_refresh_request(ss::gate::holder, ssx::semaphore_units);

        ss::future<std::error_code>
          abortable_await(ss::future<std::error_code>);
        void abort();

        bool finished = false;

        ss::gate::holder holder;
        ssx::semaphore_units units;
        ss::promise<std::error_code> done;
    };

    struct reply_status {
        ss::lowres_clock::time_point last_reply_timestamp
          = ss::lowres_clock::time_point::min();
        alive is_alive = alive::no;
    };

    using status_cache_t = absl::node_hash_map<model::node_id, reply_status>;
    using nhr_ptr = ss::lw_shared_ptr<const node_health_report>;
    using report_cache_t = absl::node_hash_map<model::node_id, nhr_ptr>;

    void tick();
    ss::future<std::error_code> collect_cluster_health_legacy();
    model::timeout_clock::time_point node_freshness(model::node_id id) const;
    /// Single-tick driver for the dissemination pull path. Defined in the .cc.
    class report_puller;
    ss::future<std::error_code>
      collect_cluster_health_disseminate(force_refresh);
    /// Refresh the self entry of \c _health_stores if it src_timestamp is
    /// older than \p min_ts. Holds \c _report_collection_mutex during the
    /// refresh and re-checks staleness inside the mutex so that concurrent
    /// callers serialize but only one regenerates the report.
    ss::future<> maybe_refresh_self(model::timeout_clock::time_point min_ts);
    ss::future<result<node_health_report>>
      collect_remote_node_health(model::node_id);
    ss::future<std::error_code> maybe_refresh_cluster_health(
      force_refresh, model::timeout_clock::time_point);
    ss::future<std::error_code> refresh_cluster_health_cache(force_refresh);

    cluster_health_report build_cluster_report(const cluster_report_filter&);

    std::optional<node_health_report_ptr>
    build_node_report(model::node_id, const node_report_filter&);

    ss::future<chunked_vector<topic_status>> collect_topic_status();

    // get the status info of all nodes which are past auto decommission timeout
    node_liveness_report collect_node_liveness_report();

    result<node_health_report>
      process_node_reply(model::node_id, result<get_node_health_reply>);

    std::chrono::milliseconds max_metadata_age();
    void abort_current_refresh();

    ss::future<errc> walk_local_and_remote_reports(
      health_monitor_backend_details::partition_leader_status_handler auto
        local_leader_handler,
      health_monitor_backend_details::partition_leader_status_handler auto
        remote_leader_handler,
      health_monitor_backend_details::partition_handler auto
        unclaimed_partition_handler);

    // read-only access to _reports
    // unsafe across scheduling points:
    const report_cache_t& reports() const;
    // safe across scheduling points:
    const ss::lw_shared_ptr<const report_cache_t> hold_reports() const;

    /**
     * @brief Stucture holding the aggregated results of partition status.
     */
    struct aggregated_report {
        // The size of the health status must be bounded: if all partitions
        // on a system with 50k partitions are under-replicated, it is not
        // helpful to try and cram all 50k NTPs into a vector here.
        static constexpr size_t max_partitions_report = 128;

        /**
         * List of leaderless or under-replicated ntps reported by any node.
         * The size of either list is capped at max_partitions_report, and
         * other elements are dropped.
         */
        chunked_hash_set<model::ntp> leaderless, under_replicated;

        /**
         * The true count of leaderless and under-replicated partitions, not
         * capped at max_partitions_report, and truncation of above the sets
         * can be detected when the size is larger than the corresponding set.
         */
        size_t leaderless_count{}, under_replicated_count{};

        bool operator==(const aggregated_report&) const = default;
    };

    static aggregated_report aggregate_reports(const report_cache_t& reports);
    /**
     * Offline nodes are missing the health reports, therefore the partitions
     * that replicas are only on those nodes will not be directly reported as
     * leader less. Therefore, we need to check the partitions that are only on
     * offline nodes and add them to the report.
     */
    ss::future<> fill_aggregate_with_offline_partitions(
      const std::vector<model::node_id>& offline_nodes,
      aggregated_report& aggr_report);

    ss::lw_shared_ptr<raft::consensus> _raft0;
    ss::sharded<members_table>& _members;
    ss::sharded<rpc::connection_cache>& _connections;
    ss::sharded<partition_manager>& _partition_manager;
    ss::sharded<raft::group_manager>& _raft_manager;
    ss::sharded<ss::abort_source>& _as;
    ss::sharded<drain_manager>& _drain_manager;
    ss::sharded<features::feature_table>& _feature_table;
    ss::sharded<partition_leaders_table>& _partition_leaders_table;
    ss::sharded<topic_table>& _topic_table;
    ss::sharded<node_status_table>& _node_status_table;

    ss::lowres_clock::time_point _last_refresh
      = ss::lowres_clock::time_point::min();
    // Number of times collect_cluster_health() has completed without
    // throwing. Exposed as the `refreshes` counter and gates the
    // `metadata_age_seconds` gauge (returns -1 while this is 0).
    uint64_t _refresh_count{0};
    ss::lw_shared_ptr<abortable_refresh_request> _refresh_request;

    status_cache_t _status;
    // individual reports get inserted but never get replaced or removed,
    // collection can also be replaced as a whole
    ss::lw_shared_ptr<report_cache_t> _reports;
    storage::disk_space_alert _reports_data_disk_health
      = storage::disk_space_alert::ok;
    bool _restart_risks_collected = false;
    std::optional<size_t> _bytes_in_cloud_storage;

    ss::gate _gate;
    ssx::mutex _refresh_mutex{"health_monitor_backend::refresh"};
    ss::sharded<node::local_monitor>& _local_monitor;
    model::node_id _self;
    model::node_boot_id _self_boot_id;

    std::vector<std::pair<cluster::notification_id_type, health_node_cb_t>>
      _node_callbacks;
    cluster::notification_id_type _next_callback_id{0};

    ssx::mutex _report_collection_mutex{"health_report_collection"};

    // Probe for the cluster_health_overview metrics; defined in .cc.
    class health_probe;
    friend class health_probe;
    std::unique_ptr<health_probe> _health_probe;

    // Per-node versioned health stores for the demand-driven pull protocol.
    absl::node_hash_map<model::node_id, health::versioned_health_store>
      _health_stores;

    friend struct health_report_accessor;
};

} // namespace cluster

namespace cluster::health {

/// Ring buffer of up to 8 diffs, ordered by start version.
/// On receipt stores a diff as it comes. On request, which is always of form
/// some_version..latest, existing diffs are composed.
class diff_store {
public:
    /// Add a new diff. If the buffer is full, the oldest-start diff is evicted.
    void add(diff_entry&& entry);

    /// Get a composed diff from version `from` to the latest stored version.
    /// Composes forward if needed, replacing intermediate diffs with
    /// composed results (e.g., A..B + B..C → replaces A..B with A..C,
    /// keeps B..C).
    /// Returns nullptr if relevant diff is not available.
    /// The returned pointer is valid until the next mutation of the store.
    const diff_entry* get_diff(node_health_version from);

    /// Replace the content of the most recent diff (the one ending at the
    /// latest version) with new data. Used when the latest version is
    /// regenerated in-place (version not sent, content updated).
    /// The diff's start and end versions are unchanged.
    void update_latest_diff(const diff_entry& incremental);

    /// Drop all stored diffs.
    void clear();

    /// The latest end version across all stored diffs, or nullopt if empty.
    std::optional<node_health_version> latest_version() const;

private:
    static constexpr size_t max_diffs = 8;
    using diffs_t = ss::circular_buffer_fixed_capacity<diff_entry, max_diffs>;
    diffs_t _diffs;
};

/// Manages versioned health data for a single source node.
/// Owns the diff_store plus version collapsing logic (sent-flag,
/// version reuse on regeneration).
///
/// On the source node, this is the authoritative state for self-reports.
/// On retransmitter nodes, sent_flag is not used (always considered sent).
class versioned_health_store {
public:
    /// The latest health data, or nullptr if no report has been produced yet.
    const node_health* current() const;

    /// The latest version, or nullopt if no report has been produced yet.
    std::optional<node_health_version> version() const;

    /// To be used on source node only: regenerate self-report. Computes and
    /// saves the diff from the previous report (if present) internally. If the
    /// previous version was never sent, the version number is reused and the
    /// latest diff is updated in-place. Otherwise a new version is created.
    /// \p self_boot stamps the version's boot id on the first call; subsequent
    /// calls preserve whatever boot id is already stored.
    void update_self(node_health report, model::node_boot_id self_boot);

    /// Receiving a full report from a peer. Replaces everything.
    /// Returns false if the report is stale (version <= current).
    bool update_from_report(versioned_report report);

    /// Receiving a diff from a peer. Applies it to the existing report.
    /// Returns false if the diff can't be applied (no current report,
    /// or diff.start doesn't match current version).
    bool update_from_diff(diff_entry&& diff);

    /// Get data for sending to a peer at the given version.
    /// Returns:
    ///   const diff_entry*       if a diff from `peer_version` is available
    ///   const versioned_report* if diff unavailable, sends full report
    ///   std::monostate          if no more fresh data available to send
    /// Marks the current version as sent.
    /// Returned pointers are valid until the next mutation of the store.
    using send_result = std::
      variant<std::monostate, const diff_entry*, const versioned_report*>;
    send_result
    get_for_sending(std::optional<node_health_version> peer_version);

    /// Timestamp of the last observed failure (heartbeat-down or RPC error)
    /// for this source. nullopt = no known failure since the last success.
    /// Stale entries keep their data and diff history; consumers should
    /// treat them as "no fresh data" and use the timestamp to back off
    /// retries.
    std::optional<ss::lowres_clock::time_point> last_failed_at() const {
        return _last_failed_at;
    }

    /// Mark this source as failed at \p t. Does not touch data or diffs.
    void mark_failed(ss::lowres_clock::time_point t) { _last_failed_at = t; }

    ss::lowres_clock::time_point freshness() const;

private:
    struct stored_report : versioned_report {
        bool sent = false;
    };

    std::optional<stored_report> _current;
    diff_store _diffs;
    std::optional<ss::lowres_clock::time_point> _last_failed_at;
};

} // namespace cluster::health
