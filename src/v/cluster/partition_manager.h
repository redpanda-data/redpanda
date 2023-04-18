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

#include "archival/fwd.h"
#include "cloud_storage/fwd.h"
#include "cluster/fwd.h"
#include "cluster/ntp_callbacks.h"
#include "cluster/partition.h"
#include "cluster/types.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "raft/consensus_client_protocol.h"
#include "raft/group_manager.h"
#include "raft/heartbeat_manager.h"
#include "storage/api.h"
#include "utils/named_type.h"

#include <absl/container/flat_hash_map.h>

namespace cluster {
class partition_manager {
public:
    using ntp_table_container
      = absl::flat_hash_map<model::ntp, ss::lw_shared_ptr<partition>>;

    partition_manager(
      ss::sharded<storage::api>&,
      ss::sharded<raft::group_manager>&,
      ss::sharded<cluster::tx_gateway_frontend>&,
      ss::sharded<cloud_storage::partition_recovery_manager>&,
      ss::sharded<cloud_storage::remote>&,
      ss::sharded<cloud_storage::cache>&,
      ss::lw_shared_ptr<const archival::configuration>,
      ss::sharded<features::feature_table>&,
      ss::sharded<cluster::tm_stm_cache_manager>&,
      ss::sharded<archival::upload_housekeeping_service>&,
      config::binding<uint64_t>);

    ~partition_manager();

    using manage_cb_t
      = ss::noncopyable_function<void(ss::lw_shared_ptr<partition>)>;
    using unmanage_cb_t = ss::noncopyable_function<void(model::partition_id)>;

    /// \brief Copies table with ntps matching a given topic namespace
    ntp_table_container
    get_topic_partition_table(const model::topic_namespace&) const;

    inline ss::lw_shared_ptr<partition> get(const model::ntp& ntp) const {
        if (auto it = _ntp_table.find(ntp); it != _ntp_table.end()) {
            return it->second;
        }
        return nullptr;
    }

    /// \brief raw api for raft/service.h
    inline consensus_ptr consensus_for(raft::group_id group) const {
        if (auto it = _raft_table.find(group); it != _raft_table.end()) {
            return it->second->raft();
        }
        return nullptr;
    }

    inline ss::lw_shared_ptr<partition>
    partition_for(raft::group_id group) const {
        if (auto it = _raft_table.find(group); it != _raft_table.end()) {
            return it->second;
        }
        return nullptr;
    }

    ss::future<> start() { return ss::now(); }
    ss::future<> stop_partitions();
    ss::future<consensus_ptr> manage(
      storage::ntp_config,
      raft::group_id,
      std::vector<model::broker>,
      std::optional<remote_topic_properties> = std::nullopt,
      std::optional<cloud_storage_clients::bucket_name> = std::nullopt,
      raft::with_learner_recovery_throttle
      = raft::with_learner_recovery_throttle::yes,
      raft::keep_snapshotted_log = raft::keep_snapshotted_log::no);

    ss::future<> shutdown(const model::ntp& ntp);

    ss::future<> remove(const model::ntp& ntp, partition_removal_mode mode);

    /*
     * register for notification of new partitions within the specific topic
     * being managed. this will invoke the callback for existing partitions
     * synchronously so the caller must be prepared for that.
     *
     * the callback must not block.
     *
     * we don't currently have any mechanism for un-managing partitions, so
     * that interface is non-existent.
     */
    notification_id_type register_manage_notification(
      const model::ns& ns, const model::topic& topic, manage_cb_t cb) {
        /*
         * first create a cb filter and apply all existing partitions. this
         * ensures that the caller initializes its state with existing
         * partitions.
         */
        ntp_callbacks<manage_cb_t> init;
        init.register_notify(
          ns, topic, [&cb](ss::lw_shared_ptr<partition> p) { cb(p); });
        for (auto& e : _ntp_table) {
            init.notify(e.first, e.second);
        }

        // now setup the permenant callback for new partitions
        return _manage_watchers.register_notify(ns, topic, std::move(cb));
    }

    /*
     * register for notification of partitions within the specific topic
     * being removed from manager. this will invoke the callback for existing
     * partitions synchronously so the caller must be prepared for that.
     *
     * the callback must not block.
     *
     * we don't currently have any mechanism for un-managing partitions, so
     * that interface is non-existent.
     */
    notification_id_type register_unmanage_notification(
      const model::ns& ns, const model::topic& topic, unmanage_cb_t cb) {
        return _unmanage_watchers.register_notify(ns, topic, std::move(cb));
    }

    void unregister_manage_notification(notification_id_type id) {
        _manage_watchers.unregister_notify(id);
    }
    void unregister_unmanage_notification(notification_id_type id) {
        _unmanage_watchers.unregister_notify(id);
    }

    /*
     * read-only interface to partitions.
     *
     * note that users of this interface must take care not to hold iterators
     * across scheduling events as the underlying table may be modified and
     * invalidate iterators.
     */
    const ntp_table_container& partitions() const { return _ntp_table; }

    ss::sharded<cluster::tx_gateway_frontend>& get_tx_frontend() {
        return _tx_gateway_frontend;
    }

    /*
     * Block/unblock current node from leadership for new and existing raft
     * groups.
     */
    void block_new_leadership() {
        _block_new_leadership = true;
        for (const auto& p : _ntp_table) {
            p.second->block_new_leadership();
        }
    }

    void unblock_new_leadership() {
        _block_new_leadership = false;
        for (const auto& p : _ntp_table) {
            p.second->unblock_new_leadership();
        }
    }

    /// Report the aggregate backlog of all archivers for all managed
    /// partitions
    uint64_t upload_backlog_size() const;

private:
    /// Download log if partition_recovery_manager is initialized.
    ///
    /// It might not be initialized if cloud storage is disable.
    /// In this case this method always returns false.
    /// \param ntp_cfg is an ntp_config instance to recover
    /// \return true if the recovery was invoked, false otherwise
    ss::future<cloud_storage::log_recovery_result> maybe_download_log(
      storage::ntp_config& ntp_cfg, std::optional<remote_topic_properties> rtp);

    ss::future<> do_shutdown(ss::lw_shared_ptr<partition>);

    storage::api& _storage;
    /// used to wait for concurrent recoveries
    ss::sharded<raft::group_manager>& _raft_manager;

    ntp_callbacks<manage_cb_t> _manage_watchers;
    ntp_callbacks<unmanage_cb_t> _unmanage_watchers;
    // XXX use intrusive containers here
    ntp_table_container _ntp_table;
    absl::flat_hash_map<raft::group_id, ss::lw_shared_ptr<partition>>
      _raft_table;
    ss::sharded<cluster::tx_gateway_frontend>& _tx_gateway_frontend;
    ss::sharded<cloud_storage::partition_recovery_manager>&
      _partition_recovery_mgr;
    ss::sharded<cloud_storage::remote>& _cloud_storage_api;
    ss::sharded<cloud_storage::cache>& _cloud_storage_cache;
    ss::lw_shared_ptr<const archival::configuration> _archival_conf;
    ss::sharded<features::feature_table>& _feature_table;
    ss::sharded<cluster::tm_stm_cache_manager>& _tm_stm_cache_manager;
    ss::sharded<archival::upload_housekeeping_service>& _upload_hks;
    ss::gate _gate;

    // In general, all our background work is in partition objects which
    // have their own abort source.  This abort source is only for work that
    // happens after partition stop, during deletion.
    ss::abort_source _as;

    bool _block_new_leadership{false};

    config::binding<uint64_t> _max_concurrent_producer_ids;

    // Our handle from registering for leadership notifications on group_manager
    std::optional<cluster::notification_id_type> _leader_notify_handle;

    friend std::ostream& operator<<(std::ostream&, const partition_manager&);
};
} // namespace cluster
