/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "base/units.h"
#include "cluster/fwd.h"
#include "cluster/utils/partition_change_notifier.h"
#include "config/property.h"
#include "container/chunked_hash_map.h"
#include "datalake/backlog_controller.h"
#include "datalake/fwd.h"
#include "datalake/location.h"
#include "datalake/record_schema_resolver.h"
#include "datalake/translation/partition_translator.h"
#include "datalake/translation/scheduling.h"
#include "datalake/translation/translation_probe.h"
#include "features/fwd.h"
#include "model/metadata.h"
#include "pandaproxy/schema_registry/fwd.h"
#include "raft/fwd.h"
#include "ssx/semaphore.h"
#include "ssx/work_queue.h"

#include <seastar/core/gate.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/sharded.hh>

namespace cloud_io {
class remote;
} // namespace cloud_io
namespace iceberg {
class catalog;
} // namespace iceberg
namespace schema {
class registry;
} // namespace schema

namespace datalake {

class core_0_disk_manager;

/*
 * Per shard instance responsible for launching and synchronizing all datalake
 * related tasks like file format translation, frontend etc.
 */
class datalake_manager : public ss::peering_sharded_service<datalake_manager> {
public:
    datalake_manager(
      model::node_id self,
      std::unique_ptr<cluster::partition_change_notifier>,
      ss::sharded<cluster::partition_manager>*,
      ss::sharded<cluster::topic_table>*,
      ss::sharded<features::feature_table>*,
      ss::sharded<coordinator::frontend>*,
      ss::sharded<cloud_io::remote>*,
      std::unique_ptr<coordinator::catalog_factory>,
      pandaproxy::schema_registry::api* schema_registry,
      ss::sharded<ss::abort_source>*,
      cloud_storage_clients::bucket_name,
      ss::scheduling_group sg,
      size_t memory_limit);
    ~datalake_manager();

    /*
     * Call prepare_staging_directory before starting. Preparation involves
     * clearing out the directory, and since start() will be invoked on all
     * cores we expect the caller to prepare the directory to avoid potential
     * affects of concurrent file creates and deletes.
     */
    ss::future<> start();

    ss::future<> shutdown();

    /*
     * Return the amount of disk space currently in use by the datalake
     * subsystem (e.g. staged translated data on disk, etc...).
     *
     * This interface computes a global value, rather than shard local.
     */
    static ss::future<uint64_t> disk_usage();

    /**
     * Returns the number of partitions that the translator is not able to keep
     * up with.
     */
    size_t overdue_translation_partition_count() const;
    /**
     * Returns count of partitions that translation is blocked. This value
     * should be 0 in normal conditions.
     */
    size_t partitions_with_translation_blocked() const;
    /**
     * Returns true if datalake translation runs with maximum allowed priority.
     */
    bool max_shares_assigned() const;

    /*
     * Ensure that the datalake scratch directory exists and is empty. The
     * directory isn't required to be empty when starting up, but there is no
     * way for translators to resume processing files so clearing the directory
     * is a convenient way to deal with orphaned files.
     */
    static ss::future<> prepare_staging_directory(std::filesystem::path);

    /**
     * Returns total number of bytes that are ready to be translated for all
     * datalake enabled partitions on a current shard.
     */
    size_t total_translation_backlog() const;

private:
    using translator = std::unique_ptr<translation::partition_translator>;

    ss::future<> handle_translator_state_change(
      model::ntp ntp,
      std::optional<cluster::partition_change_notifier::partition_state>);

    /// \note The probe is created on the first use.
    ss::lw_shared_ptr<translation_probe> get_or_create_probe(const model::ntp&);

    /*
     * Background loop that periodically invokes `check_disk_space`.
     */
    ss::future<> disk_space_monitor();

    /*
     * Checks disk space usage across all translators, and if the total usage
     * exceeds the configured limits for datalake, request schedulers to finish
     * translations and free disk space.
     */
    ss::future<> check_and_manage_disk_space();

    /*
     * Returns the disk soft limit, which is the threshold above which disk
     * utilization will trigger asynchronous requests to translators to finish
     * their translations and release on disk resources.
     */
    size_t disk_space_soft_limit();

    /*
     * Returns true if the disk space soft limit were breached.
     */
    bool disk_space_soft_limit_exceeded();

    friend class core_0_disk_manager;
    ss::future<size_t> reserve_disk(ss::shard_id);

    /*
     * A helper that can be called when disk limit configuration values change
     * which will recompute the active configuration.
     */
    void update_disk_limits();

private:
    model::node_id _self;
    std::unique_ptr<cluster::partition_change_notifier>
      _partition_notifications;
    ss::sharded<cluster::partition_manager>* _partition_mgr;
    ss::sharded<cluster::topic_table>* _topic_table;
    ss::sharded<features::feature_table>* _features;
    ss::sharded<coordinator::frontend>* _coordinator_frontend;
    std::unique_ptr<datalake::cloud_data_io> _cloud_data_io;
    location_provider _location_provider;
    std::unique_ptr<schema::registry> _schema_registry;
    std::unique_ptr<coordinator::catalog_factory> _catalog_factory;
    std::unique_ptr<iceberg::catalog> _catalog;
    std::unique_ptr<datalake::schema_manager> _schema_mgr;
    std::unique_ptr<datalake::schema_cache> _schema_cache;
    std::unique_ptr<datalake::resolved_type_cache> _resolved_type_cache;
    std::unique_ptr<backlog_controller> _backlog_controller;
    chunked_hash_map<model::ntp, ss::lw_shared_ptr<class translation_probe>>
      _translation_probe_by_ntp;
    ss::sharded<ss::abort_source>* _as;
    ss::scheduling_group _sg;
    ss::gate _gate;

    cluster::notification_id_type _partition_notifications_id
      = cluster::notification_id_type_invalid;
    config::binding<model::iceberg_invalid_record_action>
      _iceberg_invalid_record_action;
    std::filesystem::path _writer_scratch_space;
    std::unique_ptr<core_0_disk_manager> _disk_manager;
    translation::scheduling::scheduler _scheduler;
    ssx::work_queue _queue;
    ss::condition_variable _disk_space_monitor_cv;
    /*
     * _disk_bytes_reservable_total
     *
     * - this is the total amount of disk space that can be reserved. it is
     * a fixed value set when configuration changes. it's basically the upper
     * limit on the total size of the scratch space used on the system. it
     * is only accessed by core 0.
     *
     * _core0_disk_bytes_reservable
     *
     * - this is a semaphore that represents a subset of the reservable
     * total which is currently available for reserving. when the system
     * starts this semaphore has units equal to the reservable total. as
     * translators on other cores reserve disk space, they subtract off
     * units from this semaphore, making those units unreservable. the
     * semaphore is only used on core 0.
     *
     * schedulers consume units from the semaphore by contacting the datalake
     * manager on core 0. currently cores do not return units willingly.
     * instead, when the data lake manager is low on reservable space, it (1)
     * requests translators to free space by finishing and then (2) steals
     * unreserved units from each core to be handed back out on demand.
     */
    size_t _disk_bytes_reservable_total{0};
    size_t _disk_bytes_reservable_soft_limit{0};
    ssx::semaphore _core0_disk_bytes_reservable;
    config::binding<bool> _disk_space_manager_enable;
    config::binding<size_t> _scratch_space_size_bytes;
    config::binding<double> _scratch_space_soft_limit_size_percent;
};

} // namespace datalake
