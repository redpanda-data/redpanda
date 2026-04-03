/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/level_one/common/file_io.h"
#include "cloud_topics/level_one/compaction/scheduler.h"
#include "cloud_topics/level_one/domain/domain_supervisor.h"
#include "cloud_topics/level_one/frontend_reader/l1_reader_cache.h"
#include "cloud_topics/level_one/frontend_reader/level_one_reader_probe.h"
#include "cloud_topics/level_one/metastore/leader_router.h"
#include "cloud_topics/level_one/metastore/replicated_metastore.h"
#include "cloud_topics/level_zero/cluster_services_impl/cluster_services.h"
#include "cloud_topics/reconciler/reconciler.h"
#include "cloud_topics/state_accessors.h"
#include "ssx/sharded_service_container.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sharded.hh>

namespace cloud_io {
class remote;
} // namespace cloud_io
namespace cloud_storage {
class cache;
} // namespace cloud_storage
namespace storage {
class api;
} // namespace storage

namespace cloud_topics {
class data_plane_api;
class cloud_topics_manager;
template<class>
class level_zero_gc_t;
class housekeeper_manager;
class topic_manifest_upload_manager;

namespace l1 {
class flush_loop_manager;
class topic_purger_manager;
} // namespace l1

namespace read_replica {
class snapshot_manager;
class metadata_manager;
} // namespace read_replica

class app : public ssx::sharded_service_container {
public:
    explicit app(ss::sstring logger_name = "cloud_topics::app");

    app(const app&) = delete;
    app& operator=(const app&) = delete;
    app(app&&) noexcept = delete;
    app& operator=(app&&) noexcept = delete;
    ~app();

    ss::future<> construct(
      model::node_id,
      cluster::controller*,
      ss::sharded<cluster::partition_leaders_table>*,
      ss::sharded<cluster::shard_table>*,
      ss::sharded<cloud_io::remote>*,
      ss::sharded<cloud_io::cache>*,
      ss::sharded<cluster::metadata_cache>*,
      ss::sharded<rpc::connection_cache>*,
      cloud_storage_clients::bucket_name,
      ss::sharded<storage::api>*,
      bool skip_flush_loop = false,
      bool skip_level_zero_gc = false);

    ss::future<> start();

    // Call stop on each sharded service and call their destructors.
    ss::future<> stop();

    ss::sharded<l1::leader_router>* get_sharded_l1_metastore_router();
    ss::sharded<state_accessors>* get_state();
    ss::sharded<l1::domain_supervisor>* get_sharded_l1_domain_supervisor();
    ss::sharded<reconciler::reconciler<>>* get_reconciler();
    ss::sharded<l1::replicated_metastore>* get_sharded_replicated_metastore();
    l1::compaction_scheduler* get_compaction_scheduler();
    ss::sharded<level_zero_gc_t<ss::lowres_clock>>* get_level_zero_gc();
    cluster_services& get_local_cluster_services();

    // TODO: add 'get_control_plane_api' etc

private:
    ss::future<> wire_up_notifications();

    // Cleans up the temporary files in the L1 staging directory.
    ss::future<> cleanup_tmp_files();

    ss::sstring _logger_name;
    ss::sharded<level_one_reader_probe> _l1_reader_probe;
    std::unique_ptr<data_plane_api> data_plane;
    ss::sharded<l1_reader_cache> l1_reader_cache_;
    ss::sharded<state_accessors> state;
    ss::sharded<l1::file_io> l1_io;
    ss::sharded<l1::replicated_metastore> replicated_metastore;
    ss::sharded<reconciler::reconciler<>> reconciler;
    ss::sharded<l1::domain_supervisor> domain_supervisor;
    ss::sharded<l1::leader_router> l1_metastore_router;
    ss::sharded<l1::topic_purger_manager> topic_purge_manager;
    ss::sharded<l1::flush_loop_manager> flush_loop_manager;
    ss::sharded<cloud_topics_manager> manager;
    ss::sharded<level_zero_gc_t<ss::lowres_clock>> l0_gc;
    ss::sharded<housekeeper_manager> housekeeper_manager;
    ss::sharded<topic_manifest_upload_manager> topic_manifest_upload_mgr;
    std::unique_ptr<l1::compaction_scheduler> compaction_scheduler;
    ss::sharded<l0::cluster_services> cluster_services;

    // Read replica components
    ss::sharded<read_replica::snapshot_manager> rr_snapshot_manager_;
    ss::sharded<read_replica::metadata_manager> rr_metadata_manager_;
};

} // namespace cloud_topics
