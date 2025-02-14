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
#include "config/property.h"
#include "container/chunked_hash_map.h"
#include "datalake/backlog_controller.h"
#include "datalake/fwd.h"
#include "datalake/location.h"
#include "datalake/record_schema_resolver.h"
#include "datalake/translation/partition_translator_v2.h"
#include "datalake/translation/scheduling.h"
#include "features/fwd.h"
#include "model/metadata.h"
#include "pandaproxy/schema_registry/fwd.h"
#include "raft/fwd.h"
#include "ssx/semaphore.h"
#include "ssx/work_queue.h"

#include <seastar/core/gate.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/defer.hh>

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

/*
 * Per shard instance responsible for launching and synchronizing all datalake
 * related tasks like file format translation, frontend etc.
 */
class datalake_manager : public ss::peering_sharded_service<datalake_manager> {
public:
    datalake_manager(
      model::node_id self,
      ss::sharded<raft::group_manager>*,
      ss::sharded<cluster::partition_manager>*,
      ss::sharded<cluster::topic_table>*,
      ss::sharded<cluster::topics_frontend>*,
      ss::sharded<cluster::partition_leaders_table>*,
      ss::sharded<cluster::shard_table>*,
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

    ss::future<> start();
    ss::future<> stop();

    /*
     * Return the amount of disk space currently in use by the datalake
     * subsystem (e.g. staged translated data on disk, etc...).
     *
     * This interface computes a global value, rather than shard local.
     */
    static ss::future<uint64_t> disk_usage();

private:
    using translator = std::unique_ptr<translation::partition_translator>;

    ss::future<> handle_translator_state_change(const model::ntp&);

    double average_translation_backlog();
    model::node_id _self;
    ss::sharded<raft::group_manager>* _group_mgr;
    ss::sharded<cluster::partition_manager>* _partition_mgr;
    ss::sharded<cluster::topic_table>* _topic_table;
    ss::sharded<cluster::topics_frontend>* _topics_frontend;
    ss::sharded<cluster::partition_leaders_table>* _leaders;
    ss::sharded<cluster::shard_table>* _shards;
    ss::sharded<features::feature_table>* _features;
    ss::sharded<coordinator::frontend>* _coordinator_frontend;
    std::unique_ptr<datalake::cloud_data_io> _cloud_data_io;
    location_provider _location_provider;
    std::unique_ptr<schema::registry> _schema_registry;
    std::unique_ptr<coordinator::catalog_factory> _catalog_factory;
    std::unique_ptr<iceberg::catalog> _catalog;
    std::unique_ptr<datalake::schema_manager> _schema_mgr;
    std::unique_ptr<datalake::type_resolver> _type_resolver;
    std::unique_ptr<datalake::schema_cache> _schema_cache;
    std::unique_ptr<backlog_controller> _backlog_controller;
    ss::sharded<ss::abort_source>* _as;
    ss::scheduling_group _sg;
    ss::gate _gate;

    using deferred_action = ss::deferred_action<std::function<void()>>;
    std::vector<deferred_action> _deregistrations;
    config::binding<model::iceberg_invalid_record_action>
      _iceberg_invalid_record_action;
    std::filesystem::path _writer_scratch_space;
    translation::scheduling::scheduler _scheduler;
    ssx::work_queue _queue;
};

} // namespace datalake
