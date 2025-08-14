/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/app.h"

#include "cloud_topics/cluster_services.h"
#include "cloud_topics/data_plane_api.h"
#include "cloud_topics/data_plane_impl.h"
#include "cluster/cluster_epoch_service.h"
#include "cluster/controller.h"
#include "ssx/sharded_service_container.h"

#include <seastar/core/coroutine.hh>

namespace experimental::cloud_topics {

app::app(ss::sstring logger_name)
  : ssx::sharded_service_container(logger_name)
  , _logger_name(std::move(logger_name)) {}

app::~app() = default;

ss::future<> app::construct(
  model::node_id self,
  cluster::controller* controller,
  ss::sharded<cluster::partition_manager>* partition_mgr,
  ss::sharded<cluster::partition_leaders_table>* leaders_table,
  ss::sharded<cluster::shard_table>* shard_table,
  ss::sharded<cloud_io::remote>* remote,
  ss::sharded<cloud_storage::cache>* cloud_cache,
  ss::sharded<cluster::metadata_cache>* metadata_cache,
  ss::sharded<rpc::connection_cache>* connection_cache,
  cloud_storage_clients::bucket_name bucket,
  ss::sharded<storage::api>* storage) {
    data_plane = co_await make_data_plane(
      ssx::sformat("{}::data_plane", _logger_name),
      remote,
      cloud_cache,
      bucket,
      storage,
      &controller->get_cluster_epoch_generator());

    co_await construct_service(state, data_plane.get());

    co_await construct_service(
      reconciler, partition_mgr, remote, data_plane.get(), bucket);
    co_await construct_service(domain_supervisor, controller);
    co_await construct_service(
      l1_metastore_fe,
      self,
      metadata_cache,
      leaders_table,
      shard_table,
      connection_cache,
      &domain_supervisor);
}

ss::future<> app::start() {
    co_await data_plane->start();
    co_await reconciler.invoke_on_all([](auto& r) { return r.start(); });
    co_await domain_supervisor.invoke_on_all(
      [](auto& ds) { return ds.start(); });
}

ss::future<> app::stop() {
    ssx::sharded_service_container::shutdown();
    co_await data_plane->stop();
    co_return;
}

ss::sharded<l1::frontend>* app::get_sharded_l1_metastore_fe() {
    return &l1_metastore_fe;
}

ss::sharded<l1::domain_supervisor>* app::get_sharded_l1_domain_supervisor() {
    return &domain_supervisor;
}

ss::sharded<state_accessors>* app::get_state() { return &state; }

} // namespace experimental::cloud_topics
