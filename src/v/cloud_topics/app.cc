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
#include "cloud_topics/housekeeper/manager.h"
#include "cloud_topics/level_one/compaction/scheduler.h"
#include "cloud_topics/level_one/metastore/topic_purger.h"
#include "cloud_topics/level_zero/gc/level_zero_gc.h"
#include "cloud_topics/manager/manager.h"
#include "cloud_topics/reconciler/reconciler.h"
#include "cluster/cluster_epoch_service.h"
#include "cluster/controller.h"
#include "config/node_config.h"
#include "ssx/sharded_service_container.h"

#include <seastar/core/coroutine.hh>

namespace cloud_topics {

app::app(ss::sstring logger_name)
  : ssx::sharded_service_container(logger_name)
  , _logger_name(std::move(logger_name)) {}

app::~app() = default;

ss::future<> app::construct(
  model::node_id self,
  cluster::controller* controller,
  ss::sharded<cluster::partition_leaders_table>* leaders_table,
  ss::sharded<cluster::shard_table>* shard_table,
  ss::sharded<cloud_io::remote>* remote,
  ss::sharded<cloud_io::cache>* cloud_cache,
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

    // Touch the L1 staging directory before L1 i/o starts.
    co_await ss::recursive_touch_directory(
      config::node().l1_staging_path().string());

    co_await construct_service(
      l1_io,
      config::node().l1_staging_path(),
      ss::sharded_parameter([&remote] { return &remote->local(); }),
      bucket,
      ss::sharded_parameter([&cloud_cache] { return &cloud_cache->local(); }));

    co_await construct_service(
      domain_supervisor, controller, ss::sharded_parameter([this] {
          return &l1_io.local();
      }));

    co_await construct_service(
      l1_metastore_router,
      self,
      metadata_cache,
      leaders_table,
      shard_table,
      connection_cache,
      &domain_supervisor);

    co_await construct_service(
      replicated_metastore, ss::sharded_parameter([this] {
          return std::ref(l1_metastore_router.local());
      }));

    co_await construct_service(
      state,
      data_plane.get(),
      ss::sharded_parameter([this] { return &replicated_metastore.local(); }),
      ss::sharded_parameter([this] { return &l1_io.local(); }),
      ss::sharded_parameter(
        [&metadata_cache] { return &metadata_cache->local(); }));

    co_await construct_service(
      topic_purge_manager,
      ss::sharded_parameter([this] { return &replicated_metastore.local(); }),
      &controller->get_topics_state(),
      &controller->get_topics_frontend());

    co_await construct_service(
      reconciler,
      ss::sharded_parameter([this] { return &l1_io.local(); }),
      ss::sharded_parameter([this] { return &replicated_metastore.local(); }));

    co_await construct_service(
      l0_gc,
      self,
      ss::sharded_parameter([&] { return &remote->local(); }),
      bucket,
      &controller->get_health_monitor(),
      &controller->get_controller_stm(),
      &controller->get_topics_state(),
      &controller->get_members_table());

    co_await construct_service(housekeeper_manager, ss::sharded_parameter([&] {
                                   return &replicated_metastore.local();
                               }));

    construct_single_service(
      compaction_scheduler,
      l1::compaction_cluster_state{
        .self = self,
        .leaders_table = leaders_table,
        .topic_table = &controller->get_topics_state(),
        .metadata_cache = metadata_cache,
        .shard_table = &controller->get_shard_table(),
        .partition_manager = &controller->get_partition_manager()},
      &l1_io,
      &replicated_metastore);

    // Must be last to register so it will be first to be stopped in
    // `app::stop`. This is to ensure that stopped services don't receive
    // callbacks.
    co_await construct_service(
      manager,
      &controller->get_partition_manager(),
      &controller->get_raft_manager(),
      &controller->get_topics_state());
}

ss::future<> app::start() {
    co_await data_plane->start();
    co_await reconciler.invoke_on_all(&reconciler::reconciler::start);
    co_await domain_supervisor.invoke_on_all(
      [](auto& ds) { return ds.start(); });
    co_await housekeeper_manager.invoke_on_all(&housekeeper_manager::start);
    co_await compaction_scheduler->start();
    co_await l0_gc.invoke_on_all(&level_zero_gc::start);

    // When start is called, we must have registered all the callbacks before
    // this as starting the manager will invoke callbacks for partitions already
    // on the local shard.
    co_await wire_up_notifications();
    co_await manager.invoke_on_all([](auto& r) { return r.start(); });
}

ss::future<> app::wire_up_notifications() {
    co_await topic_purge_manager.invoke_on_all([this](auto& purge_mgr) {
        manager.local().on_l1_domain_leader([&purge_mgr](
                                              const model::ntp& ntp,
                                              const auto&,
                                              const auto& partition) noexcept {
            if (ntp.tp.partition != model::partition_id{0}) {
                return;
            }
            auto needs_loop = l1::topic_purger_manager::needs_loop{
              bool(partition)};
            purge_mgr.enqueue_loop_reset(needs_loop);
        });
    });
    co_await housekeeper_manager.invoke_on_all([this](auto& hm) {
        manager.local().on_ctp_partition_leader(
          [&hm](
            const model::ntp&,
            const model::topic_id_partition& tidp,
            auto partition) noexcept {
              if (partition) {
                  hm.start_housekeeper(tidp, std::move(*partition));
              } else {
                  hm.stop_housekeeper(tidp);
              }
          });
    });
    co_await reconciler.invoke_on_all([this](auto& r) {
        manager.local().on_ctp_partition_leader(
          [this, &r](
            const model::ntp& ntp,
            const model::topic_id_partition& tidp,
            auto partition) noexcept {
              if (partition) {
                  r.attach_partition(
                    ntp, tidp, data_plane.get(), std::move(*partition));
              } else {
                  r.detach(ntp);
              }
          });
    });
    co_await domain_supervisor.invoke_on_all([this](auto& ds) {
        manager.local().on_l1_domain_leader([&ds](
                                              const model::ntp& ntp,
                                              const model::topic_id_partition&,
                                              auto partition) noexcept {
            ds.on_domain_leadership_change(ntp, std::move(partition));
        });
    });
}

ss::future<> app::stop() {
    ssx::sharded_service_container::shutdown();
    co_await data_plane->stop();
}

ss::sharded<l1::leader_router>* app::get_sharded_l1_metastore_router() {
    return &l1_metastore_router;
}

ss::sharded<l1::domain_supervisor>* app::get_sharded_l1_domain_supervisor() {
    return &domain_supervisor;
}

ss::sharded<reconciler::reconciler>* app::get_reconciler() {
    return &reconciler;
}

ss::sharded<state_accessors>* app::get_state() { return &state; }

ss::sharded<l1::replicated_metastore>* app::get_sharded_replicated_metastore() {
    return &replicated_metastore;
}

l1::compaction_scheduler* app::get_compaction_scheduler() {
    return compaction_scheduler.get();
}

} // namespace cloud_topics
