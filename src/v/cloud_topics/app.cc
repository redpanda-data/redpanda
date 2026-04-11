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
#include "cloud_topics/level_one/metastore/flush_loop.h"
#include "cloud_topics/level_one/metastore/topic_purger.h"
#include "cloud_topics/level_zero/gc/level_zero_gc.h"
#include "cloud_topics/logger.h"
#include "cloud_topics/manager/manager.h"
#include "cloud_topics/read_replica/metadata_manager.h"
#include "cloud_topics/read_replica/snapshot_manager.h"
#include "cloud_topics/reconciler/reconciler.h"
#include "cloud_topics/topic_manifest_upload_manager.h"
#include "cluster/cluster_epoch_service.h"
#include "cluster/controller.h"
#include "cluster/utils/partition_change_notifier_impl.h"
#include "config/node_config.h"
#include "resource_mgmt/cpu_scheduling.h"
#include "ssx/future-util.h"
#include "ssx/sharded_service_container.h"
#include "utils/directory_walker.h"

#include <seastar/core/coroutine.hh>

#include <deque>
#include <filesystem>

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
  ss::sharded<storage::api>* storage,
  bool skip_flush_loop,
  bool skip_level_zero_gc) {
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

    co_await construct_service(_l1_reader_probe);

    co_await construct_service(
      l1_io,
      config::node().l1_staging_path(),
      ss::sharded_parameter([&remote] { return &remote->local(); }),
      bucket,
      ss::sharded_parameter([&cloud_cache] { return &cloud_cache->local(); }));

    co_await construct_service(
      domain_supervisor,
      controller,
      ss::sharded_parameter([this] { return &l1_io.local(); }),
      config::node().l1_staging_path(),
      ss::sharded_parameter([&remote] { return &remote->local(); }),
      bucket,
      scheduling_groups::instance().cloud_topics_metastore_sg());

    co_await construct_service(
      l1_metastore_router,
      self,
      metadata_cache,
      leaders_table,
      shard_table,
      connection_cache,
      &domain_supervisor);

    co_await construct_service(
      replicated_metastore,
      ss::sharded_parameter(
        [this] { return std::ref(l1_metastore_router.local()); }),
      ss::sharded_parameter([&remote] { return std::ref(remote->local()); }),
      bucket);

    co_await construct_service(l1_reader_cache_);

    co_await construct_service(
      rr_snapshot_manager_,
      config::node().l1_staging_path(),
      ss::sharded_parameter([&remote] { return &remote->local(); }),
      ss::sharded_parameter([&cloud_cache] { return &cloud_cache->local(); }));

    co_await construct_service(
      rr_metadata_manager_,
      ss::sharded_parameter([controller] {
          return cluster::partition_change_notifier_impl::make_default(
            controller->get_raft_manager(),
            controller->get_partition_manager(),
            controller->get_topics_state());
      }),
      std::ref(controller->get_partition_manager()),
      ss::sharded_parameter([this] { return &rr_snapshot_manager_.local(); }),
      std::ref(*remote),
      std::ref(controller->get_topics_state()));

    co_await construct_service(
      state,
      data_plane.get(),
      ss::sharded_parameter([this] { return &replicated_metastore.local(); }),
      ss::sharded_parameter([this] { return &l1_io.local(); }),
      ss::sharded_parameter(
        [&metadata_cache] { return &metadata_cache->local(); }),
      ss::sharded_parameter([this] { return &_l1_reader_probe.local(); }),
      ss::sharded_parameter([this] { return &l1_reader_cache_.local(); }),
      ss::sharded_parameter([this] { return &rr_metadata_manager_.local(); }),
      ss::sharded_parameter([this] { return &rr_snapshot_manager_.local(); }));

    co_await construct_service(
      topic_purge_manager,
      ss::sharded_parameter([this] { return &replicated_metastore.local(); }),
      &controller->get_topics_state(),
      &controller->get_topics_frontend());

    if (!skip_flush_loop) {
        co_await construct_service(
          flush_loop_manager, ss::sharded_parameter([this] {
              return &replicated_metastore.local();
          }));
    }

    co_await construct_service(
      reconciler,
      ss::sharded_parameter([this] { return &l1_io.local(); }),
      ss::sharded_parameter([this] { return &replicated_metastore.local(); }),
      ss::sharded_parameter(
        [&metadata_cache] { return &metadata_cache->local(); }),
      scheduling_groups::instance().cloud_topics_reconciler_sg());

    if (!skip_level_zero_gc) {
        co_await construct_service(
          l0_gc,
          self,
          ss::sharded_parameter([&] { return &remote->local(); }),
          bucket,
          &controller->get_health_monitor(),
          &controller->get_controller_stm(),
          &controller->get_topics_state(),
          &controller->get_members_table());
    }

    co_await construct_service(housekeeper_manager, ss::sharded_parameter([&] {
                                   return &replicated_metastore.local();
                               }));

    co_await construct_service(
      topic_manifest_upload_mgr, std::ref(*remote), bucket);

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
      &replicated_metastore,
      &_l1_reader_probe);

    // Must be last to register so it will be first to be stopped in
    // `app::stop`. This is to ensure that stopped services don't receive
    // callbacks.
    co_await construct_service(
      manager,
      &controller->get_partition_manager(),
      &controller->get_raft_manager(),
      &controller->get_topics_state());

    co_await construct_service(
      cluster_services, std::ref(controller->get_cluster_epoch_generator()));
}

ss::future<> app::start() {
    co_await cleanup_tmp_files();

    co_await data_plane->start();
    co_await reconciler.invoke_on_all(&reconciler::reconciler<>::start);
    co_await domain_supervisor.invoke_on_all(
      [](auto& ds) { return ds.start(); });
    co_await housekeeper_manager.invoke_on_all(&housekeeper_manager::start);
    co_await topic_manifest_upload_mgr.invoke_on_all(
      &topic_manifest_upload_manager::start);
    co_await compaction_scheduler->start();
    if (l0_gc.local_is_initialized()) {
        co_await l0_gc.invoke_on_all(&level_zero_gc::start);
    }
    if (flush_loop_manager.local_is_initialized()) {
        co_await flush_loop_manager.invoke_on_all(
          &l1::flush_loop_manager::start);
    }

    // Start read replica metadata manager
    co_await rr_metadata_manager_.invoke_on_all(
      &read_replica::metadata_manager::start);

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
    if (flush_loop_manager.local_is_initialized()) {
        co_await flush_loop_manager.invoke_on_all([this](auto& flm) {
            manager.local().on_l1_domain_leader(
              [&flm](
                const model::ntp& ntp,
                const auto&,
                const auto& partition) noexcept {
                  if (ntp.tp.partition != model::partition_id{0}) {
                      return;
                  }
                  auto needs_loop = l1::flush_loop_manager::needs_loop{
                    bool(partition)};
                  flm.enqueue_loop_reset(needs_loop);
              });
        });
    }
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
    co_await topic_manifest_upload_mgr.invoke_on_all([this](auto& mgr) {
        manager.local().on_ctp_leader_properties_change(
          [&mgr](
            const model::ntp& ntp,
            const model::topic_id_partition& tidp,
            auto partition) noexcept {
              if (ntp.tp.partition != model::partition_id{0}) {
                  return;
              }
              mgr.on_leadership_or_properties_change(
                tidp, std::move(partition));
          });
    });
}

ss::future<> app::cleanup_tmp_files() {
    auto staging_dir = config::node().l1_staging_path();
    std::deque<std::filesystem::path> dirs_to_process;
    dirs_to_process.push_back(staging_dir);

    // TODO: maybe we should parallelize this.
    size_t deleted_count = 0;
    while (!dirs_to_process.empty()) {
        auto current_dir = std::move(dirs_to_process.front());
        dirs_to_process.pop_front();
        auto walk_fut = co_await ss::coroutine::as_future(
          directory_walker::walk(
            current_dir.string(),
            [&current_dir, &dirs_to_process, &deleted_count](
              this auto, ss::directory_entry entry) -> ss::future<> {
                auto entry_path = current_dir / entry.name.c_str();
                if (!entry.type.has_value()) {
                    vlog(
                      cd_log.info,
                      "Skipping cleanup of unknown file type file: {}",
                      entry_path.string());
                    co_return;
                }
                if (entry.type == ss::directory_entry_type::directory) {
                    dirs_to_process.push_back(entry_path);
                    co_return;
                }
                if (entry.type == ss::directory_entry_type::regular) {
                    if (std::string_view(entry.name).contains(".tmp")) {
                        auto entry_path_str = entry_path.string();
                        auto rm_fut = co_await ss::coroutine::as_future(
                          ss::remove_file(entry_path_str));
                        if (rm_fut.failed()) {
                            auto ex = rm_fut.get_exception();
                            auto lvl = ssx::is_shutdown_exception(ex)
                                         ? ss::log_level::debug
                                         : ss::log_level::warn;
                            vlogl(
                              cd_log,
                              lvl,
                              "Failed to delete tmp file {}: {}",
                              entry_path_str,
                              ex);
                            co_return;
                        }
                        deleted_count++;
                    }
                }
            }));

        if (walk_fut.failed()) {
            auto ex = walk_fut.get_exception();
            auto lvl = ssx::is_shutdown_exception(ex) ? ss::log_level::debug
                                                      : ss::log_level::warn;
            vlogl(
              cd_log, lvl, "Failed to walk directory {}: {}", staging_dir, ex);
        }
    }

    if (deleted_count > 0) {
        vlog(
          cd_log.info,
          "Cleanup deleted {} tmp file(s) from {}",
          deleted_count,
          staging_dir);
    } else {
        vlog(cd_log.debug, "No tmp files found to cleanup in {}", staging_dir);
    }
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

ss::sharded<reconciler::reconciler<>>* app::get_reconciler() {
    return &reconciler;
}

ss::sharded<state_accessors>* app::get_state() { return &state; }

ss::sharded<l1::replicated_metastore>* app::get_sharded_replicated_metastore() {
    return &replicated_metastore;
}

l1::compaction_scheduler* app::get_compaction_scheduler() {
    return compaction_scheduler.get();
}

ss::sharded<level_zero_gc>* app::get_level_zero_gc() { return &l0_gc; }

cluster_services& app::get_local_cluster_services() {
    return std::ref(cluster_services.local());
}

} // namespace cloud_topics
