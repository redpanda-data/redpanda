// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_io/cache_service.h"
#include "cluster/archival/upload_controller.h"
#include "cluster/controller.h"
#include "cluster/node/local_monitor.h"
#include "cluster/partition_manager.h"
#include "cluster/partition_recovery_manager.h"
#include "cluster/topic_recovery_service.h"
#include "cluster/topic_recovery_status_frontend.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "redpanda/application.h"
#include "resource_mgmt/scheduling_groups_probe.h"
#include "resource_mgmt/storage.h"
#include "syschecks/syschecks.h"

#include <seastar/core/seastar.hh>

// Forward declaration (defined in application_config.cc)
storage::backlog_controller_config
make_upload_controller_config(ss::scheduling_group sg, uint64_t fs_avail);

void application::wire_up_services_cloud_cache(
  model::node_id node_id, uint64_t fs_avail) {
    if (!archival_storage_enabled()) {
        return;
    }

    syschecks::systemd_message("Starting shadow indexing cache").get();
    auto redpanda_dir = config::node().data_directory.value();
    construct_service(
      shadow_index_cache,
      config::node().cloud_storage_cache_path(),
      local_monitor.local().get_state_cached().get_cache_disk().total,
      ss::sharded_parameter([] {
          return config::shard_local_cfg().disk_reservation_percent.bind();
      }),
      ss::sharded_parameter([] {
          return config::shard_local_cfg().cloud_storage_cache_size.bind();
      }),
      ss::sharded_parameter([] {
          return config::shard_local_cfg()
            .cloud_storage_cache_size_percent.bind();
      }),
      ss::sharded_parameter([] {
          return config::shard_local_cfg()
            .cloud_storage_cache_max_objects.bind();
      }),
      ss::sharded_parameter([] {
          return config::shard_local_cfg()
            .cloud_storage_cache_trim_walk_concurrency.bind();
      }))
      .get();

    // Hook up local_monitor to update storage_resources when disk state
    // changes
    auto cloud_storage_cache_disk_notification
      = storage_node.local().register_disk_notification(
        storage::node::disk_type::cache,
        [this](storage::node::disk_space_info info) {
            return shadow_index_cache.local().notify_disk_status(
              info.total, info.free, info.alert);
        });
    _deferred.emplace_back([this, cloud_storage_cache_disk_notification] {
        storage_node.local().unregister_disk_notification(
          storage::node::disk_type::cache,
          cloud_storage_cache_disk_notification);
    });

    shadow_index_cache
      .invoke_on_all([](cloud_io::cache& cache) { return cache.start(); })
      .get();

    construct_service(
      _archival_upload_controller,
      std::ref(partition_manager),
      ss::sharded_parameter(
        [sg = scheduling_groups::instance().archival_upload(), fs_avail] {
            return make_upload_controller_config(sg, fs_avail);
        }))
      .get();

    construct_service(
      topic_recovery_status_frontend,
      node_id,
      std::ref(_connection_cache),
      std::ref(controller->get_members_table()))
      .get();

    construct_service(
      topic_recovery_service,
      std::ref(cloud_storage_api),
      std::ref(controller->get_topics_state()),
      std::ref(controller->get_topics_frontend()),
      std::ref(topic_recovery_status_frontend))
      .get();

    partition_recovery_manager
      .invoke_on_all([this](cloud_storage::partition_recovery_manager& prm) {
          prm.set_topic_recovery_components(
            topic_recovery_status_frontend, topic_recovery_service);
      })
      .get();
}
