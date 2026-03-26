// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_io/remote.h"
#include "cloud_storage/configuration.h"
#include "cloud_storage/remote.h"
#include "cloud_storage_clients/client_pool.h"
#include "cloud_storage_clients/configuration.h"
#include "cloud_storage_clients/upstream_registry.h"
#include "cluster/archival/archiver_manager.h"
#include "cluster/archival/ntp_archiver_service.h"
#include "cluster/archival/purger.h"
#include "cluster/archival/upload_housekeeping_service.h"
#include "cluster/cloud_metadata/offsets_upload_router.h"
#include "cluster/cloud_metadata/offsets_uploader.h"
#include "cluster/controller.h"
#include "cluster/partition_manager.h"
#include "cluster/partition_recovery_manager.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "redpanda/application.h"
#include "resource_mgmt/scheduling_groups_probe.h"
#include "syschecks/syschecks.h"

#include <seastar/core/smp.hh>

void application::wire_up_services_cloud_io(
  ::stop_signal& app_signal,
  std::optional<cloud_storage_clients::bucket_name>& bucket_name) {
    if (!requires_cloud_io()) {
        return;
    }
    syschecks::systemd_message("Starting cloud IO").get();
    auto cloud_config = cloud_storage::configuration::get_config().get();
    construct_service(upstreams, cloud_config.client_config).get();
    upstreams.invoke_on_all(&cloud_storage_clients::upstream_registry::start)
      .get();
    upstreams
      .invoke_on_all(
        &cloud_storage_clients::upstream_registry::start_evictor,
        /*interval=*/30s,
        /*max_idle_time=*/300s)
      .get();
    construct_service(
      cloud_storage_clients,
      ss::sharded_parameter([this] { return std::ref(upstreams.local()); }),
      cloud_config.connection_limit,
      cloud_config.client_config,
      cloud_storage_clients::client_pool_overdraft_policy::borrow_if_empty)
      .get();
    cloud_storage_clients
      .invoke_on_all(
        &cloud_storage_clients::client_pool::start,
        ss::sharded_parameter(
          [&app_signal]()
            -> std::optional<std::reference_wrapper<::stop_signal>> {
              if (
                ss::this_shard_id()
                == cloud_storage_clients::self_config_shard) {
                  return std::ref(app_signal);
              }

              return std::nullopt;
          }))
      .get();
    construct_service(
      cloud_io,
      std::ref(cloud_storage_clients),
      cloud_config.client_config,
      cloud_config.cloud_credentials_source,
      ss::sharded_parameter(
        [] { return scheduling_groups::instance().ts_read_sg(); }))
      .get();
    cloud_io.invoke_on_all(&cloud_io::remote::start).get();
    bucket_name = cloud_config.bucket_name;

    if (archival_storage_enabled()) {
        syschecks::systemd_message("Starting cloud storage api").get();
        construct_service(cloud_storage_api, std::ref(cloud_io), cloud_config)
          .get();
        cloud_storage_api.invoke_on_all(&cloud_storage::remote::start).get();

        construct_service(
          partition_recovery_manager,
          cloud_config.bucket_name,
          std::ref(cloud_storage_api))
          .get();

        construct_service(
          archival_upload_housekeeping,
          std::ref(cloud_storage_api),
          ss::sharded_parameter(
            [sg = scheduling_groups::instance().archival_upload()] {
                return sg;
            }))
          .get();
        archival_upload_housekeeping
          .invoke_on_all(&archival::upload_housekeeping_service::start)
          .get();

        construct_service(
          offsets_uploader,
          cloud_config.bucket_name,
          std::ref(_group_manager),
          std::ref(cloud_storage_api))
          .get();
    }
}

void application::wire_up_services_cloud_archival(model::node_id node_id) {
    if (archival_storage_enabled() && !config::node().recovery_mode_enabled()) {
        construct_service(
          offsets_upload_router,
          std::ref(offsets_uploader),
          std::ref(shard_table),
          std::ref(metadata_cache),
          std::ref(_connection_cache),
          std::ref(controller->get_partition_leaders()),
          node_id)
          .get();

        construct_service(
          _archival_purger,
          ss::sharded_parameter(
            [&api = cloud_storage_api]() { return std::ref(api.local()); }),
          ss::sharded_parameter([&t = controller->get_topics_state()]() {
              return std::ref(t.local());
          }),
          std::ref(controller->get_topics_frontend()),
          std::ref(controller->get_members_table()))
          .get();

        _archival_purger
          .invoke_on_all(
            [&housekeeping = archival_upload_housekeeping](
              archival::purger& s) { housekeeping.local().register_jobs({s}); })
          .get();

        _deferred.emplace_back([this] {
            _archival_purger
              .invoke_on_all([&housekeeping = archival_upload_housekeeping,
                              this](archival::purger& s) {
                  vlog(_log.debug, "Deregistering purger housekeeping jobs");
                  housekeeping.local().deregister_jobs({s});
              })
              .get();
        });
    }

    vlog(
      _log.info,
      "Archiver service setup, cloud_storage_enabled: {}, "
      "legacy_upload_mode_enabled: {}",
      archival_storage_enabled(),
      config::shard_local_cfg().cloud_storage_disable_archiver_manager.value());
    if (
      archival_storage_enabled()
      && !config::shard_local_cfg()
            .cloud_storage_disable_archiver_manager.value()) {
        construct_service(
          archiver_manager,
          node_id,
          std::ref(partition_manager),
          std::ref(raft_group_manager),
          std::ref(cloud_storage_api),
          std::ref(shadow_index_cache),
          std::ref(archival_upload_housekeeping),
          ss::sharded_parameter(
            [sg = scheduling_groups::instance().archival_upload(),
             enabled = archival_storage_enabled()]()
              -> ss::lw_shared_ptr<const archival::configuration> {
                if (enabled) {
                    return ss::make_lw_shared<const archival::configuration>(
                      archival::get_archival_service_config(sg));
                } else {
                    return nullptr;
                }
            }))
          .get();
    }
}
