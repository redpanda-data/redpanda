// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/vlog.h"
#include "cloud_topics/app.h"
#include "cloud_topics/level_one/metastore/lsm/stm.h"
#include "cloud_topics/level_one/metastore/simple_stm.h"
#include "cloud_topics/level_zero/stm/ctp_stm_factory.h"
#include "cloud_topics/read_replica/stm.h"
#include "cluster/archival/archival_metadata_stm.h"
#include "cluster/archival/archiver_manager.h"
#include "cluster/archival/upload_controller.h"
#include "cluster/cloud_metadata/offsets_recovery_manager.h"
#include "cluster/cloud_metadata/offsets_upload_router.h"
#include "cluster/cluster_discovery.h"
#include "cluster/controller.h"
#include "cluster/feature_manager.h"
#include "cluster/id_allocator_stm.h"
#include "cluster/log_eviction_stm.h"
#include "cluster/members_manager.h"
#include "cluster/metadata_dissemination_service.h"
#include "cluster/node_isolation_watcher.h"
#include "cluster/partition_manager.h"
#include "cluster/partition_properties_stm.h"
#include "cluster/rm_stm.h"
#include "cluster/tm_stm.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "datalake/coordinator/coordinator_manager.h"
#include "datalake/coordinator/state_machine.h"
#include "datalake/translation/state_machine.h"
#include "debug_bundle/debug_bundle_service.h"
#include "kafka/server/group_manager.h"
#include "kafka/server/group_tx_tracker_stm.h"
#include "kafka/server/quota_manager.h"
#include "kafka/server/snc_quota_manager.h"
#include "kafka/server/usage_manager.h"
#include "kafka/server/write_at_offset_stm.h"
#include "migrations/migrators.h"
#include "raft/group_manager.h"
#include "raft/service.h"
#include "redpanda/admin/kafka_connections_service.h"
#include "redpanda/admin/server.h"
#include "redpanda/application.h"
#include "resource_mgmt/scheduling_groups_probe.h"
#include "storage/compaction_controller.h"
#include "syschecks/syschecks.h"
#include "transform/stm/transform_offsets_stm.h"

#include <seastar/core/condition-variable.hh>

void application::start_runtime_services(
  cluster::cluster_discovery& cd,
  ::stop_signal& app_signal,
  cloud_topics::test_fixture_cfg ct_test_cfg) {
    // single instance
    node_status_backend.invoke_on_all(&cluster::node_status_backend::start)
      .get();
    syschecks::systemd_message("Starting the partition manager").get();
    partition_manager
      .invoke_on_all([this, ct_test_cfg](cluster::partition_manager& pm) {
          pm.register_factory<cluster::tm_stm_factory>();
          pm.register_factory<cluster::id_allocator_stm_factory>();
          pm.register_factory<transform::transform_offsets_stm_factory>();
          pm.register_factory<cluster::rm_stm_factory>(
            config::shard_local_cfg().enable_transactions.value(),
            config::shard_local_cfg().enable_idempotence.value(),
            tx_gateway_frontend,
            producer_manager,
            feature_table);
          pm.register_factory<cluster::log_eviction_stm_factory>(
            storage.local().kvs());
          pm.register_factory<cluster::archival_metadata_stm_factory>(
            config::shard_local_cfg().cloud_storage_enabled(),
            cloud_storage_api,
            feature_table);
          pm.register_factory<kafka::group_tx_tracker_stm_factory>(
            feature_table);
          pm.register_factory<cluster::partition_properties_stm_factory>(
            storage.local().kvs(),
            config::shard_local_cfg().internal_rpc_request_timeout_ms.bind());
          pm.register_factory<datalake::coordinator::stm_factory>();
          pm.register_factory<datalake::translation::stm_factory>(
            config::shard_local_cfg().iceberg_enabled());
          if (config::shard_local_cfg().cloud_topics_enabled()) {
              pm.register_factory<cloud_topics::l0::ctp_stm_factory>();
              pm.register_factory<cloud_topics::read_replica::stm_factory>();
              if (ct_test_cfg.use_lsm_metastore) {
                  pm.register_factory<cloud_topics::l1::lsm_stm_factory>();
              } else {
                  pm.register_factory<cloud_topics::l1::stm_factory>();
              }
          }
          pm.register_factory<kafka::write_at_offset_stm_factory>(
            storage.local().kvs(), model::offset_translator_batch_types());
      })
      .get();
    partition_manager.invoke_on_all(&cluster::partition_manager::start).get();

    syschecks::systemd_message("Starting Raft group manager").get();
    raft_group_manager.invoke_on_all(&raft::group_manager::start).get();

    syschecks::systemd_message("Starting Kafka group manager").get();
    _group_manager.invoke_on_all(&kafka::group_manager::start).get();

    // Initialize the Raft RPC endpoint before the rest of the runtime RPC
    // services so the cluster seeds can elect a leader and write a cluster
    // UUID before proceeding with the rest of bootstrap.
    const bool start_raft_rpc_early = cd.is_cluster_founder().get();
    if (start_raft_rpc_early) {
        syschecks::systemd_message("Starting RPC/raft").get();
        _rpc
          .invoke_on_all([this](rpc::rpc_server& s) {
              std::vector<std::unique_ptr<rpc::service>> runtime_services;
              runtime_services.push_back(
                std::make_unique<raft::service<
                  cluster::partition_manager,
                  cluster::shard_table>>(
                  scheduling_groups::instance().raft_recv_sg(),
                  smp_service_groups.raft_smp_sg(),
                  scheduling_groups::instance().raft_heartbeats(),
                  partition_manager,
                  shard_table.local(),
                  config::shard_local_cfg().raft_heartbeat_interval_ms(),
                  config::node().node_id().value()));
              s.add_services(std::move(runtime_services));
          })
          .get();
    }
    syschecks::systemd_message("Starting controller").get();
    ss::shared_ptr<cluster::cloud_metadata::offsets_upload_requestor>
      offsets_upload_requestor;
    if (offsets_upload_router.local_is_initialized()) {
        offsets_upload_requestor = offsets_upload_router.local_shared();
    }
    ss::shared_ptr<cluster::cloud_metadata::offsets_recovery_requestor>
      offsets_recovery_requestor;
    if (offsets_recovery_router.local_is_initialized()) {
        offsets_recovery_requestor = offsets_recovery_manager;
    }
    if (_datalake_coordinator_mgr.local_is_initialized()) {
        // Before starting the controller, start the coordinator manager so we
        // don't miss any partition/leadership notifications.
        _datalake_coordinator_mgr
          .invoke_on_all(&datalake::coordinator::coordinator_manager::start)
          .get();
    }
    controller
      ->start(
        cd,
        app_signal.abort_source(),
        std::move(offsets_upload_requestor),
        producer_id_recovery_manager,
        std::move(offsets_recovery_requestor),
        redpanda_start_time,
        _data_migrations_group_proxy,
        cloud_topics_app ? cloud_topics_app->get_state() : nullptr)
      .get();

    if (archiver_manager.local_is_initialized()) {
        archiver_manager.invoke_on_all(&archival::archiver_manager::start)
          .get();
    }

    // FIXME: in first patch explain why this is started after the
    // controller so the broker set will be available. Then next patch fix.
    syschecks::systemd_message("Starting metadata dissination service").get();
    md_dissemination_service
      .invoke_on_all(&cluster::metadata_dissemination_service::start)
      .get();

    syschecks::systemd_message("Starting RPC").get();
    _rpc
      .invoke_on_all([this, start_raft_rpc_early](rpc::rpc_server& s) {
          add_runtime_rpc_services(s, start_raft_rpc_early);
      })
      .get();

    syschecks::systemd_message("Starting node isolation watcher").get();
    _node_isolation_watcher->start();

    // After we have started internal RPC listener, we may join
    // the cluster (if we aren't already a member)
    controller->get_members_manager()
      .invoke_on(
        cluster::members_manager::shard,
        &cluster::members_manager::join_cluster)
      .get();

    quota_mgr.invoke_on_all(&kafka::quota_manager::start).get();
    snc_quota_mgr.invoke_on_all(&kafka::snc_quota_manager::start).get();
    usage_manager.invoke_on_all(&kafka::usage_manager::start).get();

    if (_await_controller_last_applied.has_value()) {
        syschecks::systemd_message(
          "Waiting for controller to replicate (joining cluster)")
          .get();
        controller
          ->wait_for_offset(
            _await_controller_last_applied.value(), app_signal.abort_source())
          .get();
    }

    // Verify the enterprise license when trying to upgrade Redpanda.
    // By this point during startup we have enough information to evaluate both
    // the state of the license and what enterprise features are used.
    // - If redpanda has been restarted on an existing node, we have already
    //   loaded the feature table from the local snapshot in
    //   application::load_feature_table_snapshot and replayed the local
    //   controller log in controller::start.
    // - If this is a new node joining an existing cluster, by this point we
    //   have received a controller snapshot from another node in the join
    //   response and have replicated and replayed the the controller stm to the
    //   last_applied offset received in the join_node_reply above.
    controller->get_feature_manager()
      .invoke_on(
        cluster::feature_manager::backend_shard,
        [](cluster::feature_manager& fm) {
            return fm.verify_enterprise_license();
        })
      .get();

    if (cloud_topics_app) {
        cloud_topics_app->start().get();
    }

    _debug_bundle_service.invoke_on_all(&debug_bundle::service::start).get();

    if (!config::node().admin().empty()) {
        _kafka_connections_service
          .invoke_on_all([this](admin::kafka_connections_service& kcs) {
              return kcs.start(_admin.local().memory_semaphore());
          })
          .get();

        _admin.invoke_on_all(&admin_server::start).get();
    }

    _compaction_controller.invoke_on_all(&storage::compaction_controller::start)
      .get();
    _archival_upload_controller
      .invoke_on_all(&archival::upload_controller::start)
      .get();

    for (const auto& m : _migrators) {
        m->start(controller->get_abort_source().local());
    }

    space_manager->start().get();
}

/**
 * The Kafka protocol listener startup is separate to the rest of Redpanda,
 * because it includes a wait for this node to be a full member of a redpanda
 * cluster -- this is expected to be run last, after everything else is
 * started.
 */
void application::start_kafka(
  const model::node_id& node_id, ::stop_signal& app_signal) {
    // Kafka API
    // The Kafka listener is intentionally the last thing we start: during
    // this phase we will wait for the node to be a cluster member before
    // proceeding, because it is not helpful to clients for us to serve
    // kafka requests before we have up to date knowledge of the system.
    vlog(_log.info, "Waiting for cluster membership");
    controller->get_members_table()
      .local()
      .await_membership(node_id, app_signal.abort_source())
      .get();

    // Before starting the Kafka API, wait for the cluster ID to be initialized,
    // because Kafka clients interpret a changing cluster ID as a client
    // connecting to multiple clusters and print warnings.
    if (
      !config::shard_local_cfg().cluster_id().has_value()
      && feature_table.local().get_original_version()
           >= cluster::cluster_version{10}) {
        // This check only applies for clusters created with Redpanda >=23.2,
        // because this is the version that has fast initialization of
        // cluster_id, whereas older versions may wait several minutes to
        // initialize it, causing issues during fast upgrades where the previous
        // version may have run too briefly to have initialized cluster_id
        vlog(_log.info, "Waiting for Cluster ID to initialize...");
        ss::condition_variable cvar;
        auto binding = config::shard_local_cfg().cluster_id.bind();
        binding.watch([&cvar] { cvar.signal(); });
        cvar.wait().get();
    }

    _kafka_server.start().get();
    vlog(
      _log.info,
      "Started Kafka API server listening at {}",
      config::node().kafka_api());
}
