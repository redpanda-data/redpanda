// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_storage/topic_mount_handler.h"
#include "cluster/bootstrap_backend.h"
#include "cluster/client_quota_backend.h"
#include "cluster/client_quota_frontend.h"
#include "cluster/client_quota_store.h"
#include "cluster/cloud_metadata/cluster_recovery_backend.h"
#include "cluster/cloud_metadata/uploader.h"
#include "cluster/cluster_link/frontend.h"
#include "cluster/cluster_link/table.h"
#include "cluster/cluster_recovery_manager.h"
#include "cluster/cluster_recovery_table.h"
#include "cluster/config_frontend.h"
#include "cluster/config_manager.h"
#include "cluster/controller.h"
#include "cluster/controller_api.h"
#include "cluster/controller_backend.h"
#include "cluster/controller_forced_reconfiguration_manager.h"
#include "cluster/controller_stm.h"
#include "cluster/crash_reporter.h"
#include "cluster/data_migrated_resources.h"
#include "cluster/data_migration_backend.h"
#include "cluster/data_migration_frontend.h"
#include "cluster/data_migration_group_proxy.h"
#include "cluster/data_migration_irpc_frontend.h"
#include "cluster/data_migration_router.h"
#include "cluster/data_migration_table.h"
#include "cluster/data_migration_worker.h"
#include "cluster/drain_manager.h"
#include "cluster/ephemeral_credential_frontend.h"
#include "cluster/feature_backend.h"
#include "cluster/feature_manager.h"
#include "cluster/health_manager.h"
#include "cluster/health_monitor_backend.h"
#include "cluster/health_monitor_frontend.h"
#include "cluster/logger.h"
#include "cluster/members_backend.h"
#include "cluster/members_frontend.h"
#include "cluster/members_manager.h"
#include "cluster/members_table.h"
#include "cluster/metrics_reporter.h"
#include "cluster/partition_balancer_backend.h"
#include "cluster/partition_balancer_state.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/plugin_backend.h"
#include "cluster/plugin_frontend.h"
#include "cluster/plugin_table.h"
#include "cluster/scheduling/leader_balancer.h"
#include "cluster/scheduling/partition_allocator.h"
#include "cluster/security_frontend.h"
#include "cluster/shard_balancer.h"
#include "cluster/shard_placement_table.h"
#include "cluster/topic_metrics_watcher.h"
#include "cluster/topic_table.h"
#include "cluster/topics_frontend.h"
#include "crash_reporter.h"
#include "security/authorizer.h"
#include "security/credential_store.h"
#include "security/ephemeral_credential_store.h"
#include "security/oidc_service.h"
#include "security/role_store.h"

#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>

namespace cluster {

ss::future<> controller::shutdown_input() {
    vlog(clusterlog.debug, "Shutting down controller inputs");
    if (_raft0) {
        _raft0->shutdown_input();
    }
    if (_metadata_uploader) {
        _metadata_uploader->stop();
    }

    co_await ss::smp::submit_to(controller_stm_shard, [&stm = _stm] {
        if (stm.local_is_initialized()) {
            stm.local().shutdown_apply_loop();
        }
    });

    co_await _as.invoke_on_all(&ss::abort_source::request_abort);
    vlog(clusterlog.debug, "Shut down controller inputs");
}

ss::future<> controller::stop() {
    _probe.stop();

    if (!_as.local().abort_requested()) {
        co_await shutdown_input();
    }

    co_await _cfr_m->stop();

    co_await ss::smp::submit_to(controller_stm_shard, [&stm = _stm] {
        return stm.local_is_initialized() ? stm.local().shutdown() : ss::now();
    });

    if (_leader_balancer) {
        co_await _leader_balancer->stop();
    }

    if (_metadata_uploader) {
        co_await _metadata_uploader->stop_and_wait();
    }
    co_await _data_migration_irpc_frontend.stop();
    co_await _data_migration_backend.stop();
    if (_recovery_backend) {
        co_await _recovery_backend->stop_and_wait();
    }
    co_await _topic_metrics_watcher.stop();
    co_await _recovery_manager.stop();
    co_await _recovery_table.stop();
    co_await _partition_balancer.stop();
    co_await _crash_reporter.stop();
    co_await _metrics_reporter.stop();
    co_await _feature_manager.stop();
    co_await _hm_frontend.stop();
    co_await _hm_backend.stop();
    co_await _health_manager.stop();
    co_await _members_backend.stop();
    co_await _data_migration_router.stop();
    co_await _data_migration_worker.stop();
    co_await _data_migration_frontend.stop();
    co_await _topic_mount_handler.stop();
    co_await _config_manager.stop();
    co_await _api.stop();
    co_await _shard_balancer.stop();
    co_await _backend.stop();
    co_await _tp_frontend.stop();
    co_await _plugin_frontend.stop();
    co_await _cluster_link_frontend.stop();
    co_await _quota_frontend.stop();
    co_await _ephemeral_credential_frontend.stop();
    co_await _security_frontend.stop();
    co_await _members_frontend.stop();
    co_await _config_frontend.stop();
    co_await _feature_backend.stop();
    co_await _bootstrap_backend.stop();
    co_await _oidc_service.stop();
    co_await _authorizer.stop();
    co_await _ephemeral_credentials.stop();
    co_await _data_migration_table.stop();
    co_await _data_migrated_resources.stop();
    co_await _roles.stop();
    co_await _credentials.stop();
    co_await _tp_state.stop();
    co_await _members_manager.stop();
    co_await _epoch_service.stop();
    co_await _stm.stop();
    co_await _cluster_link_table.stop();
    co_await _quota_backend.stop();
    co_await _quota_store.stop();
    co_await _plugin_backend.stop();
    co_await _plugin_table.stop();
    co_await _drain_manager.stop();
    co_await _shard_placement.stop();
    co_await _partition_balancer_state.stop();
    co_await _partition_allocator.stop();
    co_await _partition_leaders.stop();
    co_await _members_table.stop();
    co_await _gate.close();
    co_await _as.stop();
}

} // namespace cluster
