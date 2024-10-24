/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include <seastar/core/shared_ptr_incomplete.hh>

namespace cluster {

class cluster_recovery_manager;
class cluster_recovery_table;
class controller;
class controller_backend;
class controller_stm;
class controller_stm_shard;
class id_allocator_frontend;
class rm_partition_frontend;
class log_eviction_stm;
class tx_coordinator_mapper;
class tx_gateway_frontend;
class partition_leaders_table;
class partition_allocator;
class partition_manager;
class partition;
class partition_probe;
class shard_table;
class topics_frontend;
class topic_table;
class plugin_frontend;
class plugin_table;
class plugin_backend;
class topic_table_partition_generator;
class cloud_storage_size_reducer;
class members_manager;
class members_table;
class metadata_cache;
class metadata_dissemination_service;
class security_frontend;
class controller_api;
class members_frontend;
class config_frontend;
class config_manager;
class members_backend;
class tx_gateway;
class rm_group_proxy;
class health_manager;
class health_monitor_frontend;
class health_monitor_backend;
class metrics_reporter;
class feature_frontend;
class feature_backend;
class feature_manager;
class drain_manager;
class partition_balancer_backend;
class partition_balancer_state;
class node_status_backend;
class node_status_table;
class ephemeral_credential_frontend;
class self_test_backend;
class self_test_frontend;
class topic_recovery_status_frontend;
class node_isolation_watcher;
struct controller_snapshot;
struct controller_join_snapshot;
class tx_manager_migrator;
struct state_machine_factory;
class state_machine_registry;
class tx_topic_manager;
class shard_placement_table;
class shard_balancer;
class id_allocator_stm;
class tm_stm;
class rm_stm;
namespace data_migrations {
class migrated_resources;
class migration_frontend;
class worker;
class backend;
class migrations_table;
class frontend;
class irpc_frontend;
} // namespace data_migrations

namespace tx {
class producer_state_manager;
class producer_state;
class request;
struct producer_state_snapshot;
struct producer_partition_transaction_state;
} // namespace tx

namespace node {
class local_monitor;
} // namespace node

namespace cloud_metadata {
class cluster_recovery_backend;
class offsets_lookup;
class offsets_lookup_batcher;
class offsets_recoverer;
class offsets_recovery_manager;
class offsets_recovery_requestor;
class offsets_recovery_router;
class offsets_upload_requestor;
class offsets_upload_router;
class offsets_uploader;
class producer_id_recovery_manager;
class uploader;
} // namespace cloud_metadata

namespace client_quota {
class frontend;
class backend;
class store;
}; // namespace client_quota

} // namespace cluster

namespace seastar {

template<>
struct lw_shared_ptr_deleter<cluster::partition> {
    static void dispose(cluster::partition* sst);
};

} // namespace seastar
