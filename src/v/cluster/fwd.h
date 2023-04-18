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

namespace cluster {

class controller;
class controller_backend;
class controller_stm;
class controller_stm_shard;
class id_allocator_frontend;
class rm_partition_frontend;
class tm_stm_cache;
class tm_stm_cache_manager;
class tx_gateway_frontend;
class partition_leaders_table;
class partition_allocator;
class partition_manager;
class shard_table;
class topics_frontend;
class topic_table;
struct topic_table_delta;
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
class non_replicable_topics_frontend;
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

namespace node {
class local_monitor;
} // namespace node

} // namespace cluster
