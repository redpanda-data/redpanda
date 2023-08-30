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
#include "config/bounded_property.h"
#include "config/broker_endpoint.h"
#include "config/client_group_byte_rate_quota.h"
#include "config/config_store.h"
#include "config/convert.h"
#include "config/data_directory_path.h"
#include "config/endpoint_tls_config.h"
#include "config/property.h"
#include "config/throughput_control_group.h"
#include "config/tls_config.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"
#include "net/unresolved_address.h"
#include "pandaproxy/schema_registry/schema_id_validation.h"

#include <seastar/core/sstring.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/ip.hh>
#include <seastar/net/socket_defs.hh>

#include <cctype>
#include <chrono>
#include <vector>

namespace config {

/// Redpanda configuration
///
/// All application modules depend on configuration. The configuration module
/// can not depend on any other module to prevent cyclic dependencies.

struct configuration final : public config_store {
    // WAL
    bounded_property<uint64_t> log_segment_size;
    property<std::optional<uint64_t>> log_segment_size_min;
    property<std::optional<uint64_t>> log_segment_size_max;
    bounded_property<uint16_t> log_segment_size_jitter_percent;
    bounded_property<uint64_t> compacted_log_segment_size;
    property<std::chrono::milliseconds> readers_cache_eviction_timeout_ms;
    bounded_property<std::optional<std::chrono::milliseconds>> log_segment_ms;
    property<std::chrono::milliseconds> log_segment_ms_min;
    property<std::chrono::milliseconds> log_segment_ms_max;

    // Network
    bounded_property<std::optional<int>> rpc_server_listen_backlog;
    bounded_property<std::optional<int>> rpc_server_tcp_recv_buf;
    bounded_property<std::optional<int>> rpc_server_tcp_send_buf;
    // Coproc
    deprecated_property enable_coproc;
    deprecated_property coproc_max_inflight_bytes;
    deprecated_property coproc_max_ingest_bytes;
    deprecated_property coproc_max_batch_size;
    deprecated_property coproc_offset_flush_interval_ms;

    // Controller
    bounded_property<std::optional<std::size_t>> topic_memory_per_partition;
    bounded_property<std::optional<int32_t>> topic_fds_per_partition;
    bounded_property<uint32_t> topic_partitions_per_shard;
    bounded_property<uint32_t> topic_partitions_reserve_shard0;

    // Admin API
    property<bool> admin_api_require_auth;

    // Raft
    deprecated_property seed_server_meta_topic_partitions;
    bounded_property<std::chrono::milliseconds> raft_heartbeat_interval_ms;
    bounded_property<std::chrono::milliseconds> raft_heartbeat_timeout_ms;
    property<size_t> raft_heartbeat_disconnect_failures;
    deprecated_property min_version;
    deprecated_property max_version;
    bounded_property<std::optional<size_t>> raft_max_recovery_memory;
    bounded_property<size_t> raft_recovery_default_read_size;
    property<bool> raft_enable_lw_heartbeat;
    // Kafka
    property<bool> enable_usage;
    bounded_property<size_t> usage_num_windows;
    bounded_property<std::chrono::seconds> usage_window_width_interval_sec;
    bounded_property<std::chrono::seconds> usage_disk_persistance_interval_sec;
    deprecated_property use_scheduling_groups;
    deprecated_property enable_admin_api;
    bounded_property<int16_t> default_num_windows;
    bounded_property<std::chrono::milliseconds> default_window_sec;
    property<std::chrono::milliseconds> quota_manager_gc_sec;
    bounded_property<uint32_t> target_quota_byte_rate;
    property<std::optional<uint32_t>> target_fetch_quota_byte_rate;
    bounded_property<std::optional<uint32_t>> kafka_admin_topic_api_rate;
    property<std::optional<ss::sstring>> cluster_id;
    property<bool> disable_metrics;
    property<bool> disable_public_metrics;
    property<bool> aggregate_metrics;
    property<std::chrono::milliseconds> group_min_session_timeout_ms;
    property<std::chrono::milliseconds> group_max_session_timeout_ms;
    property<std::chrono::milliseconds> group_initial_rebalance_delay;
    property<std::chrono::milliseconds> group_new_member_join_timeout;
    property<std::optional<std::chrono::seconds>> group_offset_retention_sec;
    property<std::chrono::milliseconds> group_offset_retention_check_ms;
    property<bool> legacy_group_offset_retention_enabled;
    property<std::chrono::milliseconds> metadata_dissemination_interval_ms;
    property<std::chrono::milliseconds> metadata_dissemination_retry_delay_ms;
    property<int16_t> metadata_dissemination_retries;
    property<std::chrono::milliseconds> tm_sync_timeout_ms;
    deprecated_property tm_violation_recovery_policy;
    property<std::chrono::milliseconds> rm_sync_timeout_ms;
    property<std::chrono::milliseconds> find_coordinator_timeout_ms;
    deprecated_property seq_table_min_size;
    property<std::chrono::milliseconds> tx_timeout_delay_ms;
    deprecated_property rm_violation_recovery_policy;
    property<std::chrono::milliseconds> fetch_reads_debounce_timeout;
    property<std::chrono::milliseconds> alter_topic_cfg_timeout_ms;
    property<model::cleanup_policy_bitflags> log_cleanup_policy;
    enum_property<model::timestamp_type> log_message_timestamp_type;
    bounded_property<std::optional<std::chrono::milliseconds>>
      log_message_timestamp_alert_before_ms;
    bounded_property<std::chrono::milliseconds>
      log_message_timestamp_alert_after_ms;
    enum_property<model::compression> log_compression_type;
    property<size_t> fetch_max_bytes;
    property<bool> use_fetch_scheduler_group;
    property<std::chrono::milliseconds> metadata_status_wait_timeout_ms;
    property<std::chrono::seconds> kafka_tcp_keepalive_idle_timeout_seconds;
    property<std::chrono::seconds> kafka_tcp_keepalive_probe_interval_seconds;
    property<uint32_t> kafka_tcp_keepalive_probes;
    bounded_property<std::optional<int64_t>> kafka_connection_rate_limit;
    property<std::vector<ss::sstring>> kafka_connection_rate_limit_overrides;
    // same as transactional.id.expiration.ms in kafka
    property<std::chrono::milliseconds> transactional_id_expiration_ms;
    bounded_property<uint64_t> max_concurrent_producer_ids;
    bounded_property<uint64_t> max_transactions_per_coordinator;
    property<bool> enable_idempotence;
    property<bool> enable_transactions;
    property<uint32_t> abort_index_segment_size;
    // same as log.retention.ms in kafka
    retention_duration_property delete_retention_ms;
    property<std::chrono::milliseconds> log_compaction_interval_ms;
    property<bool> log_disable_housekeeping_for_tests;
    // same as retention.size in kafka - TODO: size not implemented
    property<std::optional<size_t>> retention_bytes;
    property<int32_t> group_topic_partitions;
    bounded_property<int16_t> default_topic_replication;
    deprecated_property transaction_coordinator_replication;
    deprecated_property id_allocator_replication;
    property<int32_t> transaction_coordinator_partitions;
    property<model::cleanup_policy_bitflags>
      transaction_coordinator_cleanup_policy;
    property<std::chrono::milliseconds>
      transaction_coordinator_delete_retention_ms;
    property<uint64_t> transaction_coordinator_log_segment_size;
    property<std::chrono::milliseconds>
      abort_timed_out_transactions_interval_ms;
    property<std::chrono::seconds> tx_log_stats_interval_s;
    property<std::chrono::milliseconds> create_topic_timeout_ms;
    property<std::chrono::milliseconds> wait_for_leader_timeout_ms;
    property<int32_t> default_topic_partitions;
    property<bool> disable_batch_cache;
    property<std::chrono::milliseconds> raft_election_timeout_ms;
    property<std::chrono::milliseconds> kafka_group_recovery_timeout_ms;
    property<std::chrono::milliseconds> replicate_append_timeout_ms;
    property<std::chrono::milliseconds> recovery_append_timeout_ms;
    property<size_t> raft_replicate_batch_window_size;
    property<size_t> raft_learner_recovery_rate;
    property<bool> raft_recovery_throttle_disable_dynamic_mode;
    property<std::optional<uint32_t>> raft_smp_max_non_local_requests;
    property<uint32_t> raft_max_concurrent_append_requests_per_follower;

    property<size_t> reclaim_min_size;
    property<size_t> reclaim_max_size;
    property<std::chrono::milliseconds> reclaim_growth_window;
    property<std::chrono::milliseconds> reclaim_stable_window;
    property<size_t> reclaim_batch_cache_min_free;
    property<bool> auto_create_topics_enabled;
    property<bool> enable_pid_file;
    property<std::chrono::milliseconds> kvstore_flush_interval;
    property<size_t> kvstore_max_segment_size;
    property<std::chrono::milliseconds> max_kafka_throttle_delay_ms;
    property<size_t> kafka_max_bytes_per_fetch;
    property<std::chrono::milliseconds> raft_io_timeout_ms;
    property<std::chrono::milliseconds> join_retry_timeout_ms;
    property<std::chrono::milliseconds> raft_timeout_now_timeout_ms;
    property<std::chrono::milliseconds>
      raft_transfer_leader_recovery_timeout_ms;
    property<bool> release_cache_on_segment_roll;
    property<std::chrono::milliseconds> segment_appender_flush_timeout_ms;
    property<std::chrono::milliseconds> fetch_session_eviction_timeout_ms;
    bounded_property<size_t> append_chunk_size;
    property<size_t> storage_read_buffer_size;
    property<int16_t> storage_read_readahead_count;
    property<size_t> segment_fallocation_step;
    bounded_property<uint64_t> storage_target_replay_bytes;
    bounded_property<uint64_t> storage_max_concurrent_replay;
    bounded_property<uint64_t> storage_compaction_index_memory;
    property<size_t> max_compacted_log_segment_size;
    property<std::optional<std::chrono::seconds>>
      storage_ignore_timestamps_in_future_sec;
    property<bool> storage_ignore_cstore_hints;
    bounded_property<int16_t> storage_reserve_min_segments;

    property<int16_t> id_allocator_log_capacity;
    property<int16_t> id_allocator_batch_size;
    property<bool> enable_sasl;
    property<std::vector<ss::sstring>> sasl_mechanisms;
    property<ss::sstring> sasl_kerberos_config;
    property<ss::sstring> sasl_kerberos_keytab;
    property<ss::sstring> sasl_kerberos_principal;
    property<std::vector<ss::sstring>> sasl_kerberos_principal_mapping;
    property<std::optional<bool>> kafka_enable_authorization;
    property<std::optional<std::vector<ss::sstring>>>
      kafka_mtls_principal_mapping_rules;
    property<bool> kafka_enable_partition_reassignment;
    property<std::chrono::milliseconds>
      controller_backend_housekeeping_interval_ms;
    property<std::chrono::milliseconds> node_management_operation_timeout_ms;
    property<uint32_t> kafka_request_max_bytes;
    property<uint32_t> kafka_batch_max_bytes;
    property<std::vector<ss::sstring>> kafka_nodelete_topics;
    property<std::vector<ss::sstring>> kafka_noproduce_topics;

    // Compaction controller
    property<std::chrono::milliseconds> compaction_ctrl_update_interval_ms;
    property<double> compaction_ctrl_p_coeff;
    property<double> compaction_ctrl_i_coeff;
    property<double> compaction_ctrl_d_coeff;
    property<int16_t> compaction_ctrl_min_shares;
    property<int16_t> compaction_ctrl_max_shares;
    property<std::optional<size_t>> compaction_ctrl_backlog_size;
    property<std::chrono::milliseconds> members_backend_retry_ms;
    property<std::optional<uint32_t>> kafka_connections_max;
    property<std::optional<uint32_t>> kafka_connections_max_per_ip;
    property<std::vector<ss::sstring>> kafka_connections_max_overrides;
    one_or_many_map_property<client_group_quota>
      kafka_client_group_byte_rate_quota;
    one_or_many_map_property<client_group_quota>
      kafka_client_group_fetch_byte_rate_quota;
    bounded_property<std::optional<int>> kafka_rpc_server_tcp_recv_buf;
    bounded_property<std::optional<int>> kafka_rpc_server_tcp_send_buf;
    bounded_property<std::optional<size_t>> kafka_rpc_server_stream_recv_buf;
    property<bool> kafka_enable_describe_log_dirs_remote_storage;

    // Archival storage
    property<bool> cloud_storage_enabled;
    property<bool> cloud_storage_enable_remote_read;
    property<bool> cloud_storage_enable_remote_write;
    property<std::optional<ss::sstring>> cloud_storage_access_key;
    property<std::optional<ss::sstring>> cloud_storage_secret_key;
    property<std::optional<ss::sstring>> cloud_storage_region;
    property<std::optional<ss::sstring>> cloud_storage_bucket;
    property<std::optional<ss::sstring>> cloud_storage_api_endpoint;
    enum_property<model::cloud_credentials_source>
      cloud_storage_credentials_source;
    property<std::chrono::milliseconds>
      cloud_storage_roles_operation_timeout_ms;
    deprecated_property cloud_storage_reconciliation_ms;
    property<std::chrono::milliseconds>
      cloud_storage_upload_loop_initial_backoff_ms;
    property<std::chrono::milliseconds>
      cloud_storage_upload_loop_max_backoff_ms;
    property<int16_t> cloud_storage_max_connections;
    property<bool> cloud_storage_disable_tls;
    property<int16_t> cloud_storage_api_endpoint_port;
    property<std::optional<ss::sstring>> cloud_storage_trust_file;
    property<std::chrono::milliseconds> cloud_storage_initial_backoff_ms;
    property<std::chrono::milliseconds> cloud_storage_segment_upload_timeout_ms;
    property<std::chrono::milliseconds>
      cloud_storage_manifest_upload_timeout_ms;
    property<std::chrono::milliseconds>
      cloud_storage_max_connection_idle_time_ms;
    property<std::optional<std::chrono::seconds>>
      cloud_storage_segment_max_upload_interval_sec;
    property<std::optional<std::chrono::seconds>>
      cloud_storage_manifest_max_upload_interval_sec;
    property<std::chrono::milliseconds>
      cloud_storage_readreplica_manifest_sync_timeout_ms;
    property<std::chrono::milliseconds> cloud_storage_metadata_sync_timeout_ms;
    property<std::chrono::milliseconds> cloud_storage_housekeeping_interval_ms;
    property<std::chrono::milliseconds> cloud_storage_idle_timeout_ms;
    property<std::chrono::milliseconds>
      cloud_storage_cluster_metadata_upload_interval_ms;
    property<double> cloud_storage_idle_threshold_rps;
    property<bool> cloud_storage_enable_segment_merging;
    property<bool> cloud_storage_disable_upload_loop_for_tests;
    property<bool> cloud_storage_disable_read_replica_loop_for_tests;
    property<size_t> cloud_storage_max_segments_pending_deletion_per_partition;
    property<bool> cloud_storage_enable_compacted_topic_reupload;
    property<size_t> cloud_storage_recovery_temporary_retention_bytes_default;
    property<std::optional<size_t>> cloud_storage_segment_size_target;
    property<std::optional<size_t>> cloud_storage_segment_size_min;
    property<std::optional<std::chrono::milliseconds>>
      cloud_storage_graceful_transfer_timeout_ms;
    enum_property<model::cloud_storage_backend> cloud_storage_backend;
    property<std::optional<ss::sstring>> cloud_storage_credentials_host;
    bounded_property<std::optional<size_t>>
      cloud_storage_spillover_manifest_size;
    property<std::optional<size_t>>
      cloud_storage_spillover_manifest_max_segments;
    bounded_property<size_t> cloud_storage_manifest_cache_size;
    property<std::chrono::milliseconds> cloud_storage_manifest_cache_ttl_ms;
    property<std::chrono::milliseconds>
      cloud_storage_topic_purge_grace_period_ms;
    property<bool> cloud_storage_disable_upload_consistency_checks;

    // Azure Blob Storage
    property<std::optional<ss::sstring>> cloud_storage_azure_storage_account;
    property<std::optional<ss::sstring>> cloud_storage_azure_container;
    property<std::optional<ss::sstring>> cloud_storage_azure_shared_key;
    property<std::optional<ss::sstring>> cloud_storage_azure_adls_endpoint;
    property<std::optional<uint16_t>> cloud_storage_azure_adls_port;

    // Archival upload controller
    property<std::chrono::milliseconds>
      cloud_storage_upload_ctrl_update_interval_ms;
    property<double> cloud_storage_upload_ctrl_p_coeff;
    property<double> cloud_storage_upload_ctrl_d_coeff;
    property<int16_t> cloud_storage_upload_ctrl_min_shares;
    property<int16_t> cloud_storage_upload_ctrl_max_shares;

    // Defaults for local retention for partitions of topics with
    // cloud storage read and write enabled
    property<std::optional<size_t>> retention_local_target_bytes_default;
    property<std::chrono::milliseconds> retention_local_target_ms_default;
    property<bool> retention_local_strict;
    property<std::optional<uint64_t>> retention_local_target_capacity_bytes;
    bounded_property<std::optional<double>, numeric_bounds>
      retention_local_target_capacity_percent;
    property<std::chrono::milliseconds> retention_local_trim_interval;
    property<double> retention_local_trim_overage_coeff;
    property<bool> space_management_enable;
    bounded_property<double, numeric_bounds> disk_reservation_percent;
    bounded_property<uint16_t> space_management_max_log_concurrency;
    bounded_property<uint16_t> space_management_max_segment_concurrency;

    // Archival cache
    property<uint64_t> cloud_storage_cache_size;
    bounded_property<std::optional<double>, numeric_bounds>
      cloud_storage_cache_size_percent;
    property<uint32_t> cloud_storage_cache_max_objects;
    property<std::chrono::milliseconds> cloud_storage_cache_check_interval_ms;
    property<std::optional<uint32_t>>
      cloud_storage_max_segment_readers_per_shard;
    property<std::optional<uint32_t>>
      cloud_storage_max_partition_readers_per_shard;
    property<std::optional<uint32_t>>
      cloud_storage_max_materialized_segments_per_shard;
    property<uint64_t> cloud_storage_cache_chunk_size;
    property<double> cloud_storage_hydrated_chunks_per_segment_ratio;
    property<uint64_t> cloud_storage_min_chunks_per_segment_threshold;
    property<bool> cloud_storage_disable_chunk_reads;
    enum_property<model::cloud_storage_chunk_eviction_strategy>
      cloud_storage_chunk_eviction_strategy;
    property<uint16_t> cloud_storage_chunk_prefetch;

    one_or_many_property<ss::sstring> superusers;

    // kakfa queue depth control: latency ewma
    property<double> kafka_qdc_latency_alpha;
    property<std::chrono::milliseconds> kafka_qdc_window_size_ms;
    property<size_t> kafka_qdc_window_count;

    // kakfa queue depth control: queue depth ewma and control
    property<bool> kafka_qdc_enable;
    property<double> kafka_qdc_depth_alpha;
    property<std::chrono::milliseconds> kafka_qdc_max_latency_ms;
    property<size_t> kafka_qdc_idle_depth;
    property<size_t> kafka_qdc_min_depth;
    property<size_t> kafka_qdc_max_depth;
    property<std::chrono::milliseconds> kafka_qdc_depth_update_ms;
    property<size_t> zstd_decompress_workspace_bytes;
    one_or_many_property<ss::sstring> full_raft_configuration_recovery_pattern;
    property<bool> enable_auto_rebalance_on_node_add;

    enum_property<model::partition_autobalancing_mode>
      partition_autobalancing_mode;
    property<std::chrono::seconds>
      partition_autobalancing_node_availability_timeout_sec;
    bounded_property<unsigned> partition_autobalancing_max_disk_usage_percent;
    property<std::chrono::milliseconds>
      partition_autobalancing_tick_interval_ms;
    property<size_t> partition_autobalancing_movement_batch_size_bytes;
    property<size_t> partition_autobalancing_concurrent_moves;
    property<double> partition_autobalancing_tick_moves_drop_threshold;
    property<std::optional<size_t>> partition_autobalancing_min_size_threshold;

    property<bool> enable_leader_balancer;
    enum_property<model::leader_balancer_mode> leader_balancer_mode;
    property<std::chrono::milliseconds> leader_balancer_idle_timeout;
    property<std::chrono::milliseconds> leader_balancer_mute_timeout;
    property<std::chrono::milliseconds> leader_balancer_node_mute_timeout;
    bounded_property<size_t> leader_balancer_transfer_limit_per_shard;
    property<int> internal_topic_replication_factor;
    property<std::chrono::milliseconds> health_manager_tick_interval;

    // health monitor
    property<std::chrono::milliseconds> health_monitor_tick_interval;
    property<std::chrono::milliseconds> health_monitor_max_metadata_age;
    bounded_property<unsigned> storage_space_alert_free_threshold_percent;
    bounded_property<size_t> storage_space_alert_free_threshold_bytes;
    bounded_property<size_t> storage_min_free_bytes;
    property<bool> storage_strict_data_init;

    // memory related settings
    property<bool> memory_abort_on_alloc_failure;
    property<bool> sampled_memory_profile;

    // metrics reporter
    property<bool> enable_metrics_reporter;
    property<std::chrono::milliseconds> metrics_reporter_tick_interval;
    property<std::chrono::milliseconds> metrics_reporter_report_interval;
    property<ss::sstring> metrics_reporter_url;

    property<bool> features_auto_enable;

    // enables rack aware replica assignment
    property<bool> enable_rack_awareness;

    property<std::chrono::milliseconds> node_status_interval;
    property<std::chrono::milliseconds> node_status_reconnect_max_backoff_ms;
    // controller log limitng
    property<bool> enable_controller_log_rate_limiting;
    property<size_t> rps_limit_topic_operations;
    property<std::optional<size_t>>
      controller_log_accummulation_rps_capacity_topic_operations;
    property<size_t> rps_limit_acls_and_users_operations;
    property<std::optional<size_t>>
      controller_log_accummulation_rps_capacity_acls_and_users_operations;
    property<size_t> rps_limit_node_management_operations;
    property<std::optional<size_t>>
      controller_log_accummulation_rps_capacity_node_management_operations;
    property<size_t> rps_limit_move_operations;
    property<std::optional<size_t>>
      controller_log_accummulation_rps_capacity_move_operations;
    property<size_t> rps_limit_configuration_operations;
    property<std::optional<size_t>>
      controller_log_accummulation_rps_capacity_configuration_operations;

    // node and cluster throughput limiting
    bounded_property<std::optional<int64_t>> kafka_throughput_limit_node_in_bps;
    bounded_property<std::optional<int64_t>>
      kafka_throughput_limit_node_out_bps;
    bounded_property<std::chrono::milliseconds> kafka_quota_balancer_window;
    bounded_property<std::chrono::milliseconds>
      kafka_quota_balancer_node_period;
    property<double> kafka_quota_balancer_min_shard_throughput_ratio;
    bounded_property<int64_t> kafka_quota_balancer_min_shard_throughput_bps;
    property<std::vector<ss::sstring>> kafka_throughput_controlled_api_keys;
    property<std::vector<throughput_control_group>> kafka_throughput_control;

    bounded_property<int64_t> node_isolation_heartbeat_timeout;

    property<std::chrono::seconds> controller_snapshot_max_age_sec;
    // security controls
    property<bool> legacy_permit_unsafe_log_operation;
    property<std::chrono::seconds> legacy_unsafe_log_warning_interval_sec;

    // schema id validation
    enum_property<pandaproxy::schema_registry::schema_id_validation_mode>
      enable_schema_id_validation;
    config::property<size_t> kafka_schema_id_validation_cache_capacity;

    bounded_property<double, numeric_bounds> kafka_memory_share_for_fetch;
    property<size_t> kafka_memory_batch_size_estimate_for_fetch;
    // debug controls
    property<bool> cpu_profiler_enabled;
    bounded_property<std::chrono::milliseconds> cpu_profiler_sample_period_ms;

    configuration();

    error_map_t load(const YAML::Node& root_node);
};

configuration& shard_local_cfg();

} // namespace config
