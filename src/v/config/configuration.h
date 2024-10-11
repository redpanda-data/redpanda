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
#include "config/leaders_preference.h"
#include "config/property.h"
#include "config/throughput_control_group.h"
#include "config/tls_config.h"
#include "config/types.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"
#include "pandaproxy/schema_registry/schema_id_validation.h"
#include "utils/unresolved_address.h"

#include <seastar/core/sstring.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/ip.hh>
#include <seastar/net/socket_defs.hh>

#include <cctype>
#include <chrono>
#include <vector>

class monitor_unsafe;

namespace config {

/// Redpanda configuration
///
/// All application modules depend on configuration. The configuration module
/// can not depend on any other module to prevent cyclic dependencies.

struct configuration;

/*
 * A configuration property wrapper that fails validation unless experimental
 * property support has already been enabled.
 *
 * Note that our configuration system does not understand dependencies between
 * configuration options. So if experimental feature support were enabled within
 * the same request that tried to modify an experimental feature property it may
 * fail depending on which order updates were applied to the config store.
 *
 * The simplest way to circumvent this is to enable support in one request,
 * and then proceed to interact with experiemntal feature properties.
 *
 * Development properties are hidden from the outside world until experimental
 * property support is enabled, at which point the visibility of the property is
 * identical to the wrapped property.
 */
template<typename T>
class development_feature_property : public property<T> {
public:
    development_feature_property(
      configuration& conf,
      std::string_view name,
      std::string_view desc,
      base_property::metadata meta,
      T def,
      property<T>::validator validator = property<T>::noop_validator)
      : property<T>(
          conf,
          name,
          desc,
          meta,
          def,
          [&conf, validator = std::move(validator)](
            const auto& v) -> std::optional<ss::sstring> {
              if (development_features_enabled(conf)) {
                  // delegate to the underlying property's validator
                  return validator(v);
              }
              return "Development feature support is not enabled.";
          })
      , _conf(conf)

    {}

    bool is_hidden() const override {
        if (development_features_enabled(_conf)) {
            return property<T>::is_hidden();
        }
        return true;
    }

private:
    static bool development_features_enabled(const configuration&);
    configuration& _conf;
};

struct configuration final : public config_store {
    constexpr static auto target_produce_quota_byte_rate_default
      = 0; // disabled

    // WAL
    bounded_property<uint64_t> log_segment_size;
    property<std::optional<uint64_t>> log_segment_size_min;
    property<std::optional<uint64_t>> log_segment_size_max;
    bounded_property<uint16_t> log_segment_size_jitter_percent;
    bounded_property<uint64_t> compacted_log_segment_size;
    property<std::chrono::milliseconds> readers_cache_eviction_timeout_ms;
    bounded_property<size_t> readers_cache_target_max_size;

    bounded_property<std::optional<std::chrono::milliseconds>> log_segment_ms;
    bounded_property<std::chrono::milliseconds> log_segment_ms_min;
    bounded_property<std::chrono::milliseconds> log_segment_ms_max;

    // Network
    bounded_property<std::optional<int>> rpc_server_listen_backlog;
    bounded_property<std::optional<int>> rpc_server_tcp_recv_buf;
    bounded_property<std::optional<int>> rpc_server_tcp_send_buf;
    bounded_property<int> rpc_client_connections_per_peer;
    property<bool> rpc_server_compress_replies;
    // Coproc
    deprecated_property enable_coproc;
    deprecated_property coproc_max_inflight_bytes;
    deprecated_property coproc_max_ingest_bytes;
    deprecated_property coproc_max_batch_size;
    deprecated_property coproc_offset_flush_interval_ms;

    // Data Transforms
    property<bool> data_transforms_enabled;
    property<std::chrono::milliseconds> data_transforms_commit_interval_ms;
    bounded_property<size_t> data_transforms_per_core_memory_reservation;
    bounded_property<size_t> data_transforms_per_function_memory_limit;
    property<std::chrono::milliseconds> data_transforms_runtime_limit_ms;
    bounded_property<size_t> data_transforms_binary_max_size;
    bounded_property<size_t> data_transforms_logging_buffer_capacity_bytes;
    property<std::chrono::milliseconds>
      data_transforms_logging_flush_interval_ms;
    property<size_t> data_transforms_logging_line_max_bytes;
    bounded_property<size_t> data_transforms_read_buffer_memory_percentage;
    bounded_property<size_t> data_transforms_write_buffer_memory_percentage;

    // Controller
    bounded_property<std::optional<std::size_t>> topic_memory_per_partition;
    bounded_property<std::optional<int32_t>> topic_fds_per_partition;
    bounded_property<uint32_t> topic_partitions_per_shard;
    bounded_property<uint32_t> topic_partitions_reserve_shard0;
    property<std::chrono::milliseconds>
      partition_manager_shutdown_watchdog_timeout;

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
    bounded_property<size_t> raft_recovery_concurrency_per_shard;
    property<std::optional<size_t>> raft_replica_max_pending_flush_bytes;
    property<std::chrono::milliseconds> raft_flush_timer_interval_ms;
    property<std::chrono::milliseconds> raft_replica_max_flush_delay_ms;
    property<bool> raft_enable_longest_log_detection;
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
    deprecated_property tx_registry_sync_timeout_ms;
    deprecated_property tm_violation_recovery_policy;
    property<std::chrono::milliseconds> rm_sync_timeout_ms;
    deprecated_property find_coordinator_timeout_ms;
    deprecated_property seq_table_min_size;
    property<std::chrono::milliseconds> tx_timeout_delay_ms;
    deprecated_property rm_violation_recovery_policy;
    property<std::chrono::milliseconds> fetch_reads_debounce_timeout;
    enum_property<model::fetch_read_strategy> fetch_read_strategy;
    bounded_property<double, numeric_bounds> fetch_pid_p_coeff;
    bounded_property<double, numeric_bounds> fetch_pid_i_coeff;
    bounded_property<double, numeric_bounds> fetch_pid_d_coeff;
    bounded_property<double, numeric_bounds>
      fetch_pid_target_utilization_fraction;
    property<std::chrono::milliseconds> fetch_pid_max_debounce_ms;
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
    retention_duration_property log_retention_ms;
    property<std::chrono::milliseconds> log_compaction_interval_ms;
    property<bool> log_disable_housekeeping_for_tests;
    property<bool> log_compaction_use_sliding_window;
    // same as retention.size in kafka - TODO: size not implemented
    property<std::optional<size_t>> retention_bytes;
    property<int32_t> group_topic_partitions;
    bounded_property<int16_t> default_topic_replication;
    bounded_property<int16_t> minimum_topic_replication;
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
    // same as transaction.max.timeout.ms in Apache Kafka.
    property<std::chrono::milliseconds> transaction_max_timeout_ms;
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
    enum_property<model::write_caching_mode> write_caching_default;

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
    bounded_property<uint64_t> storage_compaction_key_map_memory;
    bounded_property<double, numeric_bounds>
      storage_compaction_key_map_memory_limit_percent;
    property<size_t> max_compacted_log_segment_size;
    property<std::optional<std::chrono::seconds>>
      storage_ignore_timestamps_in_future_sec;
    property<bool> storage_ignore_cstore_hints;
    bounded_property<int16_t> storage_reserve_min_segments;
    property<std::optional<uint32_t>> debug_load_slice_warning_depth;

    deprecated_property tx_registry_log_capacity;
    property<int16_t> id_allocator_log_capacity;
    property<int16_t> id_allocator_batch_size;
    property<bool> enable_sasl;
    property<std::vector<ss::sstring>> sasl_mechanisms;
    property<ss::sstring> sasl_kerberos_config;
    property<ss::sstring> sasl_kerberos_keytab;
    property<ss::sstring> sasl_kerberos_principal;
    property<std::vector<ss::sstring>> sasl_kerberos_principal_mapping;
    bounded_property<std::optional<std::chrono::milliseconds>>
      kafka_sasl_max_reauth_ms;
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

    // Audit logging
    property<bool> audit_enabled;
    property<int32_t> audit_log_num_partitions;
    property<std::optional<int16_t>> audit_log_replication_factor;
    property<size_t> audit_client_max_buffer_size;
    property<std::chrono::milliseconds> audit_queue_drain_interval_ms;
    property<size_t> audit_queue_max_buffer_size_per_shard;
    property<std::vector<ss::sstring>> audit_enabled_event_types;
    property<std::vector<ss::sstring>> audit_excluded_topics;
    property<std::vector<ss::sstring>> audit_excluded_principals;

    // Archival storage
    property<bool> cloud_storage_enabled;
    property<bool> cloud_storage_enable_remote_read;
    property<bool> cloud_storage_enable_remote_write;
    property<bool> cloud_storage_disable_archiver_manager;
    property<std::optional<ss::sstring>> cloud_storage_access_key;
    property<std::optional<ss::sstring>> cloud_storage_secret_key;
    property<std::optional<ss::sstring>> cloud_storage_region;
    property<std::optional<ss::sstring>> cloud_storage_bucket;
    property<std::optional<ss::sstring>> cloud_storage_api_endpoint;
    enum_property<std::optional<s3_url_style>> cloud_storage_url_style;
    enum_property<model::cloud_credentials_source>
      cloud_storage_credentials_source;
    property<std::optional<ss::sstring>>
      cloud_storage_azure_managed_identity_id;
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
    property<std::optional<ss::sstring>> cloud_storage_crl_file;
    property<std::chrono::milliseconds> cloud_storage_initial_backoff_ms;
    property<std::chrono::milliseconds> cloud_storage_segment_upload_timeout_ms;
    property<std::chrono::milliseconds>
      cloud_storage_manifest_upload_timeout_ms;
    property<std::chrono::milliseconds>
      cloud_storage_garbage_collect_timeout_ms;
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
    property<std::chrono::milliseconds>
      cloud_storage_cluster_metadata_upload_timeout_ms;
    property<size_t>
      cloud_storage_cluster_metadata_num_consumer_groups_per_upload;
    property<int16_t> cloud_storage_cluster_metadata_retries;
    property<bool> cloud_storage_attempt_cluster_restore_on_bootstrap;
    property<double> cloud_storage_idle_threshold_rps;
    property<int32_t> cloud_storage_background_jobs_quota;
    property<bool> cloud_storage_enable_segment_merging;
    property<bool> cloud_storage_enable_scrubbing;
    property<std::chrono::milliseconds> cloud_storage_partial_scrub_interval_ms;
    property<std::chrono::milliseconds> cloud_storage_full_scrub_interval_ms;
    property<std::chrono::milliseconds>
      cloud_storage_scrubbing_interval_jitter_ms;
    property<bool> cloud_storage_disable_upload_loop_for_tests;
    property<bool> cloud_storage_disable_read_replica_loop_for_tests;
    property<bool> disable_cluster_recovery_loop_for_tests;
    property<bool> enable_cluster_metadata_upload_loop;
    property<size_t> cloud_storage_max_segments_pending_deletion_per_partition;
    property<bool> cloud_storage_enable_compacted_topic_reupload;
    property<size_t> cloud_storage_recovery_temporary_retention_bytes_default;
    // validation of topic manifest during recovery
    enum_property<model::recovery_validation_mode>
      cloud_storage_recovery_topic_validation_mode;
    property<uint32_t> cloud_storage_recovery_topic_validation_depth;

    property<std::optional<size_t>> cloud_storage_segment_size_target;
    property<std::optional<size_t>> cloud_storage_segment_size_min;
    property<std::optional<size_t>> cloud_storage_max_throughput_per_shard;
    bounded_property<std::optional<size_t>>
      cloud_storage_throughput_limit_percent;
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
    property<bool> cloud_storage_disable_metadata_consistency_checks;
    property<std::chrono::milliseconds> cloud_storage_hydration_timeout_ms;
    property<bool> cloud_storage_disable_remote_labels_for_tests;

    // Azure Blob Storage
    property<std::optional<ss::sstring>> cloud_storage_azure_storage_account;
    property<std::optional<ss::sstring>> cloud_storage_azure_container;
    property<std::optional<ss::sstring>> cloud_storage_azure_shared_key;
    property<std::optional<ss::sstring>> cloud_storage_azure_adls_endpoint;
    property<std::optional<uint16_t>> cloud_storage_azure_adls_port;
    property<std::optional<bool>>
      cloud_storage_azure_hierarchical_namespace_enabled;

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
    property<bool> retention_local_strict_override;
    property<std::optional<uint64_t>> retention_local_target_capacity_bytes;
    bounded_property<std::optional<double>, numeric_bounds>
      retention_local_target_capacity_percent;
    property<std::chrono::milliseconds> retention_local_trim_interval;
    property<double> retention_local_trim_overage_coeff;
    property<bool> space_management_enable;
    property<bool> space_management_enable_override;
    bounded_property<double, numeric_bounds> disk_reservation_percent;
    bounded_property<uint16_t> space_management_max_log_concurrency;
    bounded_property<uint16_t> space_management_max_segment_concurrency;
    property<std::optional<size_t>>
      initial_retention_local_target_bytes_default;
    property<std::optional<std::chrono::milliseconds>>
      initial_retention_local_target_ms_default;

    // Archival cache
    property<uint64_t> cloud_storage_cache_size;
    bounded_property<std::optional<double>, numeric_bounds>
      cloud_storage_cache_size_percent;
    property<uint32_t> cloud_storage_cache_max_objects;
    property<uint32_t> cloud_storage_cache_trim_carryover_bytes;
    property<std::chrono::milliseconds> cloud_storage_cache_check_interval_ms;
    bounded_property<uint16_t> cloud_storage_cache_trim_walk_concurrency;
    property<std::optional<uint32_t>>
      cloud_storage_max_segment_readers_per_shard;
    property<std::optional<uint32_t>>
      cloud_storage_max_partition_readers_per_shard;
    property<std::optional<uint32_t>>
      cloud_storage_max_concurrent_hydrations_per_shard;
    property<std::optional<uint32_t>>
      cloud_storage_max_materialized_segments_per_shard;
    property<uint64_t> cloud_storage_cache_chunk_size;
    property<double> cloud_storage_hydrated_chunks_per_segment_ratio;
    property<uint64_t> cloud_storage_min_chunks_per_segment_threshold;
    property<bool> cloud_storage_disable_chunk_reads;
    enum_property<model::cloud_storage_chunk_eviction_strategy>
      cloud_storage_chunk_eviction_strategy;
    property<uint16_t> cloud_storage_chunk_prefetch;
    bounded_property<uint32_t> cloud_storage_cache_num_buckets;
    bounded_property<std::optional<double>, numeric_bounds>
      cloud_storage_cache_trim_threshold_percent_size;
    bounded_property<std::optional<double>, numeric_bounds>
      cloud_storage_cache_trim_threshold_percent_objects;

    property<bool> cloud_storage_inventory_based_scrub_enabled;
    property<ss::sstring> cloud_storage_inventory_id;
    property<ss::sstring> cloud_storage_inventory_reports_prefix;
    property<bool> cloud_storage_inventory_self_managed_report_config;
    property<std::chrono::milliseconds>
      cloud_storage_inventory_report_check_interval_ms;
    property<uint64_t> cloud_storage_inventory_max_hash_size_during_parse;

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
    property<bool> lz4_decompress_reusable_buffers_disabled;
    deprecated_property full_raft_configuration_recovery_pattern;
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
    property<bool> partition_autobalancing_topic_aware;

    property<bool> enable_leader_balancer;
    deprecated_property leader_balancer_mode;
    property<std::chrono::milliseconds> leader_balancer_idle_timeout;
    property<std::chrono::milliseconds> leader_balancer_mute_timeout;
    property<std::chrono::milliseconds> leader_balancer_node_mute_timeout;
    bounded_property<size_t> leader_balancer_transfer_limit_per_shard;
    property<config::leaders_preference> default_leaders_preference;

    property<bool> core_balancing_on_core_count_change;
    property<bool> core_balancing_continuous;
    property<std::chrono::milliseconds> core_balancing_debounce_timeout;

    property<int> internal_topic_replication_factor;
    property<std::chrono::milliseconds> health_manager_tick_interval;

    // health monitor
    property<std::chrono::milliseconds> health_monitor_tick_interval;
    property<std::chrono::milliseconds> health_monitor_max_metadata_age;
    bounded_property<unsigned> storage_space_alert_free_threshold_percent;
    bounded_property<size_t> storage_space_alert_free_threshold_bytes;
    bounded_property<size_t> storage_min_free_bytes;
    property<bool> storage_strict_data_init;
    property<std::chrono::milliseconds> alive_timeout_ms;

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
    property<bool> kafka_throughput_throttling_v2;
    bounded_property<std::optional<int64_t>>
      kafka_throughput_replenish_threshold;
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

    property<bool> schema_registry_normalize_on_startup;
    property<std::optional<uint32_t>> pp_sr_smp_max_non_local_requests;
    bounded_property<size_t> max_in_flight_schema_registry_requests_per_shard;
    bounded_property<size_t> max_in_flight_pandaproxy_requests_per_shard;

    bounded_property<double, numeric_bounds> kafka_memory_share_for_fetch;
    property<size_t> kafka_memory_batch_size_estimate_for_fetch;
    // debug controls
    property<bool> cpu_profiler_enabled;
    bounded_property<std::chrono::milliseconds> cpu_profiler_sample_period_ms;
    property<std::filesystem::path> rpk_path;
    property<std::optional<std::filesystem::path>> debug_bundle_storage_dir;
    property<std::optional<std::chrono::seconds>>
      debug_bundle_auto_removal_seconds;

    // oidc authentication
    property<ss::sstring> oidc_discovery_url;
    property<ss::sstring> oidc_token_audience;
    property<std::chrono::seconds> oidc_clock_skew_tolerance;
    property<ss::sstring> oidc_principal_mapping;
    property<std::chrono::seconds> oidc_keys_refresh_interval;

    // HTTP Authentication
    property<std::vector<ss::sstring>> http_authentication;

    // MPX
    property<bool> enable_mpx_extensions;
    bounded_property<uint64_t> virtual_cluster_min_producer_ids;

    // temporary - to be deprecated
    property<bool> unsafe_enable_consumer_offsets_delete_retention;

    enum_property<tls_version> tls_min_version;

    // datalake configurations
    property<bool> iceberg_enabled;

    configuration();

    error_map_t load(const YAML::Node& root_node);

public:
    development_feature_property<bool> development_enable_cloud_topics;

    development_feature_property<int> development_feature_property_testing_only;

private:
    // to query if experimental features are enabled in order to log a nag. it
    // does not use the query to control any experimental feature.
    friend class ::monitor_unsafe;

    template<typename T>
    friend class development_feature_property;

    /*
     * This configuration property shouldn't be queried directly. Rather, it is
     * used to enable the use of other feature-specific experimental properties.
     *
     * This property is hidden when its value is the same as its default, which
     * corresponds to the case in which experimental features are not enabled.
     *
     * In addition to this property thus being hidden in production scenarios,
     * it also fixes several tests like rpk-import-export that expect to be able
     * to set values for all properties that it discovers.
     */
    hidden_when_default_property<ss::sstring>
      enable_developmental_unrecoverable_data_corrupting_features;

    bool development_features_enabled() const {
        return !enable_developmental_unrecoverable_data_corrupting_features()
                  .empty();
    }
};

template<typename T>
bool development_feature_property<T>::development_features_enabled(
  const configuration& conf) {
    return conf.development_features_enabled();
}

configuration& shard_local_cfg();

} // namespace config
