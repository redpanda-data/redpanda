/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "config/broker_endpoint.h"
#include "config/config_store.h"
#include "config/convert.h"
#include "config/data_directory_path.h"
#include "config/endpoint_tls_config.h"
#include "config/property.h"
#include "config/tls_config.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"
#include "net/unresolved_address.h"

#include <seastar/core/sstring.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/ip.hh>
#include <seastar/net/socket_defs.hh>

#include <cctype>
#include <chrono>

namespace config {

/// Redpanda configuration
///
/// All application modules depend on configuration. The configuration module
/// can not depend on any other module to prevent cyclic dependencies.

struct configuration final : public config_store {
    // WAL
    property<bool> developer_mode;
    property<uint64_t> log_segment_size;
    property<uint64_t> compacted_log_segment_size;
    property<std::chrono::milliseconds> readers_cache_eviction_timeout_ms;
    // Network
    property<std::optional<int>> rpc_server_listen_backlog;
    clamped_property<std::optional<int>> rpc_server_tcp_recv_buf;
    clamped_property<std::optional<int>> rpc_server_tcp_send_buf;
    // Coproc
    property<bool> enable_coproc;
    property<std::size_t> coproc_max_inflight_bytes;
    property<std::size_t> coproc_max_ingest_bytes;
    property<std::size_t> coproc_max_batch_size;
    property<std::chrono::milliseconds> coproc_offset_flush_interval_ms;

    // Raft
    deprecated_property seed_server_meta_topic_partitions;
    property<std::chrono::milliseconds> raft_heartbeat_interval_ms;
    property<std::chrono::milliseconds> raft_heartbeat_timeout_ms;
    property<size_t> raft_heartbeat_disconnect_failures;
    deprecated_property min_version;
    deprecated_property max_version;
    // Kafka
    deprecated_property use_scheduling_groups;
    property<bool> enable_admin_api;
    property<int16_t> default_num_windows;
    property<std::chrono::milliseconds> default_window_sec;
    property<std::chrono::milliseconds> quota_manager_gc_sec;
    property<uint32_t> target_quota_byte_rate;
    property<std::optional<ss::sstring>> cluster_id;
    property<bool> disable_metrics;
    property<std::chrono::milliseconds> group_min_session_timeout_ms;
    property<std::chrono::milliseconds> group_max_session_timeout_ms;
    property<std::chrono::milliseconds> group_initial_rebalance_delay;
    property<std::chrono::milliseconds> group_new_member_join_timeout;
    property<std::chrono::milliseconds> metadata_dissemination_interval_ms;
    property<std::chrono::milliseconds> metadata_dissemination_retry_delay_ms;
    property<int16_t> metadata_dissemination_retries;
    property<std::chrono::milliseconds> tm_sync_timeout_ms;
    property<model::violation_recovery_policy> tm_violation_recovery_policy;
    property<std::chrono::milliseconds> rm_sync_timeout_ms;
    property<std::chrono::milliseconds> tx_timeout_delay_ms;
    property<model::violation_recovery_policy> rm_violation_recovery_policy;
    property<std::chrono::milliseconds> fetch_reads_debounce_timeout;
    property<std::chrono::milliseconds> alter_topic_cfg_timeout_ms;
    property<model::cleanup_policy_bitflags> log_cleanup_policy;
    property<model::timestamp_type> log_message_timestamp_type;
    property<model::compression> log_compression_type;
    property<size_t> fetch_max_bytes;
    property<std::chrono::milliseconds> metadata_status_wait_timeout_ms;
    // same as transactional.id.expiration.ms in kafka
    property<std::chrono::milliseconds> transactional_id_expiration_ms;
    property<bool> enable_idempotence;
    property<bool> enable_transactions;
    property<uint32_t> abort_index_segment_size;
    // same as log.retention.ms in kafka
    property<std::chrono::milliseconds> delete_retention_ms;
    property<std::chrono::milliseconds> log_compaction_interval_ms;
    // same as retention.size in kafka - TODO: size not implemented
    property<std::optional<size_t>> retention_bytes;
    property<int32_t> group_topic_partitions;
    property<int16_t> default_topic_replication;
    property<int16_t> transaction_coordinator_replication;
    property<int16_t> id_allocator_replication;
    property<model::cleanup_policy_bitflags>
      transaction_coordinator_cleanup_policy;
    property<std::chrono::milliseconds>
      transaction_coordinator_delete_retention_ms;
    property<uint64_t> transaction_coordinator_log_segment_size;
    property<std::chrono::milliseconds>
      abort_timed_out_transactions_interval_ms;
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
    property<uint32_t> raft_smp_max_non_local_requests;
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
    property<size_t> append_chunk_size;
    property<size_t> storage_read_buffer_size;
    property<int16_t> storage_read_readahead_count;
    property<size_t> segment_fallocation_step;
    property<size_t> max_compacted_log_segment_size;
    property<int16_t> id_allocator_log_capacity;
    property<int16_t> id_allocator_batch_size;
    property<bool> enable_sasl;
    property<std::chrono::milliseconds>
      controller_backend_housekeeping_interval_ms;
    property<std::chrono::milliseconds> node_management_operation_timeout_ms;
    // Compaction controller
    property<std::chrono::milliseconds> compaction_ctrl_update_interval_ms;
    property<double> compaction_ctrl_p_coeff;
    property<double> compaction_ctrl_i_coeff;
    property<double> compaction_ctrl_d_coeff;
    property<int16_t> compaction_ctrl_min_shares;
    property<int16_t> compaction_ctrl_max_shares;
    property<std::optional<size_t>> compaction_ctrl_backlog_size;
    property<std::chrono::milliseconds> members_backend_retry_ms;

    // Archival storage
    property<bool> cloud_storage_enabled;
    property<bool> cloud_storage_enable_remote_read;
    property<bool> cloud_storage_enable_remote_write;
    property<std::optional<ss::sstring>> cloud_storage_access_key;
    property<std::optional<ss::sstring>> cloud_storage_secret_key;
    property<std::optional<ss::sstring>> cloud_storage_region;
    property<std::optional<ss::sstring>> cloud_storage_bucket;
    property<std::optional<ss::sstring>> cloud_storage_api_endpoint;
    property<std::chrono::milliseconds> cloud_storage_reconciliation_ms;
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

    // Archival upload controller
    property<std::chrono::milliseconds>
      cloud_storage_upload_ctrl_update_interval_ms;
    property<double> cloud_storage_upload_ctrl_p_coeff;
    property<double> cloud_storage_upload_ctrl_d_coeff;
    property<int16_t> cloud_storage_upload_ctrl_min_shares;
    property<int16_t> cloud_storage_upload_ctrl_max_shares;

    // Archival cache
    property<size_t> cloud_storage_cache_size;
    property<std::chrono::milliseconds> cloud_storage_cache_check_interval_ms;

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

    property<bool> enable_leader_balancer;
    property<std::chrono::milliseconds> leader_balancer_idle_timeout;
    property<std::chrono::milliseconds> leader_balancer_mute_timeout;
    property<std::chrono::milliseconds> leader_balancer_node_mute_timeout;
    property<int> internal_topic_replication_factor;
    property<std::chrono::milliseconds> health_manager_tick_interval;

    // health monitor
    property<std::chrono::milliseconds> health_monitor_tick_interval;
    property<std::chrono::milliseconds> health_monitor_max_metadata_age;
    // metrics reporter
    property<bool> enable_metrics_reporter;
    property<std::chrono::milliseconds> metrics_reporter_tick_interval;
    property<std::chrono::milliseconds> metrics_reporter_report_interval;
    property<ss::sstring> metrics_reporter_url;

    configuration();

    void load(const YAML::Node& root_node);
};

configuration& shard_local_cfg();

} // namespace config
