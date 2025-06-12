/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster_link/tests/test_utils.h"

#include "kafka/server/handlers/configs/config_response_utils.h"

namespace cluster_link::tests {

class cluster_link_test_metadata_adapter : public kafka::metadata_cache_info {
public:
    ::model::compression get_default_compression() const override {
        return config::shard_local_cfg().log_compression_type();
    }
    ::model::cleanup_policy_bitflags
    get_default_cleanup_policy_bitflags() const override {
        return config::shard_local_cfg().log_cleanup_policy();
    }
    size_t get_default_compacted_topic_segment_size() const override {
        return config::shard_local_cfg().compacted_log_segment_size();
    }
    size_t get_default_segment_size() const override {
        return config::shard_local_cfg().log_segment_size();
    }
    std::optional<std::chrono::milliseconds>
    get_default_retention_duration() const override {
        return config::shard_local_cfg().log_retention_ms();
    }
    std::optional<size_t> get_default_retention_bytes() const override {
        return config::shard_local_cfg().retention_bytes();
    }
    ::model::timestamp_type get_default_timestamp_type() const override {
        return config::shard_local_cfg().log_message_timestamp_type();
    }
    uint32_t get_default_batch_max_bytes() const override {
        return config::shard_local_cfg().kafka_batch_max_bytes();
    }
    ::model::shadow_indexing_mode
    get_default_shadow_indexing_mode() const override {
        ::model::shadow_indexing_mode m
          = ::model::shadow_indexing_mode::disabled;
        if (config::shard_local_cfg().cloud_storage_enable_remote_write()) {
            m = ::model::shadow_indexing_mode::archival;
        }
        if (config::shard_local_cfg().cloud_storage_enable_remote_read()) {
            m = ::model::add_shadow_indexing_flag(
              m, ::model::shadow_indexing_mode::fetch);
        }
        return m;
    }
    std::optional<size_t>
    get_default_retention_local_target_bytes() const override {
        return config::shard_local_cfg().retention_local_target_bytes_default();
    }
    std::chrono::milliseconds
    get_default_retention_local_target_ms() const override {
        return config::shard_local_cfg().retention_local_target_ms_default();
    }
    std::optional<std::chrono::milliseconds>
    get_default_segment_ms() const override {
        return config::shard_local_cfg().log_segment_ms();
    }
    std::optional<std::chrono::milliseconds>
    get_default_delete_retention_ms() const override {
        return config::shard_local_cfg().tombstone_retention_ms();
    }
    bool get_default_record_key_schema_id_validation() const override {
        return false;
    }
    pandaproxy::schema_registry::subject_name_strategy
    get_default_record_key_subject_name_strategy() const override {
        return pandaproxy::schema_registry::subject_name_strategy::topic_name;
    }
    bool get_default_record_value_schema_id_validation() const override {
        return false;
    }
    pandaproxy::schema_registry::subject_name_strategy
    get_default_record_value_subject_name_strategy() const override {
        return pandaproxy::schema_registry::subject_name_strategy::topic_name;
    }
    std::optional<size_t>
    get_default_initial_retention_local_target_bytes() const override {
        return config::shard_local_cfg()
          .initial_retention_local_target_bytes_default();
    }
    std::optional<std::chrono::milliseconds>
    get_default_initial_retention_local_target_ms() const override {
        return config::shard_local_cfg()
          .initial_retention_local_target_ms_default();
    }
    std::chrono::milliseconds
    get_default_iceberg_target_lag_ms() const override {
        return config::shard_local_cfg().iceberg_target_lag_ms();
    }
    std::optional<double>
    get_default_min_cleanable_dirty_ratio() const override {
        return config::shard_local_cfg().min_cleanable_dirty_ratio();
    }
    std::chrono::milliseconds
    get_default_min_compaction_lag_ms() const override {
        return config::shard_local_cfg().min_compaction_lag_ms();
    }
    std::chrono::milliseconds
    get_default_max_compaction_lag_ms() const override {
        return config::shard_local_cfg().max_compaction_lag_ms();
    }
};

ss::future<test_connection::result<void>> test_connection::connect() {
    _is_connected = true;
    return ss::make_ready_future<result<void>>(outcome::success());
}

ss::future<test_connection::result<void>> test_connection::disconnect() {
    _is_connected = false;
    return ss::make_ready_future<result<void>>(outcome::success());
}

ss::future<test_connection::result<kafka::metadata_response>>
test_connection::fetch_metadata(kafka::metadata_request req) {
    kafka::metadata_response resp;
    resp.data.throttle_time_ms = std::chrono::milliseconds(0);
    resp.data.brokers = {
      {.node_id = _source_cluster->controller_id(),
       .host = "localhost",
       .port = 9092}};
    resp.data.cluster_id = _source_cluster->cluster_id();
    resp.data.controller_id = _source_cluster->controller_id();
    resp.data.topics = create_metadata_topics_response(req);
    if (req.data.include_cluster_authorized_operations) {
        resp.data.cluster_authorized_operations
          = _source_cluster->cluster_authorized_operations();
    }

    co_return resp;
}

ss::future<test_connection::result<kafka::describe_configs_response>>
test_connection::describe_topic_configs(
  chunked_vector<::model::topic> topics,
  std::optional<chunked_vector<ss::sstring>> config_keys) {
    kafka::describe_configs_response resp;
    resp.data.results.reserve(topics.size());
    for (const auto& t : topics) {
        resp.data.results.push_back(kafka::describe_configs_result{
          .error_code = kafka::error_code::none,
          .resource_type = kafka::config_resource_type::topic,
          .resource_name = t});
        auto& result = resp.data.results.back();
        auto source_topic = _source_cluster->find_topic(
          {::model::kafka_namespace, t});
        if (!source_topic.has_value()) {
            result.error_code = kafka::error_code::unknown_topic_or_partition;
            continue;
        }
        const auto& cfg = source_topic->get().topic_configuration();
        const auto& properties = cfg.properties;
        auto config_resp = kafka::make_topic_configs(
          cluster_link_test_metadata_adapter{},
          properties,
          config_keys,
          true,
          false);
        result.configs.reserve(config_resp.size());
        for (auto& conf : config_resp) {
            result.configs.push_back(conf.to_describe_config());
        }
    }
    co_return resp;
}

small_fragment_vector<kafka::metadata_response_topic>
test_connection::create_metadata_topics_response(
  const kafka::metadata_request& req) {
    small_fragment_vector<kafka::metadata_response_topic> resp;
    auto all_topics = _source_cluster->all_topics();
    for (const auto& t : all_topics) {
        const auto& topic = _source_cluster->find_topic(t);
        vassert(topic.has_value(), "Topic metadata not found");
        resp.emplace_back(kafka::metadata_response_topic{
          .error_code = kafka::error_code::none,
          .name = t.tp,
          .is_internal = topic->get().topic_configuration().is_internal(),
          .partitions = create_metadata_partitions_response(topic->get()),
          .topic_authorized_operations
          = req.data.include_topic_authorized_operations
              ? topic->get().authorized_operations()
              : int32_t{-2147483648}});
    }
    return resp;
}

large_fragment_vector<kafka::metadata_response_partition>
test_connection::create_metadata_partitions_response(const source_topic& t) {
    large_fragment_vector<kafka::metadata_response_partition> partitions;
    partitions.reserve(t.model_metadata().partitions.size());
    for (const auto& p : t.model_metadata().partitions) {
        partitions.emplace_back(kafka::metadata_response_partition{
          .error_code = kafka::error_code::none,
          .partition_index = p.id,
          .leader_id = p.leader_node.value_or(::model::node_id{-1}),
          .leader_epoch = kafka::leader_epoch{0},
        });
    }
    return partitions;
}

test_connection_factory::test_connection_factory(source_cluster* source_cluster)
  : _source_cluster(source_cluster) {}

std::unique_ptr<remote_cluster_connection>
test_connection_factory::create_remote_cluster_connection() {
    return std::make_unique<test_connection>(_source_cluster);
}
} // namespace cluster_link::tests
