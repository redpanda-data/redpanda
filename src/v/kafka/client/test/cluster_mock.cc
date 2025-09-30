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
#include "kafka/client/test/cluster_mock.h"

#include "kafka/server/handlers/configs/config_response_utils.h"
#include "kafka/server/handlers/details/security.h"

namespace kafka::client {

class cluster_link_test_metadata_adapter : public kafka::metadata_cache_info {
public:
    explicit cluster_link_test_metadata_adapter(config::configuration* config)
      : _config(config) {}

    ::model::compression get_default_compression() const override {
        return _config->log_compression_type();
    }
    ::model::cleanup_policy_bitflags
    get_default_cleanup_policy_bitflags() const override {
        return _config->log_cleanup_policy();
    }
    size_t get_default_compacted_topic_segment_size() const override {
        return _config->compacted_log_segment_size();
    }
    size_t get_default_segment_size() const override {
        return _config->log_segment_size();
    }
    std::optional<std::chrono::milliseconds>
    get_default_retention_duration() const override {
        return _config->log_retention_ms();
    }
    std::optional<size_t> get_default_retention_bytes() const override {
        return _config->retention_bytes();
    }
    ::model::timestamp_type get_default_timestamp_type() const override {
        return _config->log_message_timestamp_type();
    }
    uint32_t get_default_batch_max_bytes() const override {
        return _config->kafka_batch_max_bytes();
    }
    ::model::shadow_indexing_mode
    get_default_shadow_indexing_mode() const override {
        ::model::shadow_indexing_mode m
          = ::model::shadow_indexing_mode::disabled;
        if (_config->cloud_storage_enable_remote_write()) {
            m = ::model::shadow_indexing_mode::archival;
        }
        if (_config->cloud_storage_enable_remote_read()) {
            m = ::model::add_shadow_indexing_flag(
              m, ::model::shadow_indexing_mode::fetch);
        }
        return m;
    }
    std::optional<size_t>
    get_default_retention_local_target_bytes() const override {
        return _config->retention_local_target_bytes_default();
    }
    std::chrono::milliseconds
    get_default_retention_local_target_ms() const override {
        return _config->retention_local_target_ms_default();
    }
    std::optional<std::chrono::milliseconds>
    get_default_segment_ms() const override {
        return _config->log_segment_ms();
    }
    std::optional<std::chrono::milliseconds>
    get_default_delete_retention_ms() const override {
        return _config->tombstone_retention_ms();
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
        return _config->iceberg_target_lag_ms();
    }
    std::optional<double>
    get_default_min_cleanable_dirty_ratio() const override {
        return _config->min_cleanable_dirty_ratio();
    }
    std::chrono::milliseconds
    get_default_min_compaction_lag_ms() const override {
        return _config->min_compaction_lag_ms();
    }
    std::chrono::milliseconds
    get_default_max_compaction_lag_ms() const override {
        return _config->max_compaction_lag_ms();
    }
    std::chrono::milliseconds
    get_default_message_timestamp_before_max_ms() const override {
        return _config->log_message_timestamp_before_max_ms();
    }
    std::chrono::milliseconds
    get_default_message_timestamp_after_max_ms() const override {
        return _config->log_message_timestamp_after_max_ms();
    }

private:
    config::configuration* _config;
};

broker_mock::broker_mock(
  cluster_mock* cluster_mock, model::node_id id, net::unresolved_address addr)
  : _cluster_mock(cluster_mock)
  , _id(id)
  , _addr(std::move(addr)) {}

ss::future<response_t> broker_mock::dispatch(
  request_t request,
  api_version version,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    return _cluster_mock->handle(
      _id,
      std::move(request),
      version,
      as); // Assuming handle is a method in cluster_mock
}

ss::future<std::optional<api_version_range>>
broker_mock::get_supported_versions(
  api_key key, std::optional<std::reference_wrapper<ss::abort_source>> as) {
    if (_supported_versions.empty()) {
        auto resp = co_await dispatch(
          api_versions_request{},
          api_versions_request::api_type::max_valid,
          as);

        auto api_versions_resp = std::get<api_versions_response>(
          std::move(resp));
        for (const auto& resp_key : api_versions_resp.data.api_keys) {
            _supported_versions[api_key(resp_key.api_key)] = api_version_range{
              .min = api_version(resp_key.min_version),
              .max = api_version(resp_key.max_version),
            };
        }
    }
    auto it = _supported_versions.find(key);
    if (it == _supported_versions.end()) {
        co_return std::nullopt;
    }
    co_return it->second;
}

void cluster_mock::register_default_handlers() {
    register_handler(
      metadata_api::key,
      [this](model::node_id id, request_t req, api_version version) {
          return handle_metadata_request(id, std::move(req), version);
      });

    register_handler(
      describe_configs_api::key,
      [this](model::node_id id, request_t req, api_version version) {
          return handle_describe_configs_request(id, std::move(req), version);
      });

    register_handler(
      api_versions_api::key,
      [this](model::node_id id, request_t req, api_version version) {
          return handle_api_versions_request(id, std::move(req), version);
      });

    register_handler(
      describe_acls_api::key,
      [this](model::node_id id, request_t req, api_version version) {
          return handle_describe_acls_request(id, std::move(req), version);
      });
}

void cluster_mock::register_handler(api_key key, mock_handler handler) {
    auto& h = _handlers[key];
    h.default_handler = std::move(handler);
}
void cluster_mock::register_broker_handler(
  model::node_id id, api_key key, mock_handler handler) {
    auto& h = _handlers[key];
    h.per_node_handlers[id] = std::move(handler);
}

ss::future<response_t> cluster_mock::handle_metadata_request(
  model::node_id, request_t req, api_version v) {
    static const auto topic_id_support_version = api_version(10);
    auto md_req = std::get<metadata_request>(std::move(req));
    metadata_response_data r_data;
    for (auto& b : _brokers) {
        r_data.brokers.push_back(
          metadata_response::broker{
            .node_id = b.second.id,
            .host = b.second.address.host(),
            .port = b.second.address.port(),
            .rack = b.second.rack,
          });
    }

    for (const auto& [topic, md] : _topics) {
        metadata_response::topic md_topic;
        md_topic.name = topic;
        if (v >= topic_id_support_version) {
            md_topic.topic_id = md.topic_id;
        }

        md_topic.topic_authorized_operations
          = md_req.data.include_topic_authorized_operations
              ? md.authorized_operations
              : kafka::topic_authorized_operations_not_set;
        md_topic.partitions.reserve(md.partitions.size());
        for (auto& [part_id, part_meta] : md.partitions) {
            md_topic.partitions.push_back(
              metadata_response::partition{
                .partition_index = part_id,
                .leader_id = part_meta.leader,
                .leader_epoch = part_meta.leader_epoch,
                .replica_nodes = part_meta.replicas});
        }
        r_data.topics.push_back(std::move(md_topic));
    }

    r_data.controller_id = _controller_id.value_or(_brokers.begin()->first);
    r_data.cluster_authorized_operations
      = md_req.data.include_cluster_authorized_operations
          ? _cluster_authorized_operations
          : kafka::cluster_authorized_operations_not_set;

    co_return metadata_response{.data = std::move(r_data)};
}

namespace {
void report_topic_config(
  const describe_configs_resource& resource,
  describe_configs_result& result,
  const metadata_cache_info& metadata_cache,
  const cluster::topic_properties& topic_properties,
  bool include_synonyms,
  bool include_documentation) {
    auto res = make_topic_configs(
      metadata_cache,
      topic_properties,
      resource.configuration_keys,
      include_synonyms,
      include_documentation);

    result.configs.reserve(res.size());
    for (auto& conf : res) {
        result.configs.push_back(conf.to_describe_config());
    }
}
} // namespace

ss::future<response_t> cluster_mock::handle_describe_configs_request(
  model::node_id, request_t req, api_version) {
    auto dc_req = std::get<describe_configs_request>(std::move(req));
    describe_configs_response_data r_data;
    r_data.results.reserve(dc_req.data.resources.size());

    for (const auto& resource : dc_req.data.resources) {
        describe_configs_result result;
        result.resource_name = resource.resource_name;
        result.resource_type = resource.resource_type;
        if (resource.resource_type != kafka::config_resource_type::topic) {
            result.error_code = kafka::error_code::unsupported_version;
            result.error_message = ssx::sformat(
              "Unsupported resource type: {}", resource.resource_type);
            r_data.results.emplace_back(std::move(result));
            continue;
        }
        auto topic_it = _topics.find(model::topic{resource.resource_name});
        if (topic_it == _topics.end()) {
            result.error_code = kafka::error_code::unknown_topic_or_partition;
            result.error_message = ssx::sformat(
              "Unknown topic: {}", resource.resource_name);
            r_data.results.emplace_back(std::move(result));
            continue;
        }
        report_topic_config(
          resource,
          result,
          cluster_link_test_metadata_adapter{&_mock_config},
          topic_it->second.topic_properties,
          dc_req.data.include_synonyms,
          dc_req.data.include_documentation);
        r_data.results.emplace_back(std::move(result));
    }

    co_return describe_configs_response{.data = std::move(r_data)};
}

namespace {
api_versions_response
make_api_versions_response(const supported_versions& versions) {
    api_versions_response_data r_data;
    for (const auto& [key, range] : versions) {
        r_data.api_keys.push_back(
          api_versions_response_key{
            .api_key = key,
            .min_version = range.min,
            .max_version = range.max});
    }
    return api_versions_response{.data = std::move(r_data)};
}
} // namespace

ss::future<response_t> cluster_mock::handle_api_versions_request(
  model::node_id node_id, request_t, api_version) {
    auto it = _broker_api_versions.find(node_id);
    if (it == _broker_api_versions.end()) {
        co_return make_api_versions_response(default_supported_versions);
    }
    co_return make_api_versions_response(it->second);
}

ss::future<response_t> cluster_mock::handle_describe_acls_request(
  model::node_id, request_t req, api_version) {
    auto describe_req = std::get<describe_acls_request>(std::move(req));
    auto filter = details::to_acl_binding_filter(describe_req.data);
    auto bindings = _acl_store.acls(filter);

    chunked_hash_map<
      security::resource_pattern,
      chunked_vector<security::acl_entry>>
      entries;

    kafka::describe_acls_response response;
    auto& response_data = response.data;

    for (const auto& binding : bindings) {
        entries[binding.pattern()].emplace_back(binding.entry());
    }

    for (auto& entry : entries) {
        response_data.resources.push_back(
          details::acl_entry_to_resource(
            entry.first,
            std::move(entry.second),
            describe_req.data.describe_registry_acls));
    }

    co_return response;
}

template<typename ReqT, typename Ret>
requires(KafkaApi<typename ReqT::api_type>)
ss::future<Ret> cluster_mock::do_handle(
  model::node_id node_id,
  request_t req,
  api_version version,
  std::optional<std::reference_wrapper<ss::abort_source>>) {
    using api_t = typename ReqT::api_type;
    _logger.info(
      "handling request node: {}, api: {}, request: {}",
      node_id,
      api_t::name,
      req);

    auto it = _handlers.find(api_t::key);
    if (it == _handlers.end()) {
        throw std::runtime_error(
          fmt::format("No handler registered for API key: {}", api_t::key));
    }
    auto node_handler_it = it->second.per_node_handlers.find(node_id);
    if (node_handler_it != it->second.per_node_handlers.end()) {
        // If a specific handler for the node is registered, use it
        return node_handler_it->second(node_id, std::move(req), version)
          .then([](response_t resp) { return std::get<Ret>(std::move(resp)); });
    }
    return it->second.default_handler(node_id, std::move(req), version)
      .then([](response_t resp) { return std::get<Ret>(std::move(resp)); });
}

ss::future<response_t> cluster_mock::handle(
  model::node_id node_id,
  request_t req,
  api_version version,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    return ss::visit(std::move(req), [this, node_id, version, &as](auto r) {
        return do_handle<decltype(r)>(node_id, std::move(r), version, as)
          .then([](auto resp) { return response_t{std::move(resp)}; });
    });
}

void cluster_mock::add_topic(
  model::topic topic_name,
  size_t partition_count,
  size_t replication_factor,
  kafka::topic_authorized_operations authorized_operations,
  std::optional<model::topic_id> topic_id) {
    if (_topics.contains(topic_name)) {
        // Topic already exists, do not overwrite
        throw std::invalid_argument(
          fmt::format("Topic {} already exists", topic_name));
    }
    if (replication_factor > _brokers.size()) {
        throw std::invalid_argument(
          fmt::format(
            "Replication factor {} exceeds available brokers",
            replication_factor));
    }

    auto cluster_nodes = get_broker_ids();
    std::ranges::sort(cluster_nodes);

    topic_metadata md;
    md.authorized_operations = authorized_operations;
    md.topic_id = topic_id.value_or(model::topic_id{uuid_t::create()});

    for (auto p_id : std::views::iota(size_t(0), partition_count)) {
        partition_metadata p_md{
          .id = model::partition_id(p_id), .leader = model::node_id(-1)};

        std::copy_n(
          cluster_nodes.begin(),
          replication_factor,
          std::back_inserter(p_md.replicas));
        std::ranges::rotate(cluster_nodes, cluster_nodes.begin() + 1);
        if (!p_md.replicas.empty()) {
            p_md.leader = p_md.replicas[0];
        }
        p_md.leader_epoch = kafka::invalid_leader_epoch;
        md.partitions.emplace(model::partition_id(p_id), std::move(p_md));
    }

    _topics.emplace(topic_name, std::move(md));
}

void cluster_mock::remove_topic(model::topic_view topic_name) {
    auto topics_it = _topics.find(topic_name);
    if (topics_it == _topics.end()) {
        throw std::invalid_argument(
          fmt::format("Topic {} does not exist", topic_name));
    }
    _topics.erase(topics_it);
}

void cluster_mock::set_topic_partition_count(
  model::topic_view topic_name, int32_t partition_count) {
    auto topics_it = _topics.find(topic_name);
    if (topics_it == _topics.end()) {
        throw std::invalid_argument(
          fmt::format("Topic {} does not exist", topic_name));
    }

    if (partition_count < 1) {
        throw std::invalid_argument(
          fmt::format(
            "Invalid partition count {} for topic {}",
            partition_count,
            topic_name));
    }

    if (
      topics_it->second.partitions.size()
      > static_cast<size_t>(partition_count)) {
        throw std::invalid_argument(
          fmt::format(
            "Cannot reduce partition count for topic {} from {} to {}",
            topic_name,
            topics_it->second.partitions.size(),
            partition_count));
    }

    if (
      topics_it->second.partitions.size()
      == static_cast<size_t>(partition_count)) {
        // No change needed
        return;
    }

    auto rf = topics_it->second.partitions.begin()->second.replicas.size();

    auto cluster_nodes = get_broker_ids();
    std::ranges::sort(cluster_nodes);

    for (auto p_id : std::views::iota(
           topics_it->second.partitions.size(),
           static_cast<size_t>(partition_count))) {
        partition_metadata p_md{
          .id = model::partition_id(p_id), .leader = model::node_id(-1)};
        std::copy_n(
          cluster_nodes.begin(), rf, std::back_inserter(p_md.replicas));
        if (!p_md.replicas.empty()) {
            p_md.leader = p_md.replicas[0];
        }
        p_md.leader_epoch = kafka::invalid_leader_epoch;
        topics_it->second.partitions.emplace(
          model::partition_id(p_id), std::move(p_md));
    }
}

void cluster_mock::set_topic_replication_factor(
  model::topic_view topic_name, int16_t rf) {
    auto topics_it = _topics.find(topic_name);
    if (topics_it == _topics.end()) {
        throw std::invalid_argument(
          fmt::format("Topic {} does not exist", topic_name));
    }
    if (rf < 1) {
        throw std::invalid_argument(
          fmt::format(
            "Invalid replication factor {} for topic {}", rf, topic_name));
    }
    if (static_cast<size_t>(rf) > _brokers.size()) {
        throw std::invalid_argument(
          fmt::format(
            "Replication factor {} exceeds available brokers for topic {}",
            rf,
            topic_name));
    }

    auto cur_rf = topics_it->second.partitions.begin()->second.replicas.size();
    if (cur_rf == static_cast<size_t>(rf)) {
        // No change needed
        return;
    }

    auto cluster_nodes = get_broker_ids();
    std::ranges::sort(cluster_nodes);
    for (auto& [_, meta] : topics_it->second.partitions) {
        if (cur_rf > static_cast<size_t>(rf)) {
            // Reduce replication factor
            meta.replicas.resize(static_cast<size_t>(rf));
        } else {
            auto diff = static_cast<size_t>(rf) - cur_rf;
            std::copy_n(
              cluster_nodes.begin() + cur_rf,
              diff,
              std::back_inserter(meta.replicas));
        }
    }
}

void cluster_mock::set_topic_properties(
  model::topic_view topic_name, cluster::topic_properties properties) {
    auto topics_it = _topics.find(topic_name);
    if (topics_it == _topics.end()) {
        throw std::invalid_argument(
          fmt::format("Topic {} does not exist", topic_name));
    }
    topics_it->second.topic_properties = std::move(properties);
}

cluster_mock::cluster_mock()
  : _logger(kclog, "cluster-mock") {
    default_supported_versions[metadata_api::key] = {
      .min = kafka::metadata_api::min_valid,
      .max = kafka::metadata_api::max_valid};
    default_supported_versions[api_versions_api::key] = {
      .min = kafka::api_versions_api::min_valid,
      .max = kafka::api_versions_api::max_valid};
    default_supported_versions[describe_configs_api::key] = {
      .min = kafka::describe_configs_api::min_valid,
      .max = kafka::describe_configs_api::max_valid};
    default_supported_versions[describe_acls_api::key] = {
      .min = api_version{0}, .max = api_version{2}};
}
} // namespace kafka::client
