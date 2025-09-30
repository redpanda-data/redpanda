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
#pragma once

#include "cluster/topic_properties.h"
#include "container/chunked_hash_map.h"
#include "kafka/client/broker.h"
#include "kafka/client/brokers.h"
#include "kafka/client/logger.h"
#include "security/acl_store.h"
namespace kafka::client {
class cluster_mock;
using supported_versions = absl::flat_hash_map<api_key, api_version_range>;

struct broker_mock : public kafka::client::broker {
    explicit broker_mock(
      cluster_mock* cluster_mock,
      model::node_id id,
      net::unresolved_address addr);
    ss::future<response_t> dispatch(
      request_t,
      api_version version = api_version{0},
      std::optional<std::reference_wrapper<ss::abort_source>>
      = std::nullopt) override;

    model::node_id id() const override { return _id; }

    ss::future<> stop() override { return ss::now(); }

    ss::future<std::optional<api_version_range>> get_supported_versions(
      api_key key,
      std::optional<std::reference_wrapper<ss::abort_source>>
      = std::nullopt) override;

    const net::unresolved_address& get_address() const override {
        return _addr;
    }

private:
    cluster_mock* _cluster_mock;
    supported_versions _supported_versions;
    model::node_id _id;
    net::unresolved_address _addr;
};

struct broker_mock_factory : public kafka::client::broker_factory {
    explicit broker_mock_factory(cluster_mock* cluster_mock)
      : _cluster_mock(cluster_mock) {}

    ss::future<shared_broker_t>
    create_broker(model::node_id id, net::unresolved_address address) override {
        return ss::make_ready_future<shared_broker_t>(
          ss::make_shared<broker_mock>(_cluster_mock, id, address));
    }

private:
    cluster_mock* _cluster_mock;
};

using mock_handler = std::function<ss::future<response_t>(
  model::node_id, request_t, api_version)>;

struct partition_metadata {
    model::partition_id id;
    model::node_id leader;
    std::vector<model::node_id> replicas;
    kafka::leader_epoch leader_epoch = kafka::invalid_leader_epoch;
    model::offset start_offset = model::offset(0);
    model::offset high_watermark = model::offset(0);
};

struct topic_metadata {
    kafka::topic_authorized_operations authorized_operations
      = kafka::topic_authorized_operations_not_set;
    model::topic_id topic_id;
    chunked_hash_map<model::partition_id, partition_metadata> partitions;
    ::cluster::topic_properties topic_properties;
};

class cluster_mock {
public:
    cluster_mock();
    void register_default_handlers();

    void register_handler(api_key, mock_handler);
    void register_broker_handler(model::node_id, api_key, mock_handler);

    void add_broker(
      model::node_id id,
      net::unresolved_address addr,
      std::optional<ss::sstring> rack = std::nullopt) {
        _brokers.emplace(
          id,
          broker_info{
            .id = id, .address = std::move(addr), .rack = std::move(rack)});
    }
    void remove_broker(model::node_id id) { _brokers.erase(id); }
    ss::future<response_t> handle_metadata_request(
      model::node_id node_id, request_t req, api_version version);

    ss::future<response_t> handle_api_versions_request(
      model::node_id node_id, request_t req, api_version version);

    ss::future<response_t> handle_describe_configs_request(
      model::node_id node_id, request_t req, api_version version);

    ss::future<response_t> handle_describe_acls_request(
      model::node_id node_id, request_t req, api_version version);

    void set_supported_versions(
      model::node_id id, api_key key, api_version_range range) {
        _broker_api_versions[id].insert_or_assign(key, range);
    }

    void add_topic(
      model::topic topic_name,
      size_t partition_count,
      size_t replication_factor,
      kafka::topic_authorized_operations authorized_operations
      = kafka::topic_authorized_operations_not_set,
      std::optional<model::topic_id> topic_id = std::nullopt);
    void remove_topic(model::topic_view topic_name);

    void set_topic_partition_count(
      model::topic_view topic_name, int32_t partition_count);
    void set_topic_replication_factor(model::topic_view topic_name, int16_t rf);
    void set_topic_properties(
      model::topic_view topic_name, ::cluster::topic_properties properties);

    std::vector<model::node_id> get_broker_ids() const {
        return std::ranges::views::keys(_brokers)
               | std::ranges::to<std::vector<model::node_id>>();
    }

    auto& get_topics() { return _topics; }

    void set_controller_id(std::optional<model::node_id> id) {
        _controller_id = id;
    }

    config::configuration& mock_config() { return _mock_config; }

    void set_cluster_authorized_operations(cluster_authorized_operations ops) {
        _cluster_authorized_operations = ops;
    }

    security::acl_store& acl_store() { return _acl_store; }

public:
    supported_versions default_supported_versions;

private:
    friend broker_mock;
    friend broker_mock_factory;

    ss::future<response_t> handle(
      model::node_id,
      request_t,
      api_version version,
      std::optional<std::reference_wrapper<ss::abort_source>>);

    struct handlers {
        mock_handler default_handler;
        absl::flat_hash_map<model::node_id, mock_handler> per_node_handlers;
    };

    absl::flat_hash_map<api_key, handlers> _handlers;
    struct broker_info {
        model::node_id id;
        net::unresolved_address address;
        std::optional<ss::sstring> rack;
    };

    template<
      typename ReqT,
      typename Ret = typename ReqT::api_type::response_type>
    requires(KafkaApi<typename ReqT::api_type>)
    ss::future<Ret> do_handle(
      model::node_id,
      request_t,
      api_version version,
      std::optional<std::reference_wrapper<ss::abort_source>>);

    absl::flat_hash_map<model::node_id, broker_info> _brokers;
    absl::flat_hash_map<model::node_id, supported_versions>
      _broker_api_versions;
    chunked_hash_map<model::topic, topic_metadata> _topics;

    config::configuration _mock_config;

    security::acl_store _acl_store;

    // If unset, will use the node_id of the first broker in _brokers
    std::optional<model::node_id> _controller_id;
    cluster_authorized_operations _cluster_authorized_operations{
      cluster_authorized_operations_not_set};
    prefix_logger _logger;
};

} // namespace kafka::client
