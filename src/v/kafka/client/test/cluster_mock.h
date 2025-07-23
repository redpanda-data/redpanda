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

#include "kafka/client/broker.h"
#include "kafka/client/brokers.h"
#include "kafka/client/logger.h"
namespace kafka::client {
class cluster_mock;

struct broker_mock : public kafka::client::broker {
    explicit broker_mock(
      cluster_mock* cluster_mock,
      model::node_id id,
      net::unresolved_address addr);
    ss::future<response_t> dispatch(
      request_t,
      std::optional<std::reference_wrapper<ss::abort_source>> as) override;

    model::node_id id() const override { return _id; }

    ss::future<> stop() override { return ss::now(); }

    api_version api_version_for(api_key key) const override;

    const net::unresolved_address& get_address() const override {
        return _addr;
    }

private:
    cluster_mock* _cluster_mock;
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

using mock_handler
  = std::function<ss::future<response_t>(model::node_id, request_t)>;

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
    ss::future<response_t>
    handle_metadata_request(model::node_id node_id, request_t req);

private:
    friend broker_mock;

    ss::future<response_t> handle(
      model::node_id,
      request_t,
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
      std::optional<std::reference_wrapper<ss::abort_source>>);

    absl::flat_hash_map<model::node_id, broker_info> _brokers;
    prefix_logger _logger;
};

} // namespace kafka::client
