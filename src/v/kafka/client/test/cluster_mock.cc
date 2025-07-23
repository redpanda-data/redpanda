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

namespace kafka::client {

broker_mock::broker_mock(
  cluster_mock* cluster_mock, model::node_id id, net::unresolved_address addr)
  : _cluster_mock(cluster_mock)
  , _id(id)
  , _addr(std::move(addr)) {}

ss::future<response_t> broker_mock::dispatch(
  request_t request,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    return _cluster_mock->handle(
      _id,
      std::move(request),
      as); // Assuming handle is a method in cluster_mock
}

api_version broker_mock::api_version_for(api_key) const {
    return api_version{0};
}

void cluster_mock::register_default_handlers() {
    register_handler(
      metadata_api::key, [this](model::node_id id, request_t req) {
          return handle_metadata_request(id, std::move(req));
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

ss::future<response_t>
cluster_mock::handle_metadata_request(model::node_id, request_t req) {
    auto md_req = std::get<metadata_request>(std::move(req));
    metadata_response_data r_data;
    for (auto& b : _brokers) {
        r_data.brokers.push_back(metadata_response::broker{
          .node_id = b.second.id,
          .host = b.second.address.host(),
          .port = b.second.address.port(),
          .rack = b.second.rack,
        });
    }
    co_return metadata_response{.data = std::move(r_data)};
}

template<typename ReqT, typename Ret>
requires(KafkaApi<typename ReqT::api_type>)
ss::future<Ret> cluster_mock::do_handle(
  model::node_id node_id,
  request_t req,
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
        return node_handler_it->second(node_id, std::move(req))
          .then([](response_t resp) { return std::get<Ret>(std::move(resp)); });
    }
    return it->second.default_handler(node_id, std::move(req))
      .then([](response_t resp) { return std::get<Ret>(std::move(resp)); });
}

ss::future<response_t> cluster_mock::handle(
  model::node_id node_id,
  request_t req,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    return ss::visit(std::move(req), [this, node_id, &as](auto r) {
        return do_handle<decltype(r)>(node_id, std::move(r), as)
          .then([](auto resp) { return response_t{std::move(resp)}; });
    });
}

cluster_mock::cluster_mock()
  : _logger(kclog, "cluster-mock") {}
} // namespace kafka::client
