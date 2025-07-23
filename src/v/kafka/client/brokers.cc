// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/client/brokers.h"

#include "kafka/client/types.h"
#include "kafka/protocol/metadata.h"
#include "ssx/future-util.h"

namespace kafka::client {

ss::future<> brokers::stop() {
    return ss::parallel_for_each(
      std::move(_brokers),
      [](const shared_broker_t& broker) { return broker->stop(); });
}

shared_broker_t brokers::any() {
    if (_brokers.empty()) {
        throw broker_error(unknown_node_id, error_code::broker_not_available);
    }
    _next_broker = ++_next_broker % _brokers.size();
    return *std::next(_brokers.begin(), _next_broker);
}

shared_broker_t brokers::find(model::node_id id) {
    auto b_it = _brokers.find(id);
    if (b_it == _brokers.end()) {
        throw broker_error(id, error_code::broker_not_available);
    }
    return *b_it;
}

ss::future<> brokers::erase(model::node_id node_id) {
    if (auto b_it = _brokers.find(node_id); b_it != _brokers.end()) {
        auto broker = *b_it;
        _brokers.erase(b_it);
        vlog(
          _logger->debug,
          "Erasing broker {} - {}:{}",
          broker->id(),
          broker->get_address().host(),
          broker->get_address().port());
        return broker->stop().finally([broker]() {});
    }
    return ss::now();
}

ss::future<>
brokers::apply(chunked_vector<metadata_response::broker> brokers_metadata) {
    chunked_vector<metadata_response::broker> brokers_to_add;
    chunked_vector<model::node_id> brokers_to_remove;

    for (auto& broker : brokers_metadata) {
        auto it = _brokers.find(broker.node_id);
        // not found broker, we need to add it
        if (it == _brokers.end()) {
            brokers_to_add.push_back(std::move(broker));
            continue;
        }
        auto& existing_broker = *it;
        if (
          existing_broker->get_address()
          != net::unresolved_address(broker.host, broker.port)) {
            // recreate broker with the new address
            brokers_to_remove.push_back(broker.node_id);
            brokers_to_add.push_back(std::move(broker));
        }
    }
    for (auto& b : _brokers) {
        auto m_it = std::ranges::find_if(
          brokers_metadata,
          [id = b->id()](const auto& m) { return m.node_id == id; });

        if (m_it == brokers_metadata.end()) {
            // broker not found in the metadata, we need to remove it
            brokers_to_remove.push_back(b->id());
        }
    }

    co_await ss::parallel_for_each(
      brokers_to_remove.begin(),
      brokers_to_remove.end(),
      [this](model::node_id id) { return erase(id); });
    std::exception_ptr exception = nullptr;
    for (auto& b : brokers_to_add) {
        auto broker = co_await _factory.create_broker(
          b.node_id, net::unresolved_address(b.host, b.port));
        _brokers.insert(broker);
    }
}

ss::future<shared_broker_t>
brokers::create_broker(model::node_id node_id, net::unresolved_address addr) {
    return _factory.create_broker(node_id, std::move(addr));
}

bool brokers::empty() const { return _brokers.empty(); }

} // namespace kafka::client
