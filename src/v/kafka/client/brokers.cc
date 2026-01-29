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
    _state_mutex.broken();
    return ss::parallel_for_each(
      std::move(_brokers), [](const auto& p) { return p.second->stop(); });
}

shared_broker_t brokers::any() {
    if (_brokers.empty()) {
        throw broker_error(unknown_node_id, error_code::broker_not_available);
    }
    _next_broker = ++_next_broker % _brokers.size();
    return std::next(_brokers.begin(), _next_broker)->second;
}

shared_broker_t brokers::find(model::node_id id) {
    auto b_it = _brokers.find(id);
    if (b_it == _brokers.end()) {
        throw broker_error(id, error_code::broker_not_available);
    }
    return b_it->second;
}

ss::future<> brokers::erase(model::node_id node_id) {
    auto u = co_await _state_mutex.get_units();
    co_await do_erase(node_id);
}

ss::future<> brokers::clear() {
    auto u = co_await _state_mutex.get_units();
    co_await do_clear();
}

ss::future<> brokers::do_erase(model::node_id node_id) {
    if (auto b_it = _brokers.find(node_id); b_it != _brokers.end()) {
        auto broker = b_it->second;
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

ss::future<> brokers::do_clear() {
    const auto to_string = std::views::transform([](const auto& broker) {
        return ss::format(
          "{} - {}:{}",
          broker->id(),
          broker->get_address().host(),
          broker->get_address().port());
    });
    vlog(
      _logger->debug,
      "Clear brokers: {}",
      fmt::join(_brokers | std::views::values | to_string, ","));
    auto brokers = std::exchange(_brokers, brokers_t{});
    for (auto& b : brokers | std::views::values) {
        co_await b->stop();
    }
}

ss::future<> brokers::apply(
  const chunked_vector<metadata_update::broker>& brokers_metadata) {
    auto u = co_await _state_mutex.get_units();
    chunked_vector<metadata_update::broker> brokers_to_add;
    chunked_vector<model::node_id> brokers_to_remove;

    for (const auto& broker : brokers_metadata) {
        auto it = _brokers.find(broker.node_id);
        // not found broker, we need to add it
        if (it == _brokers.end()) {
            brokers_to_add.push_back(broker);
            continue;
        }
        auto& existing_broker = it->second;
        if (
          existing_broker->get_address()
          != net::unresolved_address(broker.host, broker.port)) {
            // recreate broker with the new address
            brokers_to_remove.push_back(broker.node_id);
            brokers_to_add.push_back(broker);
        }
    }
    for (auto& [id, b] : _brokers) {
        auto m_it = std::ranges::find_if(
          brokers_metadata, [id](const auto& m) { return m.node_id == id; });

        if (m_it == brokers_metadata.end()) {
            // broker not found in the metadata, we need to remove it
            brokers_to_remove.push_back(b->id());
        }
    }

    co_await ss::parallel_for_each(
      brokers_to_remove.begin(),
      brokers_to_remove.end(),
      [this](model::node_id id) { return do_erase(id); });

    for (auto& b : brokers_to_add) {
        auto id = b.node_id;
        auto broker = co_await _factory->create_broker(
          id, net::unresolved_address(b.host, b.port));
        _brokers.emplace(id, std::move(broker));
    }
}

ss::future<shared_broker_t>
brokers::create_broker(model::node_id node_id, net::unresolved_address addr) {
    return _factory->create_broker(node_id, std::move(addr));
}

bool brokers::empty() const { return _brokers.empty(); }

ss::future<std::optional<api_version_range>> brokers::supported_api_versions(
  api_key key, std::optional<std::reference_wrapper<ss::abort_source>> as) {
    auto u = co_await _state_mutex.get_units();
    if (_brokers.empty()) {
        co_return std::nullopt;
    }
    api_version_range range{
      .min = api_version{std::numeric_limits<int16_t>::min()},
      .max = api_version{std::numeric_limits<int16_t>::max()}};
    for (auto& [id, broker] : _brokers) {
        auto v = co_await broker->get_supported_versions(key, as);

        if (!v) {
            co_return std::nullopt;
        }
        range.min = std::max(range.min, v->min);
        range.max = std::min(range.max, v->max);
    }
    co_return range;
}

ss::future<std::optional<api_version_range>> brokers::supported_api_versions(
  model::node_id id,
  api_key key,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    auto u = co_await _state_mutex.get_units();
    auto it = _brokers.find(id);
    if (it == _brokers.end()) {
        co_return std::nullopt;
    }
    co_return co_await it->second->get_supported_versions(key, as);
}
} // namespace kafka::client
