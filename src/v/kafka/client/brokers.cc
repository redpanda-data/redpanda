// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/client/brokers.h"

#include "kafka/protocol/metadata.h"
#include "ssx/future-util.h"

namespace kafka::client {

ss::future<> brokers::stop() {
    return ss::parallel_for_each(
      std::move(_brokers),
      [](const shared_broker_t& broker) { return broker->stop(); });
}

ss::future<shared_broker_t> brokers::any() {
    if (_brokers.empty()) {
        return ss::make_exception_future<shared_broker_t>(
          broker_error(unknown_node_id, error_code::broker_not_available));
    }

    return ss::make_ready_future<shared_broker_t>(
      *std::next(_brokers.begin(), _next_broker++ % _brokers.size()));
}

ss::future<shared_broker_t> brokers::find(model::node_id id) {
    auto b_it = _brokers.find(id);
    if (b_it == _brokers.end()) {
        return ss::make_exception_future<shared_broker_t>(
          broker_error(id, error_code::broker_not_available));
    }
    return ss::make_ready_future<shared_broker_t>(*b_it);
}

ss::future<> brokers::erase(model::node_id node_id) {
    if (auto b_it = _brokers.find(node_id); b_it != _brokers.end()) {
        auto broker = *b_it;
        _brokers.erase(broker);
        return broker->stop().finally([broker]() {});
    }
    return ss::now();
}

ss::future<> brokers::apply(chunked_vector<metadata_response::broker>&& res) {
    using new_brokers_t = chunked_vector<metadata_response::broker>;
    return ss::do_with(std::move(res), [this](new_brokers_t& new_brokers) {
        auto new_brokers_begin = std::partition(
          new_brokers.begin(),
          new_brokers.end(),
          [this](const metadata_response::broker& broker) {
              return _brokers.count(broker.node_id);
          });

        return ssx::parallel_transform(
                 new_brokers_begin,
                 new_brokers.end(),
                 [this](const metadata_response::broker& b) {
                     return make_broker(
                       b.node_id,
                       net::unresolved_address(b.host, b.port),
                       _config);
                 })
          .then([this, &new_brokers, new_brokers_begin](
                  std::vector<shared_broker_t> broker_endpoints) mutable {
              brokers_t brokers;
              brokers.reserve(
                broker_endpoints.size()
                + std::distance(new_brokers.begin(), new_brokers_begin));
              // Insert new brokers
              for (auto& b : broker_endpoints) {
                  brokers.emplace(std::move(b));
              }
              // Insert existing brokers
              for (auto it = new_brokers.begin(); it != new_brokers_begin;
                   ++it) {
                  auto b = _brokers.find(it->node_id);
                  if (b != _brokers.end()) {
                      brokers.emplace(*b);
                  }
              }

              std::swap(brokers, _brokers);
          });
    });
}

ss::future<bool> brokers::empty() const {
    return ss::make_ready_future<bool>(_brokers.empty());
}

} // namespace kafka::client
