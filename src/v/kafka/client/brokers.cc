// Copyright 2020 Vectorized, Inc.
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

ss::future<shared_broker_t> brokers::find(model::topic_partition tp) {
    auto l_it = _leaders.find(tp);
    if (l_it == _leaders.end()) {
        return ss::make_exception_future<shared_broker_t>(partition_error(
          std::move(tp), error_code::unknown_topic_or_partition));
    }

    return find(l_it->second);
}

ss::future<> brokers::erase(model::node_id node_id) {
    if (auto b_it = _brokers.find(node_id); b_it != _brokers.end()) {
        auto broker = *b_it;
        _brokers.erase(broker);
        return broker->stop().finally([broker]() {});
    }
    return ss::now();
}

ss::future<> brokers::apply(metadata_response&& res) {
    return ss::do_with(std::move(res), [this](metadata_response& res) {
        auto new_brokers_begin = std::partition(
          res.brokers.begin(),
          res.brokers.end(),
          [this](const metadata_response::broker& broker) {
              return _brokers.count(broker.node_id);
          });

        return ssx::parallel_transform(
                 new_brokers_begin,
                 res.brokers.end(),
                 [](const metadata_response::broker& b) {
                     return make_broker(
                       b.node_id, unresolved_address(b.host, b.port));
                 })
          .then([this, &res, new_brokers_begin, topics{std::move(res.topics)}](
                  std::vector<shared_broker_t> broker_endpoints) mutable {
              brokers_t brokers;
              brokers.reserve(
                broker_endpoints.size()
                + std::distance(res.brokers.begin(), new_brokers_begin));
              // Insert new brokers
              for (auto& b : broker_endpoints) {
                  brokers.emplace(std::move(b));
              }
              // Insert existing brokers
              for (auto it = res.brokers.begin(); it != new_brokers_begin;
                   ++it) {
                  auto b = _brokers.find(it->node_id);
                  if (b != _brokers.end()) {
                      brokers.emplace(*b);
                  }
              }

              leaders_t leaders;
              for (const auto& t : topics) {
                  for (auto const& p : t.partitions) {
                      leaders.emplace(
                        model::topic_partition(t.name, p.index), p.leader);
                  }
              }

              std::swap(brokers, _brokers);
              std::swap(leaders, _leaders);
          });
    });
}

} // namespace kafka::client
