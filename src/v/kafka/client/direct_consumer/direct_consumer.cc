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
#include "kafka/client/direct_consumer/direct_consumer.h"

#include "kafka/client/direct_consumer/data_queue.h"
#include "kafka/client/direct_consumer/fetcher.h"
#include "model/validation.h"
#include "ssx/future-util.h"

namespace kafka::client {

direct_consumer::direct_consumer(cluster& cluster, configuration cfg)
  : _cluster(&cluster)
  , _config(cfg)
  , _fetched_data_queue(
      std::make_unique<data_queue>(
        cfg.max_buffered_bytes, cfg.max_buffered_elements)) {}

ss::future<fetches>
direct_consumer::fetch_next(std::chrono::milliseconds timeout) {
    if (!_started) [[unlikely]] {
        throw std::runtime_error("Direct consumer is not started");
    }
    auto holder = _gate.hold();
    try {
        co_return co_await _fetched_data_queue->pop(timeout);
    } catch (ss::condition_variable_timed_out&) {
        co_return chunked_vector<fetched_topic_data>{};
    }
}

void direct_consumer::update_configuration(configuration cfg) {
    vlog(
      _cluster->logger().info,
      "Updating direct consumer configuration: {}",
      cfg);
    _config = cfg;
    _fetched_data_queue->set_max_bytes(cfg.max_buffered_bytes);
    _fetched_data_queue->set_max_count(cfg.max_buffered_elements);
    std::ranges::for_each(_broker_fetchers, [this](auto& pr) {
        pr.second->toggle_sessions(_config.with_sessions);
    });
}

ss::future<> direct_consumer::update_fetchers(
  [[maybe_unused]] mutex::units lock_holder,
  topic_partition_map<subscription> removals) {
    // do not update fetchers before the consumer is started
    if (!_started) {
        co_return;
    }
    auto holder = _gate.hold();
    /**
     * Unassign partitions from fetchers that are no longer needed.
     */
    for (auto& [topic, partitions] : removals) {
        for (auto& [p_id, subscription] : partitions) {
            if (subscription.current_fetcher) {
                auto& current = get_fetcher(*subscription.current_fetcher);
                co_await current.unassign_partition(
                  model::topic_partition_view(topic, p_id));
            }
        }
    }
    bool needs_metadata_update = false;
    for (auto& [topic, partitions] : _subscriptions) {
        for (auto& [p_id, sub] : partitions) {
            auto leader_id = _cluster->get_topics().leader(
              model::topic_partition_view(topic, p_id));

            if (!leader_id) {
                needs_metadata_update = true;
                if (sub.current_fetcher) {
                    // If there is a current fetcher, unassign the partition
                    auto& current = get_fetcher(*sub.current_fetcher);
                    auto offset = co_await current.unassign_partition(
                      model::topic_partition_view(topic, p_id));
                    // preserve the fetch offset for the next fetcher to use
                    sub.fetch_offset = offset;
                    sub.current_fetcher = std::nullopt;
                }
                continue;
            }

            if (sub.current_fetcher != leader_id) {
                // If the fetcher is not the same as the current one, we need to
                // assign it
                if (sub.current_fetcher) {
                    auto& current = get_fetcher(*sub.current_fetcher);
                    // If there was a previous fetcher, unassign it
                    auto offset = co_await current.unassign_partition(
                      model::topic_partition_view(topic, p_id));
                    sub.fetch_offset = offset;
                }
                sub.current_fetcher = *leader_id;
                auto& new_fetcher = get_fetcher(*leader_id);
                co_await new_fetcher.assign_partition(
                  model::topic_partition_view(topic, p_id), sub.fetch_offset);

            } else if (sub.fetch_offset) {
                // If the fetch offset is set, update it
                auto& current = get_fetcher(*sub.current_fetcher);
                co_await current.assign_partition(
                  model::topic_partition_view(topic, p_id), sub.fetch_offset);
            }
            sub.fetch_offset.reset();
        }
    }
    for (auto it = _broker_fetchers.begin(); it != _broker_fetchers.end();) {
        auto& [id, fetcher] = *it;
        if (fetcher->is_idle()) {
            co_await fetcher->stop();
            it = _broker_fetchers.erase(it);
        } else {
            ++it;
        }
    }
    if (needs_metadata_update) {
        ssx::spawn_with_gate(
          _gate, [this] { return _cluster->request_metadata_update(); });
    }

    co_return;
}

fetcher& direct_consumer::get_fetcher(model::node_id id) {
    auto it = _broker_fetchers.find(id);
    if (it == _broker_fetchers.end()) {
        vlog(_cluster->logger().debug, "Creating fetcher for broker: {}", id);
        auto new_fetcher = std::make_unique<fetcher>(
          this, id, _config.with_sessions);
        try {
            new_fetcher->start();
            auto [it, _] = _broker_fetchers.emplace(id, std::move(new_fetcher));
            return *it->second;
        } catch (...) {
            throw;
        }
    }
    return *it->second;
}

ss::future<> direct_consumer::start() {
    if (_started) {
        co_return;
    }
    _metadata_callback_id = _cluster->register_metadata_cb(
      [this](const metadata_update& d) { on_metadata_update(d); });
    _started = true;

    auto lock_holder = co_await _subscriptions_lock.get_units();
    co_await update_fetchers(std::move(lock_holder));
}

ss::future<> direct_consumer::stop() {
    _cluster->unregister_metadata_cb(_metadata_callback_id);
    _fetched_data_queue->stop();

    _subscriptions_lock.broken();
    co_await _gate.close();

    co_await ss::parallel_for_each(
      _broker_fetchers, [](auto& pair) { return pair.second->stop(); });
}

ss::future<>
direct_consumer::assign_partitions(chunked_vector<topic_assignment> topics) {
    auto lock_holder = co_await _subscriptions_lock.get_units();
    for (auto& t : topics) {
        auto ec = model::validate_kafka_topic_name(t.topic);
        if (ec) {
            throw std::invalid_argument(
              fmt::format(
                "Invalid topic name: {}, error: {}", t.topic, ec.message()));
        }
        for (auto& p : t.partitions) {
            vlog(
              _cluster->logger().trace,
              "Assigning partition: {}/{} with offset: {}",
              t.topic,
              p.partition_id,
              p.next_offset);
            auto& sub = _subscriptions[t.topic][p.partition_id];
            sub.fetch_offset = p.next_offset;
        }
    }
    co_await update_fetchers(std::move(lock_holder));
}

ss::future<>
direct_consumer::unassign_topics(chunked_vector<model::topic> topics) {
    auto lock_holder = co_await _subscriptions_lock.get_units();
    topic_partition_map<subscription> removals;
    for (const auto& topic : topics) {
        vlog(_cluster->logger().trace, "Unassigning topic: {}", topic);
        auto state_it = _subscriptions.find(topic);
        if (state_it == _subscriptions.end()) {
            continue; // nothing to remove
        }
        removals[topic].reserve(state_it->second.size());
        for (const auto& [p_id, sub] : state_it->second) {
            removals[topic][p_id] = sub;
        }
        _subscriptions.erase(topic);
    }
    co_await update_fetchers(std::move(lock_holder), std::move(removals));
}

ss::future<> direct_consumer::unassign_partitions(
  chunked_vector<model::topic_partition> partitions) {
    auto lock_holder = co_await _subscriptions_lock.get_units();
    topic_partition_map<subscription> removals;
    for (const auto& tp : partitions) {
        vlog(_cluster->logger().trace, "Unassigning partition: {}", tp);
        auto state_it = _subscriptions.find(tp.topic);
        if (state_it == _subscriptions.end()) {
            continue; // nothing to remove
        }
        auto p_it = state_it->second.find(tp.partition);

        if (p_it != state_it->second.end()) {
            removals[tp.topic][tp.partition] = p_it->second;
            state_it->second.erase(p_it);
        }
        if (state_it->second.empty()) {
            _subscriptions.erase(state_it);
        }
    }
    co_await update_fetchers(std::move(lock_holder), std::move(removals));
}

ss::future<> direct_consumer::handle_metadata_update() {
    auto lock_holder = co_await _subscriptions_lock.get_units();
    co_await update_fetchers(std::move(lock_holder));
}

void direct_consumer::on_metadata_update(const metadata_update&) {
    ssx::spawn_with_gate(_gate, [this] { return handle_metadata_update(); });
}

direct_consumer::~direct_consumer() = default;

std::ostream&
operator<<(std::ostream& o, const direct_consumer::configuration& cfg) {
    fmt::print(
      o,
      "{{ max_fetch_size: {}, partition_max_bytes: {}, reset_policy: {}, "
      "max_wait_time: {}ms, isolation_level: {}, max_buffered_bytes: {}, "
      "max_buffered_elements: {} , sessions_enabled: {}}}",
      cfg.max_fetch_size,
      cfg.partition_max_bytes,
      cfg.reset_policy,
      cfg.max_wait_time.count(),
      cfg.isolation_level,
      cfg.max_buffered_bytes,
      cfg.max_buffered_elements,
      cfg.with_sessions);

    return o;
}
} // namespace kafka::client
