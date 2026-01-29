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

#include "kafka/client/direct_consumer/api_types.h"
#include "kafka/client/direct_consumer/data_queue.h"
#include "kafka/client/direct_consumer/fetcher.h"
#include "kafka/protocol/errors.h"
#include "model/validation.h"
#include "ssx/future-util.h"

#include <algorithm>
#include <chrono>
#include <functional>

namespace kafka::client {

direct_consumer::direct_consumer(
  cluster& cluster,
  configuration cfg,
  std::optional<direct_consumer_probe::configuration> probe_cfg)
  : _cluster(&cluster)
  , _config(cfg)
  , _fetched_data_queue(
      std::make_unique<data_queue>(
        cfg.max_buffered_bytes, cfg.max_buffered_elements))
  , _probe{} {
    if (probe_cfg.has_value()) {
        _probe.emplace(std::move(probe_cfg.value()));
    }
}

ss::future<fetches>
direct_consumer::fetch_next(std::chrono::milliseconds timeout) {
    // ss::lowres_clock's resolution is around the duration of task_quota and
    // default task_quota is 500us
    // adds a reasonable minimum timeout to calls to pop
    static constexpr auto minimum_timeout = std::chrono::milliseconds{1};
    if (!_started) [[unlikely]] {
        throw std::runtime_error("Direct consumer is not started");
    }
    auto holder = _gate.hold();
    auto deadline = ss::lowres_clock::now() + timeout;

    try {
        // we'll keep attempting to pluck from the queue until the timeout is
        // exhausted
        while (ss::lowres_clock::now() < deadline) {
            // either the remaining timeout or a small but reasonable minimum
            // timeout
            auto timeout_remaining = std::max(
              std::chrono::duration_cast<std::chrono::milliseconds>(
                deadline - ss::lowres_clock::now()),
              minimum_timeout);

            auto maybe_response_to_filter = co_await _fetched_data_queue->pop(
              timeout_remaining);
            if (maybe_response_to_filter.has_error()) {
                co_return maybe_response_to_filter;
            }

            // the remainder is synchronous, no lock is needed
            auto& response_to_filter = maybe_response_to_filter.value();

            // filters if needed, will modify response in place
            filter_fetch_data(response_to_filter);

            // if the filters have removed everything, there are no updates.
            // poll the queue again
            if (response_to_filter.empty()) {
                continue;
            }

            co_return maybe_response_to_filter;
        }
    } catch (ss::condition_variable_timed_out&) {
        co_return chunked_vector<fetched_topic_data>{};
    }
    // fallthrough loop case
    co_return chunked_vector<fetched_topic_data>{};
}

void direct_consumer::filter_fetch_data(
  chunked_vector<fetched_topic_data>& responses_to_filter) {
    // for each topic, remove stale partitions
    for (auto& topic_data : responses_to_filter) {
        auto& partition_data = topic_data.partitions;

        auto to_remove_subsegment = std::ranges::partition(
          partition_data,
          [this,
           &topic_data](const fetched_partition_data& partition_data) mutable {
              auto maybe_subscription = find_subscription(
                topic_data.topic, partition_data.partition_id);

              vlog(
                _cluster->logger().debug,
                "tp: {}/{}, current subscription epoch: {}, found request "
                "epoch: {}",
                topic_data.topic,
                partition_data.partition_id,
                partition_data.subscription_epoch,
                maybe_subscription.transform([](const subscription& sub) {
                    return sub.subscription_epoch;
                }));

              // if the fetch is stale, no further checks required, remove it
              if (is_partition_data_stale(partition_data, maybe_subscription)) {
                  // adjust the size calculation given that data is potentially
                  // being dropped
                  topic_data.total_bytes -= partition_data.size_bytes;
                  return false;
              }

              vassert(
                maybe_subscription,
                "offset filtering requires that unassigned subscriptions have "
                "already been filtered out");
              return update_and_filter_offsets(
                topic_data.topic, partition_data, *maybe_subscription);
          });

        // erase everything that is stale
        auto erase_iterator = to_remove_subsegment.begin();
        partition_data.erase_to_end(erase_iterator);
    }

    // remove newly empty topics
    auto empty_subsegment = std::ranges::partition(
      responses_to_filter, [](const fetched_topic_data& topic_data) {
          return !topic_data.partitions.empty();
      });

    responses_to_filter.erase_to_end(empty_subsegment.begin());
}

bool direct_consumer::is_partition_data_stale(
  const fetched_partition_data& partition_data,
  const std::optional<subscription>& maybe_subscription) {
    if (!maybe_subscription) {
        return true;
    }
    return partition_data.subscription_epoch
           != maybe_subscription->subscription_epoch;
}

bool direct_consumer::update_and_filter_offsets(
  model::topic topic_name,
  const fetched_partition_data& partition_data,
  subscription& subscription) {
    // rules
    // if there is data attached, don't filter it
    // if there is new offset info, don't filter it
    // if the response is an error, don't filter it

    source_partition_offsets spo{
      .log_start_offset = partition_data.start_offset,
      .high_watermark = partition_data.high_watermark,
      .last_stable_offset = partition_data.last_stable_offset,
      .last_offset_update_timestamp = ss::lowres_clock::now()};

    // the response is an error, send it back to the consumer with
    // no updates to state
    if (partition_data.error != kafka::error_code::none) {
        return true;
    }

    // not an update, screen it out after updating last seen
    if (
      spo.are_offsets_equal(subscription.last_known_source_offsets)
      && partition_data.data.empty()) {
        subscription.last_known_source_offsets.last_offset_update_timestamp
          = ss::lowres_clock::now();
        return false;
    }

    // log expectations on offsets, update then return
    if (
      subscription.last_known_source_offsets.log_start_offset
      > spo.log_start_offset) {
        vlog(
          _cluster->logger().error,
          "{}/{} log start offset should never move backward, current: "
          "{}, "
          "found: {}",
          topic_name,
          partition_data.partition_id,
          subscription.last_known_source_offsets.log_start_offset,
          spo.log_start_offset);
    }
    if (
      subscription.last_known_source_offsets.high_watermark
      > spo.high_watermark) {
        vlog(
          _cluster->logger().warn,
          "{}/{} high watermark should not normally move backward, "
          "current: "
          "{}, found: {}",
          topic_name,
          partition_data.partition_id,
          subscription.last_known_source_offsets.high_watermark,
          spo.high_watermark);
    }
    if (
      subscription.last_known_source_offsets.last_stable_offset
      > spo.last_stable_offset) {
        vlog(
          _cluster->logger().error,
          "{}/{} last known stable should never move backward, "
          "current: {}, found: {}",
          topic_name,
          partition_data.partition_id,
          subscription.last_known_source_offsets.last_stable_offset,
          spo.last_stable_offset);
    }
    subscription.last_known_source_offsets = spo;
    return true;
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

std::optional<source_partition_offsets>
direct_consumer::get_source_offsets(model::topic_partition_view tp) const {
    return find_subscription(tp.topic, tp.partition)
      .transform([](std::reference_wrapper<const subscription> sub) {
          return sub.get().last_known_source_offsets;
      });
}

ss::future<> direct_consumer::update_fetchers(
  [[maybe_unused]] ssx::mutex::units lock_holder,
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
                  model::topic_partition_view(topic, p_id),
                  sub.fetch_offset,
                  sub.subscription_epoch);

            } else if (sub.fetch_offset) {
                // If the fetch offset is set, update it
                auto& current = get_fetcher(*sub.current_fetcher);
                co_await current.assign_partition(
                  model::topic_partition_view(topic, p_id),
                  sub.fetch_offset,
                  sub.subscription_epoch);
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

std::optional<std::reference_wrapper<const direct_consumer::subscription>>
direct_consumer::find_subscription(
  const model::topic& topic, model::partition_id partition_id) const {
    auto t_it = _subscriptions.find(topic);
    if (t_it == _subscriptions.end()) {
        return std::nullopt;
    }

    auto& p_map = t_it->second;
    auto p_it = p_map.find(partition_id);
    if (p_it == p_map.end()) {
        return std::nullopt;
    }
    return p_it->second;
}

std::optional<std::reference_wrapper<direct_consumer::subscription>>
direct_consumer::find_subscription(
  const model::topic& topic, model::partition_id partition_id) {
    auto t_it = _subscriptions.find(topic);
    if (t_it == _subscriptions.end()) {
        return std::nullopt;
    }
    auto& p_map = t_it->second;
    auto p_it = p_map.find(partition_id);
    if (p_it == p_map.end()) {
        return std::nullopt;
    }
    return p_it->second;
}

std::optional<subscription_epoch> direct_consumer::find_subscription_epoch(
  const model::topic& topic, model::partition_id partition_id) const {
    return find_subscription(topic, partition_id).transform([](auto sub) {
        return sub.get().subscription_epoch;
    });
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
    vlog(_cluster->logger().trace, "Stopping direct consumer");
    _cluster->unregister_metadata_cb(_metadata_callback_id);
    _fetched_data_queue->stop();

    _subscriptions_lock.broken();
    co_await _gate.close();

    co_await ss::parallel_for_each(
      _broker_fetchers, [](auto& pair) { return pair.second->stop(); });
    _broker_fetchers.clear();
    vlog(_cluster->logger().trace, "Direct consumer stopped");
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
            auto epoch_to_assign = ++epoch;
            vlog(
              _cluster->logger().trace,
              "Assigning partition: {}/{} with offset: {} and epoch {}",
              t.topic,
              p.partition_id,
              p.next_offset,
              epoch_to_assign);
            _subscriptions[t.topic].insert_or_assign(
              p.partition_id,
              subscription{std::nullopt, p.next_offset, epoch_to_assign});
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
            removals[topic].insert_or_assign(p_id, sub);
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
            removals[tp.topic].insert_or_assign(tp.partition, p_it->second);
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
