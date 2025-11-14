/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "kafka/server/datalake_throttle_manager.h"

#include "config/configuration.h"
#include "kafka/server/logger.h"
#include "metrics/prometheus_sanitize.h"
#include "ssx/future-util.h"
#include "utils/human.h"

#include <boost/range/irange.hpp>
namespace kafka {
using namespace std::chrono_literals;
namespace {

static constexpr std::string_view anonymous_client_id = "";

static constexpr std::chrono::milliseconds no_throttling{0};
std::string_view
get_effective_client_id(const std::optional<std::string_view>& client_id) {
    if (client_id.has_value()) {
        return client_id.value();
    }
    return anonymous_client_id;
}

} // namespace
void datalake_throttle_manager::merge_producer_maps(
  producers_map_t& target, const producers_map_t& source) {
    for (const auto& [client_id, state] : source) {
        auto [it, emplaced] = target.try_emplace(client_id, state);
        if (!emplaced) {
            it->second.last_datalake_produce = std::max(
              it->second.last_datalake_produce, state.last_datalake_produce);
        }
    }
}

datalake_throttle_manager::status datalake_throttle_manager::status::operator+(
  const datalake_throttle_manager::status& o) const {
    return datalake_throttle_manager::status{
      .max_shares_assigned = max_shares_assigned || o.max_shares_assigned,
      .total_translation_backlog = total_translation_backlog
                                   + o.total_translation_backlog};
}

bool datalake_throttle_manager::needs_throttling() const {
    // check if disk space info is already available
    if (!_disk_space_info) [[unlikely]] {
        return false;
    }
    auto ratio = _backlog_size_throttling_ratio();
    if (!ratio.has_value()) {
        return false;
    }
    return _translation_status.max_shares_assigned
           && _translation_status.total_translation_backlog
                > ratio.value() * _disk_space_info->total;
}

std::ostream&
operator<<(std::ostream& o, const datalake_throttle_manager::status& s) {
    fmt::print(
      o,
      "{{max_shares_assigned: {}, total_translation_backlog: {}}}",
      s.max_shares_assigned,
      human::bytes(s.total_translation_backlog));
    return o;
}

datalake_throttle_manager::datalake_throttle_manager(
  status_provider_fn status_provider,
  ss::sharded<storage::node>& storage_node,
  config::binding<std::chrono::milliseconds> producer_gc_threshold,
  config::binding<std::chrono::milliseconds> max_kafka_throttle,
  config::binding<std::optional<double>> backlog_size_throttling_ratio)
  : _shard_status_provider(std::move(status_provider))
  , _storage_node(storage_node)
  , _producer_gc_threshold(std::move(producer_gc_threshold))
  , _max_kafka_throttle(std::move(max_kafka_throttle))
  , _backlog_size_throttling_ratio(std::move(backlog_size_throttling_ratio)) {
    _update_timer.set_callback([this] {
        ssx::spawn_with_gate(
          _gate, [this] { return gc_and_update_global_producers_map(); });
    });
}

void datalake_throttle_manager::start() {
    using namespace std::chrono_literals;
    setup_metrics();
    /**
     * Only run update timer on shard 0
     */
    if (ss::this_shard_id() == 0) {
        _update_timer.arm_periodic(100ms);
        // storage node is only available on shard 0
        _storage_node_notification
          = _storage_node.local().register_disk_notification(
            storage::node::disk_type::data,
            [this](storage::node::disk_space_info dsi) {
                _disk_space_info = dsi;
            });
    }
}

ss::future<> datalake_throttle_manager::stop() {
    if (ss::this_shard_id() == 0) {
        _storage_node.local().unregister_disk_notification(
          storage::node::disk_type::data, _storage_node_notification);
    }
    _update_timer.cancel();
    return _gate.close();
}

void datalake_throttle_manager::mark_datalake_producer(
  const std::optional<std::string_view>& client_id,
  clock_type::time_point now) {
    _shard_local_producers.insert_or_assign(
      ss::sstring(get_effective_client_id(client_id)),
      producer_state{.last_datalake_produce = now});
}

ss::future<> datalake_throttle_manager::gc_and_update_global_producers_map() {
    vassert(
      ss::this_shard_id() == 0,
      "Global producers map should be updated only on shard 0");

    _translation_status = co_await container().map_reduce0(
      [](datalake_throttle_manager& instance) {
          return instance._shard_status_provider();
      },
      status{},
      [](status acc, status shard_status) { return acc + shard_status; });

    if (!needs_throttling()) {
        _last_no_issues_timestamp = clock_type::now();
    }
    /*
     * Info level log if throttling may be applied, this should be a rare
     * condition.
     */
    if (needs_throttling()) [[unlikely]] {
        vlog(
          client_quota_log.info,
          "Translation status updated: {}, throttling may be applied.",
          _translation_status);
    }

    /**
     * Collect shard local maps and while iterating over the shards update the
     * total backlog
     */
    auto shard_local_maps = co_await ssx::parallel_transform(
      boost::irange(ss::smp::count), [this](auto shard_id) {
          return container().invoke_on(
            shard_id,
            [status = _translation_status, disk_space_info = _disk_space_info](
              datalake_throttle_manager& other) {
                other._translation_status = status;
                other._disk_space_info = disk_space_info;
                return std::exchange(other._shard_local_producers, {});
            });
      });

    for (auto& shard_map : shard_local_maps) {
        merge_producer_maps(_global_producers, shard_map);
    }

    std::erase_if(_global_producers, [this](const auto& p) {
        return (clock_type::now() - p.second.last_datalake_produce)
               > _producer_gc_threshold();
    });
}

ss::future<std::chrono::milliseconds>
datalake_throttle_manager::maybe_throttle_producer(
  std::optional<std::string_view> client_id) {
    // fast path, backlog is below the threshold

    if (!needs_throttling()) [[likely]] {
        co_return no_throttling;
    }

    auto throttle_ms = co_await container().invoke_on(
      0, [&client_id](datalake_throttle_manager& shard_0_manager) {
          auto throttle_ms = shard_0_manager.get_producer_throttle(client_id);
          if (throttle_ms > 0ms) {
              vlog(
                client_quota_log.debug,
                "Throttling producer {} for {}ms, current "
                "status: {}, disk total space: {}, translation backlog "
                "throttling threshold: {}",
                client_id,
                throttle_ms,
                shard_0_manager._translation_status,
                human::bytes(shard_0_manager._disk_space_info->total),
                human::bytes(
                  shard_0_manager._backlog_size_throttling_ratio().value_or(0)
                  * shard_0_manager._disk_space_info->total));
          }
          return throttle_ms;
      });
    // record throttle for exposing a metric
    _total_throttle += throttle_ms.count();
    if (throttle_ms > 0ms) {
        _throttled_requests++;
    }
    co_return throttle_ms;
}

std::chrono::milliseconds datalake_throttle_manager::get_producer_throttle(
  const std::optional<std::string_view>& client_id) {
    vassert(
      ss::this_shard_id() == 0, "Throttle can only be calculate on shard 0");
    if (!needs_throttling()) [[likely]] {
        return no_throttling;
    }
    auto effective_client_id = get_effective_client_id(client_id);

    auto it = _global_producers.find(effective_client_id);
    // do not throttle non datalake producers
    if (it == _global_producers.end()) {
        return no_throttling;
    }

    return calculate_throttle();
}

std::chrono::milliseconds
datalake_throttle_manager::calculate_throttle() const {
    vassert(
      ss::this_shard_id() == 0, "Throttle can only be calculate on shard 0");

    if (!needs_throttling()) {
        return no_throttling;
    }
    // Heuristic:
    // we apply the incremental throttle based on time spent in the degraded
    // translation state, the longer the time the more we throttle. The
    // throttle is set to max after 50 minutes
    auto time_in_degraded_state
      = std::chrono::duration_cast<std::chrono::milliseconds>(
        ss::lowres_clock::now() - _last_no_issues_timestamp);

    static constexpr auto max_throttle_time = std::chrono::milliseconds(50min);
    const auto max_throttle_ratio = std::min(
      static_cast<double>(time_in_degraded_state.count())
        / max_throttle_time.count(),
      1.0);

    return std::chrono::milliseconds(
      static_cast<size_t>(max_throttle_ratio * _max_kafka_throttle().count()));
}

void datalake_throttle_manager::setup_metrics() {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }
    namespace sm = ss::metrics;
    _metrics.add_group(
      prometheus_sanitize::metrics_name("kafka:datalake:throttle"),
      {
        sm::make_counter(
          "total_throttle",
          [this] { return _total_throttle; },
          sm::description(
            "Total datalake producer throttle time in milliseconds")),
        sm::make_counter(
          "throttled_requests",
          [this] { return _throttled_requests; },
          sm::description("Number of requests throttled")),
      });
}

} // namespace kafka
