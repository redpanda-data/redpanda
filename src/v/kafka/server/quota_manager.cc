// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/quota_manager.h"

#include "base/vassert.h"
#include "base/vlog.h"
#include "config/configuration.h"
#include "container/fragmented_vector.h"
#include "kafka/server/atomic_token_bucket.h"
#include "kafka/server/client_quota_translator.h"
#include "kafka/server/logger.h"
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"
#include "ssx/async_algorithm.h"
#include "ssx/future-util.h"
#include "utils/log_hist.h"

#include <seastar/core/future.hh>
#include <seastar/core/map_reduce.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/later.hh>

#include <fmt/chrono.h>

#include <chrono>
#include <memory>
#include <optional>
#include <string_view>
#include <variant>

using namespace std::chrono_literals;

static const unsigned quotas_shard = 0;

namespace kafka {

class quota_manager::client_quotas_probe {
public:
    client_quotas_probe() = default;
    client_quotas_probe(const client_quotas_probe&) = delete;
    client_quotas_probe& operator=(const client_quotas_probe&) = delete;
    client_quotas_probe(client_quotas_probe&&) = delete;
    client_quotas_probe& operator=(client_quotas_probe&&) = delete;
    ~client_quotas_probe() noexcept = default;

    void setup_metrics() {
        namespace sm = ss::metrics;

        auto metric_defs = std::vector<ss::metrics::metric_definition>{};
        metric_defs.reserve(
          all_client_quota_types.size() * all_client_quota_rules.size() * 2);

        auto rule_label = metrics::make_namespaced_label("quota_rule");
        auto quota_type_label = metrics::make_namespaced_label("quota_type");

        for (auto quota_type : all_client_quota_types) {
            for (auto rule : all_client_quota_rules) {
                metric_defs.emplace_back(
                  sm::make_histogram(
                    "client_quota_throttle_time",
                    [this, rule, quota_type] {
                        return get_throttle_time(rule, quota_type);
                    },
                    sm::description(
                      "Client quota throttling delay per rule and "
                      "quota type (in seconds)"),
                    {rule_label(rule), quota_type_label(quota_type)})
                    .aggregate({sm::shard_label}));
                metric_defs.emplace_back(
                  sm::make_histogram(
                    "client_quota_throughput",
                    [this, rule, quota_type] {
                        return get_throughput(rule, quota_type);
                    },
                    sm::description(
                      "Client quota throughput per rule and quota type"),
                    {rule_label(rule), quota_type_label(quota_type)})
                    .aggregate({sm::shard_label}));
            }
        }

        auto group_name = prometheus_sanitize::metrics_name("kafka:quotas");
        if (!config::shard_local_cfg().disable_metrics()) {
            _internal_metrics.add_group(group_name, metric_defs);
        }
        if (!config::shard_local_cfg().disable_public_metrics()) {
            _public_metrics.add_group(group_name, metric_defs);
        }
    }

    void record_throttle_time(
      client_quota_rule rule, client_quota_type qt, clock::duration t) {
        auto& gm = (*this)(rule, qt);
        // The Kafka response field `ThrottleTimeMs` is in milliseconds, so
        // round the metric to milliseconds as well.
        auto t_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t);
        gm.throttle_time.record(t_ms);
    }
    void record_throughput(
      client_quota_rule rule, client_quota_type qt, uint64_t count) {
        auto& gm = (*this)(rule, qt);
        gm.throughput.record(count);
    }

private:
    ss::metrics::histogram
    get_throttle_time(client_quota_rule rule, client_quota_type qt) const {
        auto& gm = (*this)(rule, qt);
        return gm.throttle_time.client_quota_histogram_logform();
    }
    ss::metrics::histogram
    get_throughput(client_quota_rule rule, client_quota_type qt) const {
        auto& gm = (*this)(rule, qt);
        return gm.throughput.internal_histogram_logform();
    }

    struct granular_metrics {
        log_hist_client_quota throttle_time;
        log_hist_internal throughput;
    };

    granular_metrics&
    operator()(client_quota_rule rule, client_quota_type type) {
        return _metrics[static_cast<size_t>(rule)][static_cast<size_t>(type)];
    }

    const granular_metrics&
    operator()(client_quota_rule rule, client_quota_type type) const {
        return _metrics[static_cast<size_t>(rule)][static_cast<size_t>(type)];
    }

    // Assume the enums values are in sequence: [0, all_*.size())
    static_assert(static_cast<size_t>(all_client_quota_rules[0]) == 0);
    static_assert(static_cast<size_t>(all_client_quota_types[0]) == 0);
    using metrics_container_t = std::array<
      std::array<granular_metrics, all_client_quota_types.size()>,
      all_client_quota_rules.size()>;

    metrics::internal_metric_groups _internal_metrics;
    metrics::public_metric_groups _public_metrics;
    metrics_container_t _metrics{};
};

using clock = quota_manager::clock;

quota_manager::quota_manager(
  ss::sharded<cluster::client_quota::store>& client_quota_store)
  : _default_num_windows(config::shard_local_cfg().default_num_windows.bind())
  , _default_window_width(config::shard_local_cfg().default_window_sec.bind())
  , _replenish_threshold(
      config::shard_local_cfg().kafka_throughput_replenish_threshold.bind())
  , _translator{client_quota_store}
  , _gc_freq(config::shard_local_cfg().quota_manager_gc_sec())
  , _max_delay(config::shard_local_cfg().max_kafka_throttle_delay_ms.bind()) {
    if (seastar::this_shard_id() == quotas_shard) {
        _global_map = global_map_t{};

        _gc_timer.set_callback([this]() { gc(); });
        _global_map_mutex = mutex{"quota_manager/global_map"};
    }
}

quota_manager::~quota_manager() { _gc_timer.cancel(); }

ss::future<> quota_manager::stop() {
    _gc_timer.cancel();
    co_await _gate.close();
    _probe.reset();
}

ss::future<> quota_manager::start() {
    _probe = std::make_unique<client_quotas_probe>();
    _probe->setup_metrics();
    if (ss::this_shard_id() == quotas_shard) {
        _gc_timer.arm_periodic(_gc_freq);

        auto update_quotas = [this]() { update_client_quotas(); };
        _translator.watch(update_quotas);
    }
    return ss::now();
}

ss::future<clock::duration> quota_manager::maybe_add_and_retrieve_quota(
  tracker_key qid, clock::time_point now, quota_mutation_callback_t cb) {
    auto it = _local_map.find(qid);
    if (it == _local_map.end()) {
        auto new_q = co_await container().invoke_on(
          quotas_shard, [qid, now](quota_manager& me) mutable {
              return me.add_quota_id(std::move(qid), now);
          });

        vlog(
          client_quota_log.trace, "Inserting new key into local map: {}", qid);
        it = _local_map.emplace(qid, std::move(new_q)).first;
    } else {
        // bump to prevent gc
        it->second->last_seen_ms.local() = now;
    }

    co_return cb(*it->second);
}

ss::future<std::shared_ptr<quota_manager::client_quota>>
quota_manager::add_quota_id(tracker_key qid, clock::time_point now) {
    vassert(
      ss::this_shard_id() == quotas_shard,
      "add_quota_id should only be called on the owner shard");

    // Hold to lock to not distrupt background fibres (gc and update_quotas)
    // with new entries in the map
    auto lock = co_await _global_map_mutex->get_units();

    auto existing_quota = _global_map->find(qid);
    if (existing_quota != _global_map->end()) {
        co_return existing_quota->second;
    }

    vlog(client_quota_log.trace, "Inserting new key into global map: {}", qid);

    auto limits = _translator.find_quota_value(qid);
    auto replenish_threshold = static_cast<uint64_t>(
      _replenish_threshold().value_or(1));

    auto new_value = std::make_shared<client_quota>(
      ssx::sharded_value<clock::time_point>(now),
      std::nullopt,
      std::nullopt,
      std::nullopt);

    if (limits.produce_limit.has_value()) {
        new_value->tp_produce_rate.emplace(
          *limits.produce_limit,
          *limits.produce_limit,
          replenish_threshold,
          true);
    }
    if (limits.fetch_limit.has_value()) {
        new_value->tp_fetch_rate.emplace(
          *limits.fetch_limit, *limits.fetch_limit, replenish_threshold, true);
    }
    if (limits.partition_mutation_limit.has_value()) {
        new_value->pm_rate.emplace(
          *limits.partition_mutation_limit,
          *limits.partition_mutation_limit,
          replenish_threshold,
          true);
    }

    auto [it, _] = _global_map->emplace(qid, std::move(new_value));

    co_return it->second;
}

void quota_manager::update_client_quotas() {
    vassert(
      ss::this_shard_id() == quotas_shard,
      "update_client_quotas must only be called on the owner shard");

    // Hold to lock to ensure there are no updates to the map while iterating
    ssx::spawn_with_gate(_gate, [this] {
        return _global_map_mutex->with(
          [this] { return do_update_client_quotas(); });
    });
}

ss::future<> quota_manager::do_update_client_quotas() {
    constexpr auto set_bucket = [](
                                  std::optional<atomic_token_bucket>& bucket,
                                  std::optional<uint64_t> rate,
                                  std::optional<uint64_t> replenish_threshold) {
        if (!rate) {
            bucket.reset();
            return;
        }

        if (bucket.has_value() && bucket->rate() == rate) {
            return;
        }
        bucket.emplace(*rate, *rate, replenish_threshold.value_or(1), true);
        return;
    };

    return ssx::async_for_each(
      _global_map->begin(),
      _global_map->end(),
      [this, &set_bucket](auto& quota) {
          auto limits = _translator.find_quota_value(quota.first);
          set_bucket(
            quota.second->tp_produce_rate,
            limits.produce_limit,
            _replenish_threshold());

          set_bucket(
            quota.second->tp_fetch_rate,
            limits.fetch_limit,
            _replenish_threshold());

          set_bucket(
            quota.second->pm_rate,
            limits.partition_mutation_limit,
            _replenish_threshold());
      });
}

ss::future<std::chrono::milliseconds> quota_manager::record_partition_mutations(
  std::optional<std::string_view> client_id,
  uint32_t mutations,
  clock::time_point now) {
    /// KIP-599 throttles create_topics / delete_topics / create_partitions
    /// request. This delay should only be applied to these requests if the
    /// quota has been exceeded
    if (_translator.is_empty()) {
        co_return 0ms;
    }
    auto ctx = client_quota_request_ctx{
      .q_type = client_quota_type::partition_mutation_quota,
      .client_id = client_id,
    };
    auto [key, value] = _translator.find_quota(ctx);
    _probe->record_throughput(value.rule, ctx.q_type, mutations);
    if (!value.limit) {
        _probe->record_throttle_time(value.rule, ctx.q_type, 0ms);
        vlog(
          client_quota_log.trace,
          "request: ctx:{}, key:{}, value:{}, mutations: {}, delay:{}"
          "capped_delay:{}",
          ctx,
          key,
          value,
          mutations,
          0ms,
          0ms);
        co_return 0ms;
    }

    auto delay = co_await maybe_add_and_retrieve_quota(
      key, now, [now, mutations](quota_manager::client_quota& cq) {
          if (!cq.pm_rate.has_value()) {
              return clock::duration::zero();
          }
          auto& pm_rate_tracker = cq.pm_rate.value();
          auto result
            = pm_rate_tracker.update_and_calculate_delay<clock::duration>(
              now, 0);
          pm_rate_tracker.record(mutations);
          return result;
      });

    auto capped_delay = cap_to_max_delay(key, delay);
    _probe->record_throttle_time(value.rule, ctx.q_type, capped_delay);
    vlog(
      client_quota_log.trace,
      "request: ctx:{}, key:{}, value:{}, mutations: {}, delay:{}"
      "capped_delay:{}",
      ctx,
      key,
      value,
      mutations,
      delay,
      capped_delay);
    co_return duration_cast<std::chrono::milliseconds>(capped_delay);
}

clock::duration quota_manager::cap_to_max_delay(
  const tracker_key& quota_id, clock::duration delay) {
    std::chrono::milliseconds max_delay_ms(_max_delay());
    std::chrono::milliseconds delay_ms
      = std::chrono::duration_cast<std::chrono::milliseconds>(delay);
    if (delay_ms > max_delay_ms) {
        vlog(
          klog.info,
          "Client:{}, Estimated backpressure delay of {}, limiting to {}",
          quota_id,
          delay_ms,
          max_delay_ms);
        delay = max_delay_ms;
    }
    return delay;
}

// record a new observation and return <previous delay, new delay>
ss::future<clock::duration> quota_manager::record_produce_tp_and_throttle(
  std::optional<std::string_view> client_id,
  uint64_t bytes,
  clock::time_point now) {
    if (_translator.is_empty()) {
        co_return 0ms;
    }
    auto ctx = client_quota_request_ctx{
      .q_type = client_quota_type::produce_quota,
      .client_id = client_id,
    };
    auto [key, value] = _translator.find_quota(ctx);
    _probe->record_throughput(value.rule, ctx.q_type, bytes);
    if (!value.limit) {
        _probe->record_throttle_time(value.rule, ctx.q_type, 0ms);
        vlog(
          client_quota_log.trace,
          "request: ctx:{}, key:{}, value:{}, bytes: {}, delay:{}, "
          "capped_delay:{}",
          ctx,
          key,
          value,
          bytes,
          0ms,
          0ms);
        co_return clock::duration::zero();
    }
    auto delay = co_await maybe_add_and_retrieve_quota(
      key, now, [now, bytes](quota_manager::client_quota& cq) {
          if (!cq.tp_produce_rate.has_value()) {
              return clock::duration::zero();
          }
          auto& produce_tracker = cq.tp_produce_rate.value();
          return produce_tracker.update_and_calculate_delay<clock::duration>(
            now, bytes);
      });

    auto capped_delay = cap_to_max_delay(key, delay);
    _probe->record_throttle_time(value.rule, ctx.q_type, capped_delay);
    vlog(
      client_quota_log.trace,
      "request: ctx:{}, key:{}, value:{}, bytes: {}, delay:{}, "
      "capped_delay:{}",
      ctx,
      key,
      value,
      bytes,
      delay,
      capped_delay);
    co_return capped_delay;
}

ss::future<> quota_manager::record_fetch_tp(
  std::optional<std::string_view> client_id,
  uint64_t bytes,
  clock::time_point now) {
    if (_translator.is_empty()) {
        co_return;
    }
    auto ctx = client_quota_request_ctx{
      .q_type = client_quota_type::fetch_quota,
      .client_id = client_id,
    };
    auto [key, value] = _translator.find_quota(ctx);
    _probe->record_throughput(value.rule, ctx.q_type, bytes);
    vlog(
      client_quota_log.trace,
      "record request: ctx:{}, key:{}, value:{}, bytes:{}",
      ctx,
      key,
      value,
      bytes);
    if (!value.limit) {
        co_return;
    }
    auto delay [[maybe_unused]] = co_await maybe_add_and_retrieve_quota(
      key, now, [bytes](quota_manager::client_quota& cq) {
          if (!cq.tp_fetch_rate.has_value()) {
              return clock::duration::zero();
          }
          auto& fetch_tracker = cq.tp_fetch_rate.value();
          fetch_tracker.record(bytes);
          return clock::duration::zero();
      });
}

ss::future<clock::duration> quota_manager::throttle_fetch_tp(
  std::optional<std::string_view> client_id, clock::time_point now) {
    if (_translator.is_empty()) {
        co_return 0ms;
    }
    auto ctx = client_quota_request_ctx{
      .q_type = client_quota_type::fetch_quota,
      .client_id = client_id,
    };
    auto [key, value] = _translator.find_quota(ctx);
    if (!value.limit) {
        _probe->record_throttle_time(value.rule, ctx.q_type, 0ms);
        vlog(
          client_quota_log.trace,
          "throttle request: ctx:{}, key:{}, value:{}, delay:{}, "
          "capped_delay:{}",
          ctx,
          key,
          value,
          0ms,
          0ms);
        co_return clock::duration::zero();
    }

    auto delay = co_await maybe_add_and_retrieve_quota(
      key, now, [now](quota_manager::client_quota& cq) {
          if (!cq.tp_fetch_rate.has_value()) {
              return clock::duration::zero();
          }
          auto& fetch_tracker = cq.tp_fetch_rate.value();
          return fetch_tracker.update_and_calculate_delay<clock::duration>(now);
      });

    auto capped_delay = cap_to_max_delay(key, delay);
    _probe->record_throttle_time(value.rule, ctx.q_type, capped_delay);
    vlog(
      client_quota_log.trace,
      "throttle request: ctx:{}, key:{}, value:{}, delay:{}, "
      "capped_delay:{}",
      ctx,
      key,
      value,
      delay,
      capped_delay);
    co_return capped_delay;
}

const std::optional<quota_manager::global_map_t>&
quota_manager::get_global_map_for_testing() const {
    return _global_map;
}

// erase inactive tracked quotas. windows are considered inactive if
// they have not received any updates in ten window's worth of time.
void quota_manager::gc() {
    vassert(
      ss::this_shard_id() == quotas_shard,
      "gc should only be performed on the owner shard");
    auto full_window = _default_num_windows() * _default_window_width();
    auto expire_threshold = clock::now() - 10 * full_window;
    ssx::background
      = ssx::spawn_with_gate_then(_gate, [this, expire_threshold]() {
            return container()
              .invoke_on_all([expire_threshold](quota_manager& qm) {
                  return qm.do_local_gc(expire_threshold);
              })
              .then([this]() { return do_global_gc(); });
        }).handle_exception([](const std::exception_ptr& e) {
            vlog(klog.warn, "Error garbage collecting quotas - {}", e);
        });
}

ss::future<> quota_manager::do_global_gc() {
    vassert(
      ss::this_shard_id() == quotas_shard,
      "do_global_gc() should only be called on the owner shard");

    // Hold to lock to ensure there are no updates to the map while iterating
    auto lock = co_await _global_map_mutex->get_units();

    for (auto it = _global_map->begin(); it != _global_map->end();) {
        auto& [key, value] = *it;
        if (value.use_count() == 1) {
            // The pointer in the global map is effectively a weak pointer in
            // that we want to destroy the quota when only the global map holds
            // a reference to it. The reason why we have a std::shared_ptr<>
            // instead of a std::weak_ptr<> in the global map is to ensure that
            // deallocation happens on shard 0 (here in do_global_gc).
            vlog(client_quota_log.trace, "Global GC expiring key: {}", key);
            it = _global_map->erase(it);
        } else {
            ++it;
        }

        co_await ss::coroutine::maybe_yield();
    }
}

ss::future<> quota_manager::do_local_gc(clock::time_point expire_threshold) {
    for (auto it = _local_map.begin(); it != _local_map.end();) {
        auto& [key, value] = *it;
        if (value->last_seen_ms.local() < expire_threshold) {
            vlog(client_quota_log.trace, "Local GC expiring key: {}", key);
            it = _local_map.erase(it);
        } else {
            ++it;
        }

        co_await ss::coroutine::maybe_yield();
    }
}

} // namespace kafka
