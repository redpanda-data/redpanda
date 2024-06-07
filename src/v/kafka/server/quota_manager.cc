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
#include "prometheus/prometheus_sanitize.h"
#include "ssx/future-util.h"
#include "utils/log_hist.h"

#include <seastar/core/future.hh>
#include <seastar/core/map_reduce.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/smp.hh>

#include <fmt/chrono.h>

#include <chrono>
#include <memory>
#include <optional>
#include <string_view>
#include <variant>

using namespace std::chrono_literals;

namespace kafka {

template<typename clock>
class client_quotas_probe {
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
  client_quotas_t& client_quotas,
  ss::sharded<cluster::client_quota::store>& client_quota_store)
  : _default_num_windows(config::shard_local_cfg().default_num_windows.bind())
  , _default_window_width(config::shard_local_cfg().default_window_sec.bind())
  , _replenish_threshold(
      config::shard_local_cfg().kafka_throughput_replenish_threshold.bind())
  , _client_quotas{client_quotas}
  , _translator{client_quota_store}
  , _gc_freq(config::shard_local_cfg().quota_manager_gc_sec())
  , _max_delay(config::shard_local_cfg().max_kafka_throttle_delay_ms.bind()) {
    if (seastar::this_shard_id() == _client_quotas.shard_id()) {
        _gc_timer.set_callback([this]() { gc(); });
        auto update_quotas = [this]() { update_client_quotas(); };
        _translator.watch(update_quotas);
    }
}

quota_manager::~quota_manager() { _gc_timer.cancel(); }

ss::future<> quota_manager::stop() {
    _gc_timer.cancel();
    co_await _gate.close();
    _probe.reset();
}

ss::future<> quota_manager::start() {
    _probe = std::make_unique<client_quotas_probe<clock>>();
    _probe->setup_metrics();
    if (ss::this_shard_id() == _client_quotas.shard_id()) {
        co_await _client_quotas.reset(client_quotas_map_t{});
        _gc_timer.arm_periodic(_gc_freq);
    }
}

ss::future<clock::duration> quota_manager::maybe_add_and_retrieve_quota(
  tracker_key qid, clock::time_point now, quota_mutation_callback_t cb) {
    vassert(_client_quotas, "_client_quotas should have been initialized");

    auto it = _client_quotas->find(qid);
    if (it == _client_quotas->end()) {
        co_await container().invoke_on(
          _client_quotas.shard_id(), [qid, now](quota_manager& me) mutable {
              return me.add_quota_id(std::move(qid), now);
          });

        it = _client_quotas->find(qid);
        if (it == _client_quotas->end()) {
            // The newly inserted quota map entry should always be available
            // here because the update to the map is guarded by a mutex, so
            // there is no chance of losing updates. There is a low chance
            // that we insert into the map, then there is an unusually long
            // pause on the handler core, gc() gets scheduled and cleans up
            // the inserted quota, and by the time the handler gets here the
            // inserted entry is gone. In that case, we give up and don't
            // throttle.
            vlog(klog.debug, "Failed to find quota id after insert...");
            co_return clock::duration::zero();
        }
    } else {
        // bump to prevent gc
        it->second->last_seen_ms.local() = now;
    }

    co_return cb(*it->second);
}

ss::future<>
quota_manager::add_quota_id(tracker_key qid, clock::time_point now) {
    vassert(
      ss::this_shard_id() == _client_quotas.shard_id(),
      "add_quota_id should only be called on the owner shard");

    auto update_func = [this, qid = std::move(qid), now](
                         client_quotas_map_t new_map) -> client_quotas_map_t {
        auto limits = _translator.find_quota_value(qid);
        auto replenish_threshold = static_cast<uint64_t>(
          _replenish_threshold().value_or(1));

        auto new_value = ss::make_lw_shared<client_quota>(
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
              *limits.fetch_limit,
              *limits.fetch_limit,
              replenish_threshold,
              true);
        }
        if (limits.partition_mutation_limit.has_value()) {
            new_value->pm_rate.emplace(
              *limits.partition_mutation_limit,
              *limits.partition_mutation_limit,
              replenish_threshold,
              true);
        }

        new_map.emplace(qid, std::move(new_value));

        return new_map;
    };

    co_await _client_quotas.update(std::move(update_func));
}

void quota_manager::update_client_quotas() {
    vassert(
      ss::this_shard_id() == _client_quotas.shard_id(),
      "update_client_quotas must only be called on the owner shard");

    ssx::spawn_with_gate(_gate, [this] {
        return _client_quotas.update([this](client_quotas_map_t quotas) {
            constexpr auto set_bucket =
              [](
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
                  bucket.emplace(
                    *rate, *rate, replenish_threshold.value_or(1), true);
              };

            for (auto& quota : quotas) {
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
            }

            return quotas;
        });
    });
}

ss::future<std::chrono::milliseconds> quota_manager::record_partition_mutations(
  std::optional<std::string_view> client_id,
  uint32_t mutations,
  clock::time_point now) {
    /// KIP-599 throttles create_topics / delete_topics / create_partitions
    /// request. This delay should only be applied to these requests if the
    /// quota has been exceeded
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

// erase inactive tracked quotas. windows are considered inactive if
// they have not received any updates in ten window's worth of time.
void quota_manager::gc() {
    vassert(
      ss::this_shard_id() == _client_quotas.shard_id(),
      "gc should only be performed on the owner shard");
    auto full_window = _default_num_windows() * _default_window_width();
    auto expire_threshold = clock::now() - 10 * full_window;
    ssx::background
      = ssx::spawn_with_gate_then(_gate, [this, expire_threshold]() {
            return do_gc(expire_threshold);
        }).handle_exception([](const std::exception_ptr& e) {
            vlog(klog.warn, "Error garbage collecting quotas - {}", e);
        });
}

namespace {

/// A simplified copy paste of `std::set_intersection`. Copied here because we
/// rely on the fact that we're allowed to have first1 == result for in-place
/// set intersection in `do_gc()` which the contract of `std::set_intersection`
/// does not allow despite that its implementation does.
/// Requires: the inputs to be sorted
/// Guarantees: the output to be sorted
template<typename It1, typename It2, typename ItR>
ItR set_intersection(It1 first1, It1 last1, It2 first2, It2 last2, ItR result) {
    while (first1 != last1 && first2 != last2) {
        if (*first1 < *first2) {
            ++first1;
        } else if (*first2 < *first1) {
            ++first2;
        } else { // *first1 == *first2
            *result = *first1;
            ++first1;
            ++first2;
        }
    }
    return result;
}

} // namespace

ss::future<> quota_manager::do_gc(clock::time_point expire_threshold) {
    vassert(
      ss::this_shard_id() == _client_quotas.shard_id(),
      "do_gc() should only be called on the owner shard");

    using key_set = chunked_vector<tracker_key>;

    auto mapper = [expire_threshold](const quota_manager& qm) -> key_set {
        auto res = key_set{};
        auto map_shared_ptr = qm._client_quotas.local().get();
        for (const auto& kv : *map_shared_ptr) {
            auto last_seen_tp = kv.second->last_seen_ms.local();
            if (last_seen_tp < expire_threshold) {
                res.push_back(kv.first);
            }
        }
        // Note: need to pre-sort the vector for the std::set_intersection
        // in the reduce step
        std::sort(res.begin(), res.end());
        return res;
    };

    auto reducer = [](key_set acc, const key_set& next) -> key_set {
        // In-place set intersection assumes that the inputs are sorted and
        // guarantees that the output is also sorted
        auto it = set_intersection(
          acc.begin(), acc.end(), next.begin(), next.end(), acc.begin());
        acc.erase_to_end(it);
        return acc;
    };

    auto expired_keys = co_await container().map_reduce0(
      mapper, key_set{}, reducer);

    if (expired_keys.empty()) {
        // Nothing to gc, so we're done
        co_return;
    }

    // Note: it is possible that we remove client ids here that we have not
    // seen for a long time before the map_reduce step, but that we see
    // again between the map_reduce step and this map update. This race
    // between the two steps can cause recently seen client ids to be
    // deleted. That's acceptable because this should be rare and the client
    // will be tracked correctly again from when we next see it.
    co_await _client_quotas.update(
      [expired_keys{std::move(expired_keys)}](client_quotas_map_t new_map) {
          for (auto& k : expired_keys) {
              new_map.erase(k);
          }
          return new_map;
      });
}

} // namespace kafka
