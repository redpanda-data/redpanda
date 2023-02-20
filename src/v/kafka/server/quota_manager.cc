// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/quota_manager.h"

#include "config/configuration.h"
#include "kafka/server/logger.h"
#include "vlog.h"

#include <fmt/chrono.h>

#include <chrono>

using namespace std::chrono_literals;

namespace kafka {
using clock = quota_manager::clock;
using throttle_delay = quota_manager::throttle_delay;

quota_manager::quota_manager()
  : _default_num_windows(config::shard_local_cfg().default_num_windows.bind())
  , _default_window_width(config::shard_local_cfg().default_window_sec.bind())
  , _default_target_produce_tp_rate(
      config::shard_local_cfg().target_quota_byte_rate.bind())
  , _default_target_fetch_tp_rate(
      config::shard_local_cfg().target_fetch_quota_byte_rate.bind())
  , _target_partition_mutation_quota(
      config::shard_local_cfg().kafka_admin_topic_api_rate.bind())
  , _target_produce_tp_rate_per_client_group(
      config::shard_local_cfg().kafka_client_group_byte_rate_quota.bind())
  , _target_fetch_tp_rate_per_client_group(
      config::shard_local_cfg().kafka_client_group_fetch_byte_rate_quota.bind())
  , _gc_freq(config::shard_local_cfg().quota_manager_gc_sec())
  , _max_delay(config::shard_local_cfg().max_kafka_throttle_delay_ms.bind()) {
    _gc_timer.set_callback([this] {
        auto full_window = _default_num_windows() * _default_window_width();
        gc(full_window);
    });
}

quota_manager::~quota_manager() { _gc_timer.cancel(); }

ss::future<> quota_manager::stop() {
    _gc_timer.cancel();
    return ss::make_ready_future<>();
}

ss::future<> quota_manager::start() {
    _gc_timer.arm_periodic(_gc_freq);
    return ss::make_ready_future<>();
}

quota_manager::client_quotas_t::iterator
quota_manager::maybe_add_and_retrieve_quota(
  const std::optional<std::string_view>& quota_id,
  const clock::time_point& now) {
    // requests without a client id are grouped into an anonymous group that
    // shares a default quota. the anonymous group is keyed on empty string.
    auto qid = quota_id ? *quota_id : "";

    // find or create the throughput tracker for this client
    //
    // c++20: heterogeneous lookup for unordered_map can avoid creation of
    // an sstring here but std::unordered_map::find isn't using an
    // equal_to<> overload. this is a general issue we'll be looking at. for
    // now, these client-name strings are small. This will be solved in
    // c++20 via Hash::transparent_key_equal.
    auto [it, inserted] = _client_quotas.try_emplace(
      ss::sstring(qid),
      client_quota{
        now,
        clock::duration(0),
        {static_cast<size_t>(_default_num_windows()), _default_window_width()},
        {static_cast<size_t>(_default_num_windows()), _default_window_width()},
        /// pm_rate is only non-nullopt on the qm home shard
        (ss::this_shard_id() == quota_manager_shard)
          ? std::optional<token_bucket_rate_tracker>(
            {*_target_partition_mutation_quota(),
             static_cast<uint32_t>(_default_num_windows()),
             _default_window_width()})
          : std::optional<token_bucket_rate_tracker>()});

    // bump to prevent gc
    if (!inserted) {
        it->second.last_seen = now;
    }

    return it;
}

// If client is part of some group then client quota ID is a group
// else client quota ID is client_id
static std::optional<std::string_view> get_client_quota_id(
  const std::optional<std::string_view>& client_id,
  const std::unordered_map<ss::sstring, config::client_group_quota>&
    group_quota) {
    if (!client_id) {
        return std::nullopt;
    }
    for (const auto& group_and_limit : group_quota) {
        if (client_id->starts_with(
              std::string_view(group_and_limit.second.clients_prefix))) {
            return group_and_limit.first;
        }
    }
    return client_id;
}

int64_t quota_manager::get_client_target_produce_tp_rate(
  const std::optional<std::string_view>& quota_id) {
    if (!quota_id) {
        return _default_target_produce_tp_rate();
    }
    auto group_tp_rate = _target_produce_tp_rate_per_client_group().find(
      ss::sstring(quota_id.value()));
    if (group_tp_rate != _target_produce_tp_rate_per_client_group().end()) {
        return group_tp_rate->second.quota;
    }
    return _default_target_produce_tp_rate();
}

std::optional<int64_t> quota_manager::get_client_target_fetch_tp_rate(
  const std::optional<std::string_view>& quota_id) {
    if (!quota_id) {
        return _default_target_fetch_tp_rate();
    }
    auto group_tp_rate = _target_fetch_tp_rate_per_client_group().find(
      ss::sstring(quota_id.value()));
    if (group_tp_rate != _target_fetch_tp_rate_per_client_group().end()) {
        return group_tp_rate->second.quota;
    }
    return _default_target_fetch_tp_rate();
}

ss::future<std::chrono::milliseconds> quota_manager::record_partition_mutations(
  std::optional<std::string_view> client_id,
  uint32_t mutations,
  clock::time_point now) {
    /// KIP-599 throttles create_topics / delete_topics / create_partitions
    /// request. This delay should only be applied to these requests if the
    /// quota has been exceeded
    if (!_target_partition_mutation_quota()) {
        co_return 0ms;
    }
    co_return co_await container().invoke_on(
      quota_manager_shard, [client_id, mutations, now](quota_manager& qm) {
          return qm.do_record_partition_mutations(client_id, mutations, now);
      });
}

std::chrono::milliseconds quota_manager::do_record_partition_mutations(
  std::optional<std::string_view> client_id,
  uint32_t mutations,
  clock::time_point now) {
    vassert(
      ss::this_shard_id() == quota_manager_shard,
      "This method can only be executed from quota manager home shard");

    auto quota_id = get_client_quota_id(client_id, {});
    auto it = maybe_add_and_retrieve_quota(quota_id, now);
    const auto units = it->second.pm_rate->record_and_measure(mutations, now);
    auto delay_ms = 0ms;
    if (units < 0) {
        /// Throttle time is defined as -K * R, where K is the number of
        /// tokens in the bucket and R is the avg rate. This only works when
        /// the number of tokens are negative which is the case when the
        /// rate limiter recommends throttling
        const auto rate = (units * -1)
                          * std::chrono::seconds(
                            *_target_partition_mutation_quota());
        delay_ms = std::chrono::duration_cast<std::chrono::milliseconds>(rate);
        std::chrono::milliseconds max_delay_ms(_max_delay());
        if (delay_ms > max_delay_ms) {
            vlog(
              klog.info,
              "Found partition mutation rate for window of: {}. Client:{}, "
              "Estimated backpressure delay of {}. Limiting to {} backpressure "
              "delay",
              rate,
              it->first,
              delay_ms,
              max_delay_ms);
            delay_ms = max_delay_ms;
        }
    }
    return delay_ms;
}

static std::chrono::milliseconds calculate_delay(
  double rate, uint32_t target_rate, clock::duration window_size) {
    std::chrono::milliseconds delay_ms(0);
    if (rate > target_rate) {
        auto diff = rate - target_rate;
        double delay = (diff / target_rate)
                       * static_cast<double>(
                         std::chrono::duration_cast<std::chrono::milliseconds>(
                           window_size)
                           .count());
        delay_ms = std::chrono::milliseconds(static_cast<uint64_t>(delay));
    }
    return delay_ms;
}

std::chrono::milliseconds quota_manager::throttle(
  std::optional<std::string_view> quota_id,
  uint32_t target_rate,
  const clock::time_point& now,
  rate_tracker& rate_tracker) {
    auto rate = rate_tracker.measure(now);
    auto delay_ms = calculate_delay(
      rate, target_rate, rate_tracker.window_size());

    std::chrono::milliseconds max_delay_ms(_max_delay());
    if (delay_ms > max_delay_ms) {
        vlog(
          klog.info,
          "Found data rate for window of: {} bytes. Client:{}, "
          "Estimated "
          "backpressure delay of {}. Limiting to {} backpressure delay",
          rate,
          quota_id,
          delay_ms,
          max_delay_ms);
        delay_ms = max_delay_ms;
    }
    return delay_ms;
}

// record a new observation and return <previous delay, new delay>
throttle_delay quota_manager::record_produce_tp_and_throttle(
  std::optional<std::string_view> client_id,
  uint64_t bytes,
  clock::time_point now) {
    auto quota_id = get_client_quota_id(
      client_id, _target_produce_tp_rate_per_client_group());
    auto it = maybe_add_and_retrieve_quota(quota_id, now);

    it->second.tp_produce_rate.record(bytes, now);
    auto target_tp_rate = get_client_target_produce_tp_rate(quota_id);
    auto delay_ms = throttle(
      quota_id, target_tp_rate, now, it->second.tp_produce_rate);
    auto prev = it->second.delay;
    it->second.delay = delay_ms;
    throttle_delay res{};
    res.enforce = prev.count() > 0;
    res.duration = it->second.delay;
    return res;
}

void quota_manager::record_fetch_tp(
  std::optional<std::string_view> client_id,
  uint64_t bytes,
  clock::time_point now) {
    auto quota_id = get_client_quota_id(
      client_id, _target_fetch_tp_rate_per_client_group());
    auto it = maybe_add_and_retrieve_quota(quota_id, now);
    it->second.tp_fetch_rate.record(bytes, now);
}

throttle_delay quota_manager::throttle_fetch_tp(
  std::optional<std::string_view> client_id, clock::time_point now) {
    auto quota_id = get_client_quota_id(
      client_id, _target_fetch_tp_rate_per_client_group());
    auto target_tp_rate = get_client_target_fetch_tp_rate(quota_id);

    if (!target_tp_rate) {
        return {};
    }
    auto it = maybe_add_and_retrieve_quota(quota_id, now);
    it->second.tp_fetch_rate.maybe_advance_current(now);
    auto delay_ms = throttle(
      quota_id, *target_tp_rate, now, it->second.tp_fetch_rate);
    throttle_delay res{};
    res.enforce = true;
    res.duration = delay_ms;
    return res;
}

// erase inactive tracked quotas. windows are considered inactive if
// they have not received any updates in ten window's worth of time.
void quota_manager::gc(clock::duration full_window) {
    auto now = clock::now();
    auto expire_age = full_window * 10;
    // c++20: replace with std::erase_if
    absl::erase_if(
      _client_quotas,
      [now, expire_age](const std::pair<ss::sstring, client_quota>& q) {
          return (now - q.second.last_seen) > expire_age;
      });
}

} // namespace kafka
