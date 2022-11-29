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

quota_manager::~quota_manager() { _gc_timer.cancel(); }

ss::future<> quota_manager::stop() {
    _gc_timer.cancel();
    return ss::make_ready_future<>();
}

ss::future<> quota_manager::start() {
    _gc_timer.arm_periodic(_gc_freq);
    return ss::make_ready_future<>();
}

quota_manager::underlying_t::iterator
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
    auto [it, inserted] = _quotas.try_emplace(
      ss::sstring(qid),
      quota{
        now,
        clock::duration(0),
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

// If client is part of some group then client qota is group
// else client quota is client_id
std::optional<std::string_view> quota_manager::get_client_quota(
  const std::optional<std::string_view>& client_id) {
    if (!client_id) {
        return std::nullopt;
    }
    for (const auto& group_and_limit : _target_tp_rate_per_client_group()) {
        if (client_id->starts_with(
              std::string_view(group_and_limit.second.clients_prefix))) {
            return group_and_limit.first;
        }
    }
    return client_id;
}

int64_t quota_manager::get_client_target_tp_rate(
  const std::optional<std::string_view>& quota_id) {
    if (!quota_id) {
        return _default_target_tp_rate();
    }
    auto group_tp_rate = _target_tp_rate_per_client_group().find(
      ss::sstring(quota_id.value()));
    if (group_tp_rate != _target_tp_rate_per_client_group().end()) {
        return group_tp_rate->second.quota;
    }
    return _default_target_tp_rate();
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

    auto quota_id = get_client_quota(client_id);
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

// record a new observation and return <previous delay, new delay>
throttle_delay quota_manager::record_tp_and_throttle(
  std::optional<std::string_view> client_id,
  uint64_t bytes,
  clock::time_point now) {
    auto quota_id = get_client_quota(client_id);
    auto it = maybe_add_and_retrieve_quota(quota_id, now);

    auto rate = it->second.tp_rate.record_and_measure(bytes, now);

    std::chrono::milliseconds delay_ms(0);
    auto target_tp_rate = get_client_target_tp_rate(quota_id);
    if (rate > target_tp_rate) {
        auto diff = rate - target_tp_rate;
        double delay = (diff / target_tp_rate)
                       * static_cast<double>(
                         std::chrono::duration_cast<std::chrono::milliseconds>(
                           it->second.tp_rate.window_size())
                           .count());
        delay_ms = std::chrono::milliseconds(static_cast<uint64_t>(delay));
    }

    std::chrono::milliseconds max_delay_ms(_max_delay());
    if (delay_ms > max_delay_ms) {
        vlog(
          klog.info,
          "Found data rate for window of: {} bytes. Client:{}, Estimated "
          "backpressure delay of {}. Limiting to {} backpressure delay",
          rate,
          it->first,
          delay_ms,
          max_delay_ms);
        delay_ms = max_delay_ms;
    }

    auto prev = it->second.delay;
    it->second.delay = delay_ms;

    throttle_delay res{};
    res.first_violation = prev.count() == 0;
    res.duration = it->second.delay;
    return res;
}
// erase inactive tracked quotas. windows are considered inactive if they
// have not received any updates in ten window's worth of time.
void quota_manager::gc(clock::duration full_window) {
    auto now = clock::now();
    auto expire_age = full_window * 10;
    // c++20: replace with std::erase_if
    absl::erase_if(
      _quotas, [now, expire_age](const std::pair<ss::sstring, quota>& q) {
          return (now - q.second.last_seen) > expire_age;
      });
}

} // namespace kafka
