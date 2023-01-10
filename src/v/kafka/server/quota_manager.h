/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "config/client_group_byte_rate_quota.h"
#include "config/property.h"
#include "kafka/server/token_bucket_rate_tracker.h"
#include "resource_mgmt/rate.h"
#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/timer.hh>

#include <absl/container/flat_hash_map.h>

#include <chrono>
#include <optional>
#include <string_view>

namespace kafka {

// Shard on which partition mutation rate metrics are aggregated on
static constexpr ss::shard_id quota_manager_shard = 0;

// quota_manager tracks quota usage
//
// TODO:
//   - we will want to eventually add support for configuring the quotas and
//   quota settings as runtime through the kafka api and other mechanisms.
//
//   - currently only total throughput per client_id is tracked. in the future
//   we will want to support additional quotas and accouting granularities to be
//   at parity with kafka. for example:
//
//      - splitting out rates separately for produce and fetch
//      - accounting per user vs per client (these are separate in kafka)
//
//   - it may eventually be beneficial to periodically reduce stats across
//   shards or track stats globally to produce a more accurate per-node
//   representation of a statistic (e.g. bandwidth).
//
class quota_manager : public ss::peering_sharded_service<quota_manager> {
public:
    using clock = ss::lowres_clock;

    struct throttle_delay {
        bool enforce{false};
        clock::duration duration{0};
        clock::duration enforce_duration() const {
            if (enforce) {
                return duration;
            } else {
                return clock::duration::zero();
            }
        }
    };

    quota_manager();
    quota_manager(const quota_manager&) = delete;
    quota_manager& operator=(const quota_manager&) = delete;
    quota_manager(quota_manager&&) = delete;
    quota_manager& operator=(quota_manager&&) = delete;
    ~quota_manager();

    ss::future<> stop();

    ss::future<> start();

    // record a new observation
    throttle_delay record_produce_tp_and_throttle(
      std::optional<std::string_view> client_id,
      uint64_t bytes,
      clock::time_point now = clock::now());

    // record a new observation
    void record_fetch_tp(
      std::optional<std::string_view> client_id,
      uint64_t bytes,
      clock::time_point now = clock::now());

    throttle_delay throttle_fetch_tp(
      std::optional<std::string_view> client_id,
      clock::time_point now = clock::now());

    // Used to record new number of partitions mutations
    // Only for use with the quotas introduced by KIP-599, namely to track
    // partition creation and deletion events (create topics, delete topics &
    // create partitions)
    //
    // NOTE: This method will be invoked on shard 0, therefore ensure that it is
    // not called within a tight loop from another shard
    ss::future<std::chrono::milliseconds> record_partition_mutations(
      std::optional<std::string_view> client_id,
      uint32_t mutations,
      clock::time_point now = clock::now());

private:
    std::chrono::milliseconds do_record_partition_mutations(
      std::optional<std::string_view> client_id,
      uint32_t mutations,
      clock::time_point now);

    // throttle, return <previous delay, new delay>
    std::chrono::milliseconds throttle(
      std::optional<std::string_view> client_id,
      uint32_t target_rate,
      const clock::time_point& now,
      rate_tracker& rate_tracker);

    // Accounting for quota on per-client and per-client-group basis
    // last_seen: used for gc keepalive
    // delay: last calculated delay
    // tp_rate: throughput tracking
    // pm_rate: partition mutation quota tracking - only on home shard
    struct client_quota {
        clock::time_point last_seen;
        clock::duration delay;
        rate_tracker tp_produce_rate;
        rate_tracker tp_fetch_rate;
        std::optional<token_bucket_rate_tracker> pm_rate;
    };
    using client_quotas_t = absl::flat_hash_map<ss::sstring, client_quota>;

private:
    // erase inactive tracked quotas. windows are considered inactive if they
    // have not received any updates in ten window's worth of time.
    void gc(clock::duration full_window);

    client_quotas_t::iterator maybe_add_and_retrieve_quota(
      const std::optional<std::string_view>&, const clock::time_point&);
    int64_t get_client_target_produce_tp_rate(
      const std::optional<std::string_view>& quota_id);
    std::optional<int64_t> get_client_target_fetch_tp_rate(
      const std::optional<std::string_view>& quota_id);

private:
    config::binding<int16_t> _default_num_windows;
    config::binding<std::chrono::milliseconds> _default_window_width;

    config::binding<uint32_t> _default_target_produce_tp_rate;
    config::binding<std::optional<uint32_t>> _default_target_fetch_tp_rate;
    config::binding<std::optional<uint32_t>> _target_partition_mutation_quota;
    config::binding<std::unordered_map<ss::sstring, config::client_group_quota>>
      _target_produce_tp_rate_per_client_group;
    config::binding<std::unordered_map<ss::sstring, config::client_group_quota>>
      _target_fetch_tp_rate_per_client_group;

    client_quotas_t _client_quotas;

    ss::timer<> _gc_timer;
    clock::duration _gc_freq;
    config::binding<std::chrono::milliseconds> _max_delay;
};

} // namespace kafka
