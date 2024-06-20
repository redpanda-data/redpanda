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
#include "base/seastarx.h"
#include "config/client_group_byte_rate_quota.h"
#include "config/property.h"
#include "kafka/server/atomic_token_bucket.h"
#include "kafka/server/client_quota_translator.h"
#include "ssx/sharded_ptr.h"
#include "ssx/sharded_value.h"
#include "utils/absl_sstring_hash.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/timer.hh>
#include <seastar/util/noncopyable_function.hh>
#include <seastar/util/shared_token_bucket.hh>

#include <absl/container/node_hash_map.h>

#include <chrono>
#include <memory>
#include <optional>
#include <string_view>

namespace kafka {

template<typename clock>
class client_quotas_probe;

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

    // Accounting for quota on per-client and per-client-group basis
    // last_seen_ms: used for gc keepalive
    // tp_produce_rate: produce throughput tracking
    // tp_fetch_rate: fetch throughput tracking
    // pm_rate: partition mutation quota tracking
    struct client_quota {
        ssx::sharded_value<clock::time_point> last_seen_ms;
        std::optional<atomic_token_bucket> tp_produce_rate;
        std::optional<atomic_token_bucket> tp_fetch_rate;
        std::optional<atomic_token_bucket> pm_rate;
    };

    using client_quotas_map_t
      = absl::node_hash_map<tracker_key, ss::lw_shared_ptr<client_quota>>;
    using client_quotas_t = ssx::sharded_ptr<client_quotas_map_t>;

    quota_manager(
      client_quotas_t& client_quotas,
      ss::sharded<cluster::client_quota::store>& client_quota_store);
    quota_manager(const quota_manager&) = delete;
    quota_manager& operator=(const quota_manager&) = delete;
    quota_manager(quota_manager&&) = delete;
    quota_manager& operator=(quota_manager&&) = delete;
    ~quota_manager();

    ss::future<> stop();

    ss::future<> start();

    // record a new observation
    ss::future<clock::duration> record_produce_tp_and_throttle(
      std::optional<std::string_view> client_id,
      uint64_t bytes,
      clock::time_point now = clock::now());

    // record a new observation
    ss::future<> record_fetch_tp(
      std::optional<std::string_view> client_id,
      uint64_t bytes,
      clock::time_point now = clock::now());

    ss::future<clock::duration> throttle_fetch_tp(
      std::optional<std::string_view> client_id,
      clock::time_point now = clock::now());

    // Used to record new number of partitions mutations
    // Only for use with the quotas introduced by KIP-599, namely to track
    // partition creation and deletion events (create topics, delete topics &
    // create partitions)
    ss::future<std::chrono::milliseconds> record_partition_mutations(
      std::optional<std::string_view> client_id,
      uint32_t mutations,
      clock::time_point now = clock::now());

private:
    using quota_mutation_callback_t
      = ss::noncopyable_function<clock::duration(client_quota&)>;

    using quota_config
      = std::unordered_map<ss::sstring, config::client_group_quota>;

    clock::duration cap_to_max_delay(const tracker_key&, clock::duration);

    // erase inactive tracked quotas. windows are considered inactive if they
    // have not received any updates in ten window's worth of time.
    void gc();
    ss::future<> do_gc(clock::time_point expire_threshold);

    ss::future<clock::duration> maybe_add_and_retrieve_quota(
      tracker_key quota_id,
      clock::time_point now,
      quota_mutation_callback_t cb);
    ss::future<> add_quota_id(tracker_key quota_id, clock::time_point now);
    void update_client_quotas();

    config::binding<int16_t> _default_num_windows;
    config::binding<std::chrono::milliseconds> _default_window_width;
    config::binding<std::optional<int64_t>> _replenish_threshold;

    client_quotas_t& _client_quotas;
    client_quota_translator _translator;
    std::unique_ptr<client_quotas_probe<clock>> _probe;

    ss::timer<> _gc_timer;
    clock::duration _gc_freq;
    config::binding<std::chrono::milliseconds> _max_delay;
    ss::gate _gate;
};

} // namespace kafka
