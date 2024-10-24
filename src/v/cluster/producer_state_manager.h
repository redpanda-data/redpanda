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
#include "cluster/namespaced_cache.h"
#include "cluster/producer_state.h"
#include "config/property.h"
#include "metrics/metrics.h"

#include <seastar/core/sharded.hh>

namespace cluster::tx {
class producer_state_manager {
public:
    explicit producer_state_manager(

      config::binding<uint64_t> max_producer_ids,
      config::binding<std::chrono::milliseconds> producer_expiration_ms,
      config::binding<size_t> virtual_cluster_min_producer_ids);

    ss::future<> start();
    ss::future<> stop();
    /**
     * Adds producer state to the producer state manager cache
     */
    void register_producer(producer_state&, std::optional<model::vcluster_id>);
    /**
     * Removes producer from the producer state manager. (This method does not
     * call eviction hook)
     */
    void
    deregister_producer(producer_state&, std::optional<model::vcluster_id>);
    /**
     * Touch a producer in underlying queue
     */
    void touch(producer_state&, std::optional<model::vcluster_id>);

private:
    static constexpr std::chrono::seconds period{5};
    /**
     *  Constant to be used when a partition has no vcluster_id assigned.
     */
    static constexpr model::vcluster_id no_vcluster{xid::data_t{0x00}};

    struct pre_eviction_hook {
        bool operator()(producer_state&) const noexcept;
    };

    struct post_eviction_hook {
        explicit post_eviction_hook(producer_state_manager&);

        void operator()(producer_state&) const noexcept;

        producer_state_manager& _state_manger;
    };
    friend post_eviction_hook;
    using cache_t = namespaced_cache<
      producer_state,
      model::vcluster_id,
      &producer_state::_hook,
      pre_eviction_hook,
      post_eviction_hook>;

    void setup_metrics();
    void evict_excess_producers();
    size_t _eviction_counter = 0;
    // if a producer is inactive for this long, it will be gc-ed
    config::binding<std::chrono::milliseconds> _producer_expiration_ms;
    // maximum number of active producers allowed on this shard across
    // all partitions. When exceeded, producers are evicted on an
    // LRU basis.
    config::binding<uint64_t> _max_ids;
    config::binding<size_t> _virtual_cluster_min_producer_ids;
    // cache of all producers on this shard
    cache_t _cache;
    ss::timer<ss::steady_clock_type> _reaper;
    ss::gate _gate;
    metrics::internal_metric_groups _metrics;

    friend struct ::test_fixture;
};
} // namespace cluster::tx
