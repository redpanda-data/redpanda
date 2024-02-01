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

#include <seastar/core/sharded.hh>

namespace cluster {
class producer_state_manager {
public:
    explicit producer_state_manager(config::binding<uint64_t> max_producer_ids);

    ss::future<> start();
    ss::future<> stop();
    /**
     * Adds producer state to the producer state manager cache
     */
    void register_producer(producer_state&, std::optional<vcluster_id>);
    /**
     * Removes producer from the producer state manager. (This method does not
     * call eviction hook)
     */
    void deregister_producer(producer_state&, std::optional<vcluster_id>);
    /**
     * Touch a producer in underlying queue
     */
    void touch(producer_state&, std::optional<vcluster_id>);

private:
    /**
     *  Constant to be used when a partition has no vcluster_id assigned.
     */
    static constexpr vcluster_id no_vcluster{xid::data_t{0x00}};

    struct producer_state_evictor {
        bool operator()(producer_state&) const noexcept;
    };

    struct producer_state_disposer {
        void operator()(producer_state&) const noexcept;
    };
    using cache_t = namespaced_cache<
      producer_state,
      vcluster_id,
      &producer_state::_hook,
      producer_state_evictor,
      producer_state_disposer>;

    void setup_metrics();

    // maximum number of active producers allowed on this shard across
    // all partitions. When exceeded, producers are evicted on an
    // LRU basis.
    config::binding<uint64_t> _max_ids;
    // cache of all producers on this shard
    cache_t _cache;
    ss::gate _gate;
    metrics::internal_metric_groups _metrics;

    friend struct ::test_fixture;
};
} // namespace cluster
