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
#include "cluster/producer_state.h"
#include "config/property.h"
#include "metrics/metrics.h"

#include <seastar/core/sharded.hh>

namespace cluster {

class producer_state_manager
  : public ss::peering_sharded_service<producer_state_manager> {
public:
    explicit producer_state_manager(
      config::binding<uint64_t> max_producer_ids,
      std::chrono::milliseconds producer_expiration_ms);

    ss::future<> start();
    ss::future<> stop();

    // register = link + producer_count++
    // note: register is a reserved word in c++
    void register_producer(producer_state&);
    void deregister_producer(producer_state&);
    // temporary relink already accounted producer
    void link(producer_state&);

private:
    static constexpr std::chrono::seconds period{5};
    void setup_metrics();
    void evict_excess_producers();
    void do_evict_excess_producers();

    bool can_evict_producer(const producer_state&) const;

    size_t _num_producers = 0;
    size_t _eviction_counter = 0;
    // if a producer is inactive for this long, it will be gc-ed
    std::chrono::milliseconds _producer_expiration_ms;
    // maximum # of active producers allowed on this shard across
    // all partitions. When exceeded, producers are evctied on an
    // LRU basis.
    config::binding<uint64_t> _max_ids;
    // list of all producers on this shard. producer lifetime is tied to
    // raft group which owns it. linking/unlinking and LRU-ness maintenance
    // is the responsibility of the producers themselves.
    // Check producer_state::run_func()
    intrusive_list<producer_state, &producer_state::_hook> _lru_producers;
    ss::timer<ss::steady_clock_type> _reaper;
    ss::gate _gate;
    metrics::internal_metric_groups _metrics;

    friend struct ::test_fixture;
};
} // namespace cluster
