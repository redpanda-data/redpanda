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

#include "producer_state_manager.h"

#include "cluster/logger.h"
#include "prometheus/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>
#include <seastar/util/defer.hh>

namespace cluster {

producer_state_manager::producer_state_manager(
  config::binding<uint64_t> max_producer_ids,
  std::chrono::milliseconds producer_expiration_ms)
  : _producer_expiration_ms(producer_expiration_ms)
  , _max_ids(std::move(max_producer_ids)) {
    setup_metrics();
}

ss::future<> producer_state_manager::start() {
    _reaper.set_callback([this] { evict_excess_producers(); });
    _reaper.arm(period);
    vlog(clusterlog.info, "Started producer state manager");
    return ss::now();
}

ss::future<> producer_state_manager::stop() {
    _reaper.cancel();
    return _gate.close();
}

void producer_state_manager::setup_metrics() {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }
    namespace sm = ss::metrics;
    _metrics.add_group(
      prometheus_sanitize::metrics_name("cluster:producer_state_manager"),
      {sm::make_gauge(
         "producer_manager_total_active_producers",
         [this] { return _num_producers; },
         sm::description(
           "Total number of active idempotent and transactional producers.")),
       sm::make_counter(
         "evicted_producers",
         [this] { return _eviction_counter; },
         sm::description("Number of evicted producers so far."))});
}

void producer_state_manager::register_producer(producer_state& state) {
    link(state);
    ++_num_producers;
    vlog(clusterlog.debug, "Registered producer: {}", state);
}

void producer_state_manager::deregister_producer(producer_state& state) {
    if (state._hook.is_linked()) {
        state._hook.unlink();
        --_num_producers;
        vlog(clusterlog.debug, "Removing producer: {}", state);
    }
}

void producer_state_manager::link(producer_state& state) {
    vassert(
      !state._hook.is_linked(),
      "double linking of producer state {}",
      state._id);
    _lru_producers.push_back(state);
}

bool producer_state_manager::can_evict_producer(
  const producer_state& state) const {
    return _num_producers > _max_ids()
           || state.ms_since_last_update() > _producer_expiration_ms;
}

void producer_state_manager::evict_excess_producers() {
    ssx::background = ssx::spawn_with_gate_then(_gate, [this]() {
                          do_evict_excess_producers();
                      }).finally([this] {
        if (!_gate.is_closed()) {
            _reaper.arm(period);
        }
    });
}

void producer_state_manager::do_evict_excess_producers() {
    if (_gate.is_closed()) {
        return;
    }
    vlog(clusterlog.debug, "producer eviction tick");
    auto it = _lru_producers.begin();
    while (it != _lru_producers.end() && can_evict_producer(*it)) {
        auto it_copy = it;
        ++it;
        auto& state = *it_copy;
        // Here eviction does not need to check if an operation is
        // currently in progress on the producer at this point. That
        // is because the producer unlinks itself from this list
        // temporarily and relinks back after the operation is finished
        // essentially resulting in the fact that only currently inactive
        // producers are in the list. This makes the whole logic lock free.
        ssx::spawn_with_gate(_gate, [&state] { return state.evict(); });
        --_num_producers;
        ++_eviction_counter;
    }
}

}; // namespace cluster
