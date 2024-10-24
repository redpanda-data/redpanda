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
#include "cluster/producer_state.h"
#include "cluster/types.h"
#include "config/property.h"
#include "metrics/prometheus_sanitize.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/metrics.hh>
#include <seastar/util/defer.hh>

namespace cluster::tx {

producer_state_manager::producer_state_manager(
  config::binding<uint64_t> max_producer_ids,
  config::binding<std::chrono::milliseconds> producer_expiration_ms,
  config::binding<size_t> virtual_cluster_min_producer_ids)
  : _producer_expiration_ms(std::move(producer_expiration_ms))
  , _max_ids(std::move(max_producer_ids))
  , _virtual_cluster_min_producer_ids(
      std::move(virtual_cluster_min_producer_ids))
  , _cache(
      _max_ids,
      _virtual_cluster_min_producer_ids,
      pre_eviction_hook{},
      post_eviction_hook(*this)) {
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
         [this] { return _cache.get_stats().total_size; },
         sm::description(
           "Total number of active idempotent and transactional producers.")),
       sm::make_counter(
         "evicted_producers",
         [this] { return _eviction_counter; },
         sm::description("Number of evicted producers so far."))});
}

void producer_state_manager::register_producer(
  producer_state& state, std::optional<model::vcluster_id> vcluster) {
    vlog(
      clusterlog.debug,
      "Registering producer: {}, current producer count: {}",
      state,
      _cache.get_stats().total_size);
    _cache.insert(vcluster.value_or(no_vcluster), state);
}

void producer_state_manager::deregister_producer(
  producer_state& state, std::optional<model::vcluster_id> vcluster) {
    vlog(
      clusterlog.debug,
      "Removing producer: {}, current producer count: {}",
      state,
      _cache.get_stats().total_size);
    _cache.remove(vcluster.value_or(no_vcluster), state);
}
void producer_state_manager::touch(
  producer_state& state, std::optional<model::vcluster_id> vcluster) {
    vlog(clusterlog.trace, "Touched producer: {}", state);
    _cache.touch(vcluster.value_or(no_vcluster), state);
}
void producer_state_manager::evict_excess_producers() {
    _cache.evict_older_than<ss::lowres_system_clock>(
      ss::lowres_system_clock::now() - _producer_expiration_ms());
    if (!_gate.is_closed()) {
        _reaper.arm(period);
    }
}

bool producer_state_manager::pre_eviction_hook::operator()(
  producer_state& state) const noexcept {
    return state.can_evict();
}

producer_state_manager::post_eviction_hook::post_eviction_hook(
  producer_state_manager& mgr)
  : _state_manger(mgr) {}

void producer_state_manager::post_eviction_hook::operator()(
  producer_state& state) const noexcept {
    _state_manger._eviction_counter++;
    return state._post_eviction_hook();
}
}; // namespace cluster::tx
