/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/gc/epoch_barrier_coordinator.h"

#include "cloud_topics/data_plane_api.h"
#include "cloud_topics/logger.h"
#include "cluster/cluster_epoch_service.h"

namespace cloud_topics::l0::gc {

// TODO(oren): error handling in here is really bad

epoch_barrier_coordinator::epoch_barrier_coordinator(
  ss::sharded<cluster::cluster_epoch_service<ss::lowres_clock>>& epoch_service,
  data_plane_api& data_plane)
  : _epoch_service(epoch_service)
  , _data_plane(data_plane) {}

ss::future<> epoch_barrier_coordinator::start() { return ss::now(); }

ss::future<> epoch_barrier_coordinator::stop() { return ss::now(); }

ss::future<std::expected<std::monostate, ::rpc::errc>>
epoch_barrier_coordinator::invalidate(cluster_epoch candidate) {
    vlog(
      cd_log.debug,
      "Epoch barrier: invalidating epoch cache for candidate {}",
      candidate);
    co_await _epoch_service.local().force_epoch_update(candidate());
    co_await _epoch_service.local().invalidate_epoch_cache(
      cluster_epoch::max());
    co_return std::monostate{};
}

ss::future<std::expected<bool, ::rpc::errc>>
epoch_barrier_coordinator::poll_drain(cluster_epoch candidate) {
    vlog(
      cd_log.debug,
      "Epoch barrier: draining in-flight writes for candidate {}",
      candidate);
    co_await _data_plane.drain_inflight_writes();
    vlog(
      cd_log.debug,
      "Epoch barrier: drain complete for candidate {}",
      candidate);
    co_return true;
}

ss::future<std::expected<std::monostate, ::rpc::errc>>
epoch_barrier_coordinator::publish_safe_epoch(cluster_epoch safe_epoch) {
    vlog(cd_log.debug, "Epoch barrier: publishing safe epoch {}", safe_epoch);
    co_await container().invoke_on_all(
      [safe_epoch](epoch_barrier_coordinator& c) {
          c._safe_epoch = safe_epoch;
      });
    co_return std::monostate{};
}

} // namespace cloud_topics::l0::gc
