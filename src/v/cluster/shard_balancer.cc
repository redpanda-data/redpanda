/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/shard_balancer.h"

#include "cluster/cluster_utils.h"
#include "cluster/logger.h"
#include "config/node_config.h"

namespace cluster {

shard_balancer::shard_balancer(
  ss::sharded<topic_table>& topics,
  ss::sharded<shard_placement_table>& spt,
  ss::sharded<controller_backend>& cb)
  : _topics(topics)
  , _shard_placement(spt)
  , _controller_backend(cb)
  , _self(*config::node().node_id())
  , _work_queue([](auto ex) {
      if (!ssx::is_shutdown_exception(ex)) {
          vlog(clusterlog.error, "shard balancer exception: {}", ex);
      }
  }) {}

ss::future<> shard_balancer::start() {
    vassert(
      ss::this_shard_id() == shard_id,
      "method can only be invoked on shard {}",
      shard_id);

    auto tt_version = _topics.local().topics_map_revision();

    co_await _shard_placement.invoke_on_all([this](shard_placement_table& spt) {
        return spt.initialize(_topics.local(), _self);
    });

    // we shouldn't be receiving any controller updates at this point, so no
    // risk of missing a notification between initializing shard_placement_table
    // and subscribing.
    vassert(
      tt_version == _topics.local().topics_map_revision(),
      "topic_table unexpectedly changed");

    _topic_table_notify_handle = _topics.local().register_delta_notification(
      [this](topic_table::delta_range_t deltas_range) {
          fragmented_vector<topic_table::delta> deltas(
            deltas_range.begin(), deltas_range.end());
          // Process deltas asynchronously in the work queue to preserve the
          // order in which they appeared.
          _work_queue.submit([this, deltas = std::move(deltas)]() mutable {
              return ss::do_with(std::move(deltas), [this](auto& deltas) {
                  return ss::do_for_each(
                    deltas, [this](const topic_table::delta& d) {
                        return process_delta(d);
                    });
              });
          });
      });
}

ss::future<> shard_balancer::stop() {
    vassert(
      ss::this_shard_id() == shard_id,
      "method can only be invoked on shard {}",
      shard_id);

    _topics.local().unregister_delta_notification(_topic_table_notify_handle);
    co_await _work_queue.shutdown();
}

ss::future<> shard_balancer::process_delta(const topic_table::delta& delta) {
    const auto& ntp = delta.ntp;

    auto shard_callback = [this](const model::ntp& ntp) {
        _controller_backend.local().notify_reconciliation(ntp);
    };

    auto maybe_replicas_view = _topics.local().get_replicas_view(ntp);
    if (!maybe_replicas_view) {
        if (delta.type == topic_table_delta_type::removed) {
            co_await _shard_placement.local().set_target(
              ntp, std::nullopt, shard_callback);
        }
        co_return;
    }
    auto replicas_view = maybe_replicas_view.value();

    // Has value if the partition is expected to exist on this node.
    auto target = placement_target_on_node(replicas_view, _self);

    vlog(
      clusterlog.trace,
      "[{}] setting placement target on on this node: {}",
      ntp,
      target);

    co_await _shard_placement.local().set_target(ntp, target, shard_callback);
}

} // namespace cluster
