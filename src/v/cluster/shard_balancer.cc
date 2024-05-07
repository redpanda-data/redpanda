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
  , _self(*config::node().node_id()) {}

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
          for (const auto& delta : deltas_range) {
              // Filter out only deltas that might change the set of partition
              // replicas on this node.
              switch (delta.type) {
              case topic_table_delta_type::disabled_flag_updated:
              case topic_table_delta_type::properties_updated:
                  continue;
              default:
                  _to_assign.insert(delta.ntp);
                  _wakeup_event.set();
                  break;
              }
          }
      });

    ssx::background = assign_fiber();
}

ss::future<> shard_balancer::stop() {
    vassert(
      ss::this_shard_id() == shard_id,
      "method can only be invoked on shard {}",
      shard_id);

    _topics.local().unregister_delta_notification(_topic_table_notify_handle);
    _wakeup_event.set();
    return _gate.close();
}

ss::future<> shard_balancer::assign_fiber() {
    if (_gate.is_closed()) {
        co_return;
    }
    auto gate_holder = _gate.hold();

    while (true) {
        co_await _wakeup_event.wait(1s);
        if (_gate.is_closed()) {
            co_return;
        }

        co_await do_assign_ntps();
    }
}

ss::future<> shard_balancer::do_assign_ntps() {
    auto to_assign = std::exchange(_to_assign, {});
    co_await ss::max_concurrent_for_each(
      to_assign, 128, [this](const model::ntp& ntp) {
          return assign_ntp(ntp);
      });
}

ss::future<> shard_balancer::assign_ntp(const model::ntp& ntp) {
    auto shard_callback = [this](const model::ntp& ntp) {
        _controller_backend.local().notify_reconciliation(ntp);
    };

    std::optional<shard_placement_target> target;
    auto replicas_view = _topics.local().get_replicas_view(ntp);
    if (replicas_view) {
        // Has value if the partition is expected to exist on this node.
        target = placement_target_on_node(replicas_view.value(), _self);
    }
    vlog(
      clusterlog.trace,
      "[{}] setting placement target on this node: {}",
      ntp,
      target);

    try {
        co_await _shard_placement.local().set_target(
          ntp, target, shard_callback);
    } catch (...) {
        auto ex = std::current_exception();
        if (!ssx::is_shutdown_exception(ex)) {
            vlog(
              clusterlog.warn,
              "[{}] exception while setting target: {}",
              ntp,
              ex);
            // Retry on the next tick.
            _to_assign.insert(ntp);
        }
    }
}

} // namespace cluster
