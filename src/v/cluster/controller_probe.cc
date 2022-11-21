/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/controller_probe.h"

#include "cluster/controller.h"
#include "cluster/members_table.h"
#include "cluster/partition_leaders_table.h"
#include "prometheus/prometheus_sanitize.h"
#include "ssx/metrics.h"

#include <seastar/core/metrics.hh>

#include <absl/container/flat_hash_set.h>

namespace cluster {

controller_probe::controller_probe(controller& c) noexcept
  : _controller(c)
  , _leadership_notification_handle{} {}

void controller_probe::start() {
    _leadership_notification_handle
      = _controller._raft_manager.local().register_leadership_notification(
        [this](
          raft::group_id group,
          model::term_id /*term*/,
          std::optional<model::node_id> leader_id) {
            // We are only interested in notifications regarding the controller
            // group.
            if (!_controller._raft0 || _controller._raft0->group() != group) {
                return;
            }

            if (leader_id != _controller.self()) {
                _public_metrics.reset();
            } else {
                setup_metrics();
            }
        });
}

void controller_probe::stop() {
    _public_metrics.reset();
    _controller._raft_manager.local().unregister_leadership_notification(
      _leadership_notification_handle);
}

void controller_probe::setup_metrics() {
    namespace sm = ss::metrics;

    if (config::shard_local_cfg().disable_public_metrics()) {
        return;
    }

    _public_metrics = std::make_unique<ss::metrics::metric_groups>(
      ssx::metrics::public_metrics_handle);
    _public_metrics->add_group(
      prometheus_sanitize::metrics_name("cluster"),
      {
        sm::make_gauge(
          "brokers",
          [this] {
              const auto& members_table
                = _controller.get_members_table().local();
              return members_table.node_count();
          },
          sm::description("Number of configured brokers in the cluster"))
          .aggregate({sm::shard_label}),
        sm::make_gauge(
          "topics",
          [this] {
              const auto& topic_table = _controller.get_topics_state().local();
              return topic_table.all_topics_count();
          },
          sm::description("Number of topics in the cluster"))
          .aggregate({sm::shard_label}),
        sm::make_gauge(
          "partitions",
          [this] {
              return _controller.get_topics_state().local().partition_count();
          },
          sm::description(
            "Number of partitions in the cluster (replicas not included)"))
          .aggregate({sm::shard_label}),
        sm::make_gauge(
          "unavailable_partitions",
          [this] {
              const auto& leaders_table
                = _controller.get_partition_leaders().local();
              auto unavailable_partitions_count = 0;

              leaders_table.for_each_leader([&unavailable_partitions_count](
                                              const auto& /*tp_ns*/,
                                              auto /*pid*/,
                                              auto leader,
                                              auto /*term*/) {
                  if (!leader.has_value()) {
                      ++unavailable_partitions_count;
                  }
              });

              return unavailable_partitions_count;
          },
          sm::description(
            "Number of partitions that lack quorum among replicants"))
          .aggregate({sm::shard_label}),
      });
}

} // namespace cluster
