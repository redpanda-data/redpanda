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

#include "cluster/cloud_metadata/uploader.h"
#include "cluster/controller.h"
#include "cluster/members_table.h"
#include "cluster/partition_leaders_table.h"
#include "config/node_config.h"
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"
#include "model/fips_config.h"

#include <seastar/core/metrics.hh>

#include <absl/algorithm/container.h>
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

    _public_metrics = std::make_unique<metrics::public_metric_groups>();
    constexpr static auto cluster_metric_prefix = "cluster";
    _public_metrics->add_group(
      prometheus_sanitize::metrics_name(cluster_metric_prefix),
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

              return leaders_table.leaderless_partition_count();
          },
          sm::description(
            "Number of partitions that lack quorum among replicants"))
          .aggregate({sm::shard_label}),
        sm::make_gauge(
          "non_homogenous_fips_mode",
          [this] {
              const auto& members_table
                = _controller.get_members_table().local();
              const auto& nodes = members_table.nodes();
              auto fips_mode_val = model::from_config(
                config::node().fips_mode());
              return absl::c_count_if(nodes, [fips_mode_val](const auto& iter) {
                  return iter.second.broker.properties().in_fips_mode
                         != fips_mode_val;
              });
          },
          sm::description(
            "Number of nodes that have a non-homogenous FIPS mode value"))
          .aggregate({sm::shard_label}),
      });

    if (auto maybe_uploader = _controller.metadata_uploader()) {
        // add the next metric only if the uploader is available. internally
        // this depends on cloud storage configuration.
        _public_metrics->add_group(
          prometheus_sanitize::metrics_name(cluster_metric_prefix),
          {
            sm::make_gauge(
              "latest_cluster_metadata_manifest_age",
              [this] {
                  auto maybe_manifest_ref
                    = _controller.metadata_uploader().value().get().manifest();
                  if (!maybe_manifest_ref.has_value()) {
                      return int64_t{0};
                  }

                  const auto& manifest = maybe_manifest_ref.value().get();
                  if (manifest.upload_time_since_epoch == 0ms) {
                      // we never uploaded, so let's return a value that is not
                      // problematic to the aggregation of this metric
                      return int64_t{0};
                  }

                  auto now_ts
                    = ss::lowres_system_clock::now().time_since_epoch();

                  auto age_s = std::chrono::duration_cast<std::chrono::seconds>(
                    now_ts - manifest.upload_time_since_epoch);
                  return int64_t{age_s.count()};
              },
              sm::description("Age in seconds of the latest "
                              "cluster_metadata_manifest uploaded"))
              .aggregate({sm::shard_label}),
          });
    }
}

} // namespace cluster
