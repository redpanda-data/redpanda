/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster_link/link_probe.h"

#include "cluster_link/replication/types.h"
#include "model/namespace.h"

#include <seastar/core/metrics.hh>

#include <ranges>

namespace sm = ss::metrics;

namespace cluster_link {

link_probe::link_probe(link& link)
  : _link(link)
  , _fetch_data{ss::make_lw_shared<replication::link_data_probe>()}
  , _public_counter_metrics()
  , _public_status_metrics() {
    if (
      _link.partition_manager().is_current_shard_leader(
        ::model::controller_ntp)) {
        setup_status_metrics();
    }

    setup_counter_metrics();
}

void link_probe::handle_on_leadership_change(
  const ::model::ntp& ntp, ntp_leader is_ntp_leader) {
    if (ntp != ::model::controller_ntp) {
        return;
    }

    if (is_ntp_leader) {
        setup_status_metrics();
    } else {
        reset_status_metrics();
    }
}

int link_probe::count_topics(const model::mirror_topic_status status) const {
    if (!_link.partition_manager().is_current_shard_leader(
          ::model::controller_ntp)) {
        return 0;
    }

    auto mirror_topics = _link.get_mirror_topics_for_link();
    if (!mirror_topics.has_value()) {
        return 0;
    }

    auto to_status = std::views::transform(
      &model::mirror_topic_metadata::status);
    auto topic_statuses = mirror_topics.value() | std::views::values
                          | to_status;
    return std::ranges::count(topic_statuses, status);
}

void link_probe::setup_counter_metrics() {
    if (config::shard_local_cfg().disable_public_metrics()) {
        return;
    }

    const auto sl_name = shadow_link_name(_link.get_config()->name);

    _public_counter_metrics.add_group(
      shadow_link_group,
      {
        sm::make_total_bytes(
          "total_records_fetched",
          [this] { return _fetch_data->total_fetched.n_records; },
          sm::description("Total number of records fetched by the replicator"),
          {sl_name})
          .aggregate({sm::shard_label}),
        sm::make_total_bytes(
          "total_records_written",
          [this] { return _fetch_data->total_written.n_records; },
          sm::description("Total number of records written by the replicator"),
          {sl_name})
          .aggregate({sm::shard_label}),
        sm::make_total_bytes(
          "total_bytes_fetched",
          [this] { return _fetch_data->total_fetched.n_bytes; },
          sm::description("Total number of bytes fetched by the replicator"),
          {sl_name})
          .aggregate({sm::shard_label}),
        sm::make_total_bytes(
          "total_bytes_written",
          [this] { return _fetch_data->total_written.n_bytes; },
          sm::description("Total number of bytes written by the replicator"),
          {sl_name})
          .aggregate({sm::shard_label}),
      });
}

void link_probe::setup_status_metrics() {
    if (_public_status_metrics.has_value()) {
        return;
    }

    if (config::shard_local_cfg().disable_public_metrics()) {
        return;
    }

    const auto sl_name = shadow_link_name(_link.get_config()->name);

    // topics per mirror_topic_status
    auto status_to_metric = std::views::transform(
      [this, &sl_name](const model::mirror_topic_status status) {
          auto status_label = sm::label_instance(
            "status", to_string_view(status));
          return sm::make_gauge(
                   "shadow_topic_state",
                   [this, status] { return count_topics(status); },
                   sm::description(
                     "Number of shadow topics in the respective state"),
                   {sl_name, status_label})
            .aggregate({sm::shard_label});
      });
    constexpr auto statuses = std::to_array({
      model::mirror_topic_status::active,
      model::mirror_topic_status::failed,
      model::mirror_topic_status::paused,
      model::mirror_topic_status::failing_over,
      model::mirror_topic_status::failed_over,
      model::mirror_topic_status::promoting,
      model::mirror_topic_status::promoted,
    });

    std::vector<sm::metric_definition> defs;
    defs.reserve(statuses.size());

    std::ranges::copy(statuses | status_to_metric, std::back_inserter(defs));

    _public_status_metrics.emplace().add_group(shadow_link_group, defs);
}

} // namespace cluster_link
