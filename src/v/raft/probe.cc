// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/probe.h"

#include "config/configuration.h"
#include "model/fundamental.h"
#include "prometheus/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>

namespace raft {

std::vector<ss::metrics::label_instance>
probe::create_metric_labels(const model::ntp& ntp) {
    namespace sm = ss::metrics;
    auto ns_label = sm::label("namespace");
    auto topic_label = sm::label("topic");
    auto partition_label = sm::label("partition");
    return {
      ns_label(ntp.ns()),
      topic_label(ntp.tp.topic()),
      partition_label(ntp.tp.partition()),
    };
}

void probe::setup_public_metrics(const model::ntp& ntp) {
    namespace sm = ss::metrics;

    auto ns_label = ssx::metrics::make_namespaced_label("namespace");
    auto topic_label = ssx::metrics::make_namespaced_label("topic");
    auto partition_label = ssx::metrics::make_namespaced_label("partition");

    auto labels = {
      ns_label(ntp.ns()),
      topic_label(ntp.tp.topic()),
      partition_label(ntp.tp.partition())};

    auto aggregate_labels = {sm::shard_label, partition_label};

    _public_metrics.add_group(
      prometheus_sanitize::metrics_name("raft"),
      {sm::make_counter(
         "leadership_changes",
         [this] { return _leadership_changes; },
         sm::description("Number of leadership changes across all partitions "
                         "of a given topic"),
         labels)
         .aggregate(aggregate_labels)});
}

void probe::setup_metrics(const model::ntp& ntp) {
    namespace sm = ss::metrics;
    auto labels = create_metric_labels(ntp);
    auto aggregate_labels
      = config::shard_local_cfg().aggregate_metrics()
          ? std::vector<sm::label>{sm::shard_label, sm::label("partition")}
          : std::vector<sm::label>{};
    ;

    _metrics.add_group(
      prometheus_sanitize::metrics_name("raft"),
      {sm::make_counter(
         "received_vote_requests",
         [this] { return _vote_requests; },
         sm::description("Number of vote requests received"),
         labels)
         .aggregate(aggregate_labels),
       sm::make_counter(
         "received_append_requests",
         [this] { return _append_requests; },
         sm::description("Number of append requests received"),
         labels)
         .aggregate(aggregate_labels),
       sm::make_counter(
         "sent_vote_requests",
         [this] { return _vote_requests_sent; },
         sm::description("Number of vote requests sent"),
         labels)
         .aggregate(aggregate_labels),
       sm::make_counter(
         "replicate_ack_all_requests",
         [this] { return _replicate_requests_ack_all; },
         sm::description(
           "Number of replicate requests with quorum ack consistency"),
         labels)
         .aggregate(aggregate_labels),
       sm::make_counter(
         "replicate_ack_leader_requests",
         [this] { return _replicate_requests_ack_leader; },
         sm::description(
           "Number of replicate requests with leader ack consistency"),
         labels)
         .aggregate(aggregate_labels),
       sm::make_counter(
         "replicate_ack_none_requests",
         [this] { return _replicate_requests_ack_none; },
         sm::description(
           "Number of replicate requests with no ack consistency"),
         labels)
         .aggregate(aggregate_labels),
       sm::make_counter(
         "done_replicate_requests",
         [this] { return _replicate_requests_done; },
         sm::description("Number of finished replicate requests"),
         labels)
         .aggregate(aggregate_labels),
       sm::make_counter(
         "log_flushes",
         [this] { return _log_flushes; },
         sm::description("Number of log flushes"),
         labels)
         .aggregate(aggregate_labels),
       sm::make_counter(
         "log_truncations",
         [this] { return _log_truncations; },
         sm::description("Number of log truncations"),
         labels)
         .aggregate(aggregate_labels),
       sm::make_counter(
         "leadership_changes",
         [this] { return _leadership_changes; },
         sm::description("Number of leadership changes"),
         labels)
         .aggregate(aggregate_labels),
       sm::make_counter(
         "replicate_request_errors",
         [this] { return _replicate_request_error; },
         sm::description("Number of failed replicate requests"),
         labels)
         .aggregate(aggregate_labels),
       sm::make_counter(
         "heartbeat_requests_errors",
         [this] { return _heartbeat_request_error; },
         sm::description("Number of failed heartbeat requests"),
         labels)
         .aggregate(aggregate_labels),
       sm::make_counter(
         "recovery_requests_errors",
         [this] { return _recovery_request_error; },
         sm::description("Number of failed recovery requests"),
         labels)
         .aggregate(aggregate_labels),
       sm::make_counter(
         "recovery_requests",
         [this] { return _recovery_requests; },
         sm::description("Number of recovery requests"),
         labels)
         .aggregate(aggregate_labels),
       sm::make_counter(
         "group_configuration_updates",
         [this] { return _configuration_updates; },
         sm::description("Number of raft group configuration updates"),
         labels)
         .aggregate(aggregate_labels),
       sm::make_counter(
         "replicate_batch_flush_requests",
         [this] { return _replicate_batch_flushed; },
         sm::description("Number of replicate batch flushes"),
         labels)
         .aggregate(aggregate_labels)});
}

} // namespace raft
