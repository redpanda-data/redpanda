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

void probe::setup_metrics(const model::ntp& ntp) {
    namespace sm = ss::metrics;
    auto labels = create_metric_labels(ntp);

    _metrics.add_group(
      prometheus_sanitize::metrics_name("raft"),
      {sm::make_derive(
         "received_vote_requests",
         [this] { return _vote_requests; },
         sm::description("Number of vote requests received"),
         labels),
       sm::make_derive(
         "received_append_requests",
         [this] { return _append_requests; },
         sm::description("Number of append requests received"),
         labels),
       sm::make_derive(
         "sent_vote_requests",
         [this] { return _vote_requests_sent; },
         sm::description("Number of vote requests sent"),
         labels),
       sm::make_derive(
         "replicate_ack_all_requests",
         [this] { return _replicate_requests_ack_all; },
         sm::description(
           "Number of replicate requests with quorum ack consistency"),
         labels),
       sm::make_derive(
         "replicate_ack_leader_requests",
         [this] { return _replicate_requests_ack_leader; },
         sm::description(
           "Number of replicate requests with leader ack consistency"),
         labels),
       sm::make_derive(
         "replicate_ack_none_requests",
         [this] { return _replicate_requests_ack_none; },
         sm::description(
           "Number of replicate requests with no ack consistency"),
         labels),
       sm::make_derive(
         "done_replicate_requests",
         [this] { return _replicate_requests_done; },
         sm::description("Number of finished replicate requests"),
         labels),
       sm::make_derive(
         "log_flushes",
         [this] { return _log_flushes; },
         sm::description("Number of log flushes"),
         labels),
       sm::make_derive(
         "log_truncations",
         [this] { return _log_truncations; },
         sm::description("Number of log truncations"),
         labels),
       sm::make_derive(
         "leadership_changes",
         [this] { return _leadership_changes; },
         sm::description("Number of leadership changes"),
         labels)});
}

} // namespace raft
