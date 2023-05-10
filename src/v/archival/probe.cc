/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/probe.h"

#include "archival/ntp_archiver_service.h"
#include "config/configuration.h"
#include "prometheus/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/smp.hh>

namespace archival {

ntp_level_probe::ntp_level_probe(
  const ntp_archiver& archiver,
  per_ntp_metrics_disabled disabled,
  model::ntp ntp)
  : _parent{archiver}
  , _ntp{std::move(ntp)}
  , _private_metrics_disabled{disabled} {
    if (!_private_metrics_disabled) {
        setup_static_metrics(_ntp);
    }

    if (!config::shard_local_cfg().disable_public_metrics()) {
        setup_static_public_metrics(_ntp);
    }
}

void ntp_level_probe::setup_static_metrics(const model::ntp& ntp) {
    namespace sm = ss::metrics;

    auto ns_label = sm::label("namespace");
    auto topic_label = sm::label("topic");
    auto partition_label = sm::label("partition");
    const std::vector<sm::label_instance> labels = {
      ns_label(ntp.ns()),
      topic_label(ntp.tp.topic()),
      partition_label(ntp.tp.partition()),
    };
    auto aggregate_labels = config::shard_local_cfg().aggregate_metrics()
                              ? std::vector<sm::label>{sm::shard_label}
                              : std::vector<sm::label>{};

    _static_metrics.add_group(
      prometheus_sanitize::metrics_name("ntp_archiver"),
      {sm::make_counter(
         "uploaded",
         [this] { return _uploaded; },
         sm::description("Uploaded offsets"),
         labels)
         .aggregate(aggregate_labels),
       sm::make_total_bytes(
         "uploaded_bytes",
         [this] { return _uploaded_bytes; },
         sm::description("Total number of uploaded bytes"),
         labels)
         .aggregate(aggregate_labels),
       sm::make_counter(
         "missing",
         [this] { return _missing; },
         sm::description("Missing offsets due to gaps"),
         labels)
         .aggregate(aggregate_labels)});
}
void ntp_level_probe::setup_dynamic_metrics(const model::ntp& ntp) {
    namespace sm = ss::metrics;

    auto ns_label = sm::label("namespace");
    auto topic_label = sm::label("topic");
    auto partition_label = sm::label("partition");
    const std::vector<sm::label_instance> labels = {
      ns_label(ntp.ns()),
      topic_label(ntp.tp.topic()),
      partition_label(ntp.tp.partition()),
    };
    auto aggregate_labels = config::shard_local_cfg().aggregate_metrics()
                              ? std::vector<sm::label>{sm::shard_label}
                              : std::vector<sm::label>{};

    _dynamic_metrics->add_group(
      prometheus_sanitize::metrics_name("ntp_archiver"),
      {
        sm::make_gauge(
          "pending",
          [this] { return _parent.records_pending_upload(); },
          sm::description("Pending Kafka offsets"),
          labels)
          .aggregate(aggregate_labels),
      });
}

void ntp_level_probe::setup_static_public_metrics(const model::ntp& ntp) {
    namespace sm = ss::metrics;

    auto ns_label = ssx::metrics::make_namespaced_label("namespace");
    auto topic_label = ssx::metrics::make_namespaced_label("topic");
    auto partition_label = ssx::metrics::make_namespaced_label("partition");
    const std::vector<sm::label_instance> labels = {
      ns_label(ntp.ns()),
      topic_label(ntp.tp.topic()),
      partition_label(ntp.tp.partition()),
    };

    auto aggregate_labels = std::vector<sm::label>{
      sm::shard_label, partition_label};

    _static_public_metrics.add_group(
      prometheus_sanitize::metrics_name("cloud_storage"),
      {sm::make_total_bytes(
         "uploaded_bytes",
         [this] { return _uploaded_bytes; },
         sm::description("Total number of uploaded bytes for the topic"),
         labels)
         .aggregate(aggregate_labels),
       sm::make_counter(
         "deleted_segments",
         [this] { return _segments_deleted; },
         sm::description(
           "Number of segments that have been deleted from S3 for the topic. "
           "This may grow due to retention or non compacted segments being "
           "replaced with their compacted equivalent."),
         labels)
         .aggregate(aggregate_labels)});
}

void ntp_level_probe::setup_dynamic_public_metrics(const model::ntp& ntp) {
    namespace sm = ss::metrics;

    auto ns_label = ssx::metrics::make_namespaced_label("namespace");
    auto topic_label = ssx::metrics::make_namespaced_label("topic");
    auto partition_label = ssx::metrics::make_namespaced_label("partition");
    const std::vector<sm::label_instance> labels = {
      ns_label(ntp.ns()),
      topic_label(ntp.tp.topic()),
      partition_label(ntp.tp.partition()),
    };

    auto aggregate_labels = std::vector<sm::label>{
      sm::shard_label, partition_label};

    _dynamic_public_metrics->add_group(
      prometheus_sanitize::metrics_name("cloud_storage"),
      {
        sm::make_gauge(
          "segments",
          [this] { return _segments_in_manifest; },
          sm::description(
            "Total number of accounted segments in the cloud for the topic"),
          labels)
          .aggregate(aggregate_labels),
        sm::make_gauge(
          "segments_pending_deletion",
          [this] { return _segments_to_delete; },
          sm::description("Total number of segments pending deletion from the "
                          "cloud for the topic"),
          labels)
          .aggregate(aggregate_labels),
        sm::make_gauge(
          "records_pending_upload",
          [this] { return _parent.records_pending_upload(); },
          sm::description("The number of records in the local log that have "
                          "not yet been uploaded"),
          labels)
          .aggregate(aggregate_labels),
        sm::make_gauge(
          "estimated_backlog_size_bytes",
          [this] { return _parent.estimate_backlog_size(); },
          sm::description("Estimated upload backlog size in bytes"),
          labels)
          .aggregate(aggregate_labels),
      });
}

upload_housekeeping_probe::upload_housekeeping_probe() {
    namespace sm = ss::metrics;

    auto aggregate_labels = std::vector<sm::label>{sm::shard_label};

    _service_metrics.add_group(
      prometheus_sanitize::metrics_name("cloud_storage_housekeeping"),
      {
        sm::make_counter(
          "rounds",
          [this] { return _housekeeping_rounds; },
          sm::description("Number of upload housekeeping rounds"))
          .aggregate(aggregate_labels),
        sm::make_total_bytes(
          "jobs_completed",
          [this] { return _housekeeping_jobs; },
          sm::description("Number of executed housekeeping jobs"))
          .aggregate(aggregate_labels),
        sm::make_counter(
          "jobs_failed",
          [this] { return _housekeeping_jobs_failed; },
          sm::description("Number of failed housekeeping jobs"))
          .aggregate(aggregate_labels),
        sm::make_counter(
          "jobs_skipped",
          [this] { return _housekeeping_jobs_skipped; },
          sm::description("Number of skipped housekeeping jobs"))
          .aggregate(aggregate_labels),
        sm::make_gauge(
          "resumes",
          [this] { return _housekeeping_resumes; },
          sm::description("Number of times upload housekeeping was resumed"))
          .aggregate(aggregate_labels),
        sm::make_gauge(
          "pauses",
          [this] { return _housekeeping_pauses; },
          sm::description("Number of times upload housekeeping was paused"))
          .aggregate(aggregate_labels),
        sm::make_gauge(
          "drains",
          [this] { return _housekeeping_drains; },
          sm::description(
            "Number of times upload housekeeping queue was drained"))
          .aggregate(aggregate_labels),
      });

    _jobs_metrics.add_group(
      prometheus_sanitize::metrics_name("cloud_storage_jobs"),
      {
        sm::make_gauge(
          "local_segment_reuploads",
          [this] { return _local_segment_reuploads; },
          sm::description(
            "Number of segment reuploads from local data directory"))
          .aggregate(aggregate_labels),
        sm::make_gauge(
          "cloud_segment_reuploads",
          [this] { return _cloud_segment_reuploads; },
          sm::description(
            "Number of segment reuploads from cloud storage sources (cloud "
            "storage cache or direct download from cloud storage)"))
          .aggregate(aggregate_labels),
        sm::make_gauge(
          "manifest_reuploads",
          [this] { return _manifest_reuploads; },
          sm::description(
            "Number of manifest reuploads performed by all housekeeping jobs"))
          .aggregate(aggregate_labels),
        sm::make_gauge(
          "segment_deletions",
          [this] { return _segment_deletions; },
          sm::description(
            "Number of segments deleted by all housekeeping jobs"))
          .aggregate(aggregate_labels),
        sm::make_gauge(
          "metadata_syncs",
          [this] { return _metadata_syncs; },
          sm::description("Number of archival configuration updates performed "
                          "by all housekeeping jobs"))
          .aggregate(aggregate_labels),
      });
}

} // namespace archival
