/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/remote_probe.h"

#include "cloud_io/io_resources.h"
#include "cloud_storage/materialized_resources.h"
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/smp.hh>

namespace cloud_storage {

remote_probe::remote_probe(
  remote_metrics_disabled disabled,
  remote_metrics_disabled public_disabled,
  materialized_resources& ms,
  const cloud_io::io_resources& io) {
    namespace sm = ss::metrics;

    if (!disabled) {
        _metrics.add_group(
          prometheus_sanitize::metrics_name("cloud_storage"),
          {
            sm::make_counter(
              "topic_manifest_uploads",
              [this] { return get_topic_manifest_uploads(); },
              sm::description("Number of topic manifest uploads")),
            sm::make_counter(
              "partition_manifest_uploads",
              [this] { return get_partition_manifest_uploads(); },
              sm::description("Number of partition manifest (re)uploads")),
            sm::make_counter(
              "topic_manifest_downloads",
              [this] { return get_topic_manifest_downloads(); },
              sm::description("Number of topic manifest downloads")),
            sm::make_counter(
              "partition_manifest_downloads",
              [this] { return get_partition_manifest_downloads(); },
              sm::description("Number of partition manifest downloads")),
            sm::make_counter(
              "cluster_metadata_manifest_uploads",
              [this] { return get_cluster_metadata_manifest_uploads(); },
              sm::description("Number of partition manifest uploads")),
            sm::make_counter(
              "cluster_metadata_manifest_downloads",
              [this] { return get_cluster_metadata_manifest_downloads(); },
              sm::description("Number of partition manifest downloads")),
            sm::make_counter(
              "manifest_upload_backoff",
              [this] { return get_manifest_upload_backoffs(); },
              sm::description(
                "Number of times backoff was applied during manifest upload")),
            sm::make_counter(
              "manifest_download_backoff",
              [this] { return get_manifest_download_backoffs(); },
              sm::description("Number of times backoff was applied during "
                              "manifest download")),
            sm::make_counter(
              "successful_uploads",
              [this] { return get_successful_uploads(); },
              sm::description("Number of completed log-segment uploads")),
            sm::make_counter(
              "successful_downloads",
              [this] { return get_successful_downloads(); },
              sm::description("Number of completed log-segment downloads")),
            sm::make_counter(
              "failed_uploads",
              [this] { return get_failed_uploads(); },
              sm::description("Number of failed log-segment uploads")),
            sm::make_counter(
              "failed_downloads",
              [this] { return get_failed_downloads(); },
              sm::description("Number of failed log-segment downloads")),
            sm::make_counter(
              "failed_manifest_uploads",
              [this] { return get_failed_manifest_uploads(); },
              sm::description("Number of failed manifest uploads")),
            sm::make_counter(
              "failed_manifest_downloads",
              [this] { return get_failed_manifest_downloads(); },
              sm::description("Number of failed manifest downloads")),
            sm::make_counter(
              "upload_backoff",
              [this] { return get_upload_backoffs(); },
              sm::description("Number of times backoff was applied during "
                              "log-segment uploads")),
            sm::make_counter(
              "download_backoff",
              [this] { return get_download_backoffs(); },
              sm::description("Number of times backoff  was applied during "
                              "log-segment downloads")),
            sm::make_counter(
              "bytes_sent",
              [this] { return _cnt_bytes_sent; },
              sm::description("Number of bytes sent to cloud storage")),
            sm::make_counter(
              "bytes_received",
              [this] { return _cnt_bytes_received; },
              sm::description("Number of bytes received from cloud storage")),
            sm::make_counter(
              "index_uploads",
              [this] { return get_index_uploads(); },
              sm::description("Number of segment indices uploaded")),
            sm::make_counter(
              "index_downloads",
              [this] { return get_index_downloads(); },
              sm::description("Number of segment indices downloaded")),
            sm::make_counter(
              "failed_index_uploads",
              [this] { return get_failed_index_uploads(); },
              sm::description("Number of failed segment index uploads")),
            sm::make_counter(
              "failed_index_downloads",
              [this] { return get_failed_index_downloads(); },
              sm::description("Number of failed segment index downloads")),
            sm::make_counter(
              "spillover_manifest_uploads",
              [this] { return get_spillover_manifest_uploads(); },
              sm::description("Number of spillover manifest (re)uploads")),
            sm::make_counter(
              "spillover_manifest_downloads",
              [this] { return get_spillover_manifest_downloads(); },
              sm::description("Number of spillover manifest downloads")),
            sm::make_counter(
              "controller_snapshot_successful_uploads",
              [this] { return get_controller_snapshot_successful_uploads(); },
              sm::description(
                "Number of completed controller snapshot uploads")),
            sm::make_counter(
              "controller_snapshot_failed_uploads",
              [this] { return get_controller_snapshot_failed_uploads(); },
              sm::description("Number of failed controller snapshot uploads")),
            sm::make_counter(
              "controller_snapshot_upload_backoff",
              [this] { return get_controller_snapshot_upload_backoffs(); },
              sm::description("Number of times backoff was applied during "
                              "controller snapshot uploads")),
            sm::make_histogram(
              "client_acquisition_latency",
              [this] {
                  return _client_acquisition_latency.public_histogram_logform();
              },
              sm::description("Client acquisition latency histogram")),
            sm::make_histogram(
              "segment_download_latency",
              [this] {
                  return _segment_download_latency.public_histogram_logform();
              },
              sm::description("Segment download latency histogram")),
          });
    }

    if (!public_disabled) {
        auto direction_label = metrics::make_namespaced_label("direction");

        _public_metrics.add_group(
          prometheus_sanitize::metrics_name("cloud_storage"),
          {
            sm::make_counter(
              "errors_total",
              [this] {
                  return get_failed_uploads() + get_failed_manifest_uploads()
                         + get_failed_index_uploads();
              },
              sm::description("Number of transmit errors"),
              {direction_label("tx")})
              .aggregate({sm::shard_label}),

            sm::make_counter(
              "errors_total",
              [this] {
                  return get_failed_downloads()
                         + get_failed_manifest_downloads();
              },
              sm::description("Number of receive errors"),
              {direction_label("rx")})
              .aggregate({sm::shard_label}),
            sm::make_counter(
              "partition_manifest_uploads_total",
              [this] { return get_partition_manifest_uploads(); },
              sm::description("Successful partition manifest uploads"),
              {})
              .aggregate({sm::shard_label}),
            sm::make_counter(
              "segment_uploads_total",
              [this] { return get_successful_uploads(); },
              sm::description("Successful data segment uploads"),
              {})
              .aggregate({sm::shard_label}),
            sm::make_gauge(
              "active_segments",
              [&ms] { return ms.current_segments(); },
              sm::description(
                "Number of remote log segments currently hydrated for read"))
              .aggregate({sm::shard_label}),
            sm::make_gauge(
              "readers",
              [&ms] { return ms.current_segment_readers(); },
              sm::description("Number of segment read cursors for hydrated "
                              "remote log segments"))
              .aggregate({sm::shard_label}),
            sm::make_gauge(
              "partition_readers",
              [&io] { return io.current_ongoing_hydrations(); },
              sm::description(
                "Number of partition reader instances (number of current "
                "fetch/timequery requests reading from tiered storage)"))
              .aggregate({sm::shard_label}),
            sm::make_counter(
              "partition_readers_delayed",
              [&ms] { return ms.get_partition_readers_delayed(); },
              sm::description("How many partition reades were delayed due to "
                              "hitting reader limit. This indicates cluster "
                              "is saturated with tiered storage reads."))
              .aggregate({sm::shard_label}),
            sm::make_counter(
              "segment_readers_delayed",
              [&ms] { return ms.get_segment_readers_delayed(); },
              sm::description("How many segment readers were delayed due to "
                              "hitting reader limit. This indicates cluster "
                              "is saturated with tiered storage reads."))
              .aggregate({sm::shard_label}),
            sm::make_counter(
              "segment_materializations_delayed",
              [&ms] { return ms.get_segments_delayed(); },
              sm::description(
                "How many segment materializations were delayed due to "
                "hitting reader limit. This indicates cluster "
                "is saturated with tiered storage reads."))
              .aggregate({sm::shard_label}),
            sm::make_counter(
              "segment_index_uploads_total",
              [this] { return get_index_uploads(); },
              sm::description("Successful segment index uploads"),
              {})
              .aggregate({sm::shard_label}),
            sm::make_counter(
              "spillover_manifest_uploads_total",
              [this] { return get_spillover_manifest_uploads(); },
              sm::description("Successful spillover manifest uploads"),
              {})
              .aggregate({sm::shard_label}),
            sm::make_gauge(
              "spillover_manifests_materialized_count",
              [&ms] { return ms.get_materialized_manifest_cache().size(); },
              sm::description(
                "How many spilled manifests are currently cached in memory"),
              {})
              .aggregate({sm::shard_label}),
            sm::make_gauge(
              "spillover_manifests_materialized_bytes",
              [&ms] {
                  return ms.get_materialized_manifest_cache().size_bytes();
              },
              sm::description("Bytes of memory used for spilled manifests "
                              "currently cached in memory"),
              {})
              .aggregate({sm::shard_label}),
          });
    }
}

} // namespace cloud_storage
