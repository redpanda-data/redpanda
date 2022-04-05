/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/probe.h"

#include "prometheus/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/smp.hh>

namespace cloud_storage {

remote_probe::remote_probe(remote_metrics_disabled disabled) {
    if (disabled) {
        return;
    }
    namespace sm = ss::metrics;

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
          "manifest_upload_backoff",
          [this] { return get_manifest_upload_backoffs(); },
          sm::description(
            "Number of times backoff was applied during manifest upload")),
        sm::make_counter(
          "manifest_download_backoff",
          [this] { return get_manifest_download_backoffs(); },
          sm::description(
            "Number of times backoff was applied during manifest download")),
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
          sm::description(
            "Number of times backoff was applied during log-segment uploads")),
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
      });
}

} // namespace cloud_storage
