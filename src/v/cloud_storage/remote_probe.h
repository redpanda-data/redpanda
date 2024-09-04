/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "cloud_io/io_resources.h"
#include "cloud_storage/types.h"
#include "metrics/metrics.h"
#include "model/fundamental.h"
#include "utils/log_hist.h"

#include <seastar/core/metrics_registration.hh>

#include <cstdint>

namespace cloud_storage {

class materialized_resources;

/// Cloud storage endpoint level probe
class remote_probe {
public:
    using hist_t = log_hist_internal;

    explicit remote_probe(
      remote_metrics_disabled disabled,
      remote_metrics_disabled public_disabled,
      materialized_resources&,
      const cloud_io::io_resources&);
    remote_probe(const remote_probe&) = delete;
    remote_probe& operator=(const remote_probe&) = delete;
    remote_probe(remote_probe&&) = delete;
    remote_probe& operator=(remote_probe&&) = delete;
    ~remote_probe() = default;

    /// Register topic manifest upload
    void topic_manifest_upload() { _cnt_topic_manifest_uploads++; }

    /// Get topic manifest upload
    uint64_t get_topic_manifest_uploads() const {
        return _cnt_topic_manifest_uploads;
    }

    /// Register topic manifest download
    void topic_manifest_download() { _cnt_topic_manifest_downloads++; }

    /// Get topic manifest download
    uint64_t get_topic_manifest_downloads() const {
        return _cnt_topic_manifest_downloads;
    }

    /// Register manifest (re)upload
    void partition_manifest_upload() { _cnt_partition_manifest_uploads++; }

    /// Get manifest (re)upload
    uint64_t get_partition_manifest_uploads() const {
        return _cnt_partition_manifest_uploads;
    }

    /// Register manifest (re)upload
    void spillover_manifest_upload() { _cnt_spillover_manifest_uploads++; }

    /// Get manifest (re)upload
    uint64_t get_spillover_manifest_uploads() const {
        return _cnt_spillover_manifest_uploads;
    }

    /// Register manifest download
    void partition_manifest_download() { _cnt_partition_manifest_downloads++; }

    /// Get manifest download
    uint64_t get_partition_manifest_downloads() const {
        return _cnt_partition_manifest_downloads;
    }

    /// Register spillover manifest download
    void spillover_manifest_download() { _cnt_spillover_manifest_downloads++; }

    /// Get manifest download
    uint64_t get_spillover_manifest_downloads() const {
        return _cnt_spillover_manifest_downloads;
    }

    void cluster_metadata_manifest_upload() {
        ++_cnt_cluster_metadata_manifest_uploads;
    }
    uint64_t get_cluster_metadata_manifest_uploads() const {
        return _cnt_cluster_metadata_manifest_uploads;
    }

    void cluster_metadata_manifest_download() {
        ++_cnt_cluster_metadata_manifest_downloads;
    }
    uint64_t get_cluster_metadata_manifest_downloads() const {
        return _cnt_cluster_metadata_manifest_downloads;
    }

    /// Register manifest (re)upload
    void txrange_manifest_upload() { _cnt_tx_manifest_uploads++; }

    /// Get manifest (re)upload
    uint64_t get_txrange_manifest_uploads() const {
        return _cnt_tx_manifest_uploads;
    }

    /// Register manifest download
    void txrange_manifest_download() { _cnt_tx_manifest_downloads++; }

    /// Get manifest download
    uint64_t get_txrange_manifest_downloads() const {
        return _cnt_tx_manifest_downloads;
    }

    /// Register manifest (re)upload
    void topic_mount_manifest_upload() { _cnt_topic_mount_manifest_uploads++; }

    /// Get manifest (re)upload
    uint64_t get_topic_mount_manifest_uploads() const {
        return _cnt_topic_mount_manifest_uploads;
    }

    /// Register manifest download
    void topic_mount_manifest_download() {
        _cnt_topic_mount_manifest_downloads++;
    }

    /// Get manifest download
    uint64_t get_topic_mount_manifest_downloads() const {
        return _cnt_topic_mount_manifest_downloads;
    }

    /// Register backof invocation during manifest upload
    void manifest_upload_backoff() { _cnt_manifest_upload_backoff++; }

    /// Get backof invocation during manifest upload or download
    uint64_t get_manifest_upload_backoffs() const {
        return _cnt_manifest_upload_backoff;
    }

    /// Register backof invocation during manifest upload
    void manifest_download_backoff() { _cnt_manifest_download_backoff++; }

    /// Get backof invocation during manifest upload or download
    uint64_t get_manifest_download_backoffs() const {
        return _cnt_manifest_download_backoff;
    }

    /// Register failed manifest upload
    void failed_manifest_upload() { _cnt_failed_manifest_uploads++; }

    /// Get failed manifest uploads
    uint64_t get_failed_manifest_uploads() const {
        return _cnt_failed_manifest_uploads;
    }

    /// Register failed manifest download
    void failed_manifest_download() { _cnt_failed_manifest_downloads++; }

    /// Get failed manifest downloads
    uint64_t get_failed_manifest_downloads() const {
        return _cnt_failed_manifest_downloads;
    }

    /// Register successfull uploads
    void successful_upload() { _cnt_successful_uploads++; }

    /// Get successfull uploads
    uint64_t get_successful_uploads() const { return _cnt_successful_uploads; }

    /// Register successfull downloads
    void successful_download() { _cnt_successful_downloads++; }

    /// Get successfull downloads
    uint64_t get_successful_downloads() const {
        return _cnt_successful_downloads;
    }

    /// Register failed uploads
    void failed_upload() { _cnt_failed_uploads++; }

    /// Get failed uploads
    uint64_t get_failed_uploads() const { return _cnt_failed_uploads; }

    /// Register failed download
    void failed_download() { _cnt_failed_downloads++; }

    /// Get failed downloads
    uint64_t get_failed_downloads() const { return _cnt_failed_downloads; }

    /// Register backoff during log-segment upload
    void upload_backoff() { _cnt_upload_backoff++; }

    /// Get backoff during log-segment upload
    uint64_t get_upload_backoffs() const { return _cnt_upload_backoff; }

    /// Register backoff during log-segment download
    void download_backoff() { _cnt_download_backoff++; }

    /// Get backoff during log-segment download
    uint64_t get_download_backoffs() const { return _cnt_download_backoff; }

    void register_upload_size(size_t n) { _cnt_bytes_sent += n; }

    void register_download_size(size_t n) { _cnt_bytes_received += n; }

    uint64_t get_failed_index_uploads() const {
        return _cnt_failed_index_uploads;
    }

    uint64_t get_failed_index_downloads() const {
        return _cnt_failed_index_downloads;
    }

    uint64_t get_index_uploads() const { return _cnt_index_uploads; }

    uint64_t get_index_downloads() const { return _cnt_index_downloads; }

    void failed_index_upload() { ++_cnt_failed_index_uploads; }

    void failed_index_download() { ++_cnt_failed_index_downloads; }

    void index_upload() { ++_cnt_index_uploads; }

    void index_download() { ++_cnt_index_downloads; }

    auto client_acquisition() {
        return _client_acquisition_latency.auto_measure();
    }

    auto segment_download() { return _segment_download_latency.auto_measure(); }

    void controller_snapshot_successful_upload() {
        _cnt_controller_snapshot_successful_uploads++;
    }
    void controller_snapshot_failed_upload() {
        _cnt_controller_snapshot_failed_uploads++;
    }
    void controller_snapshot_upload_backoff() {
        _cnt_controller_snapshot_upload_backoffs++;
    }
    uint64_t get_controller_snapshot_successful_uploads() const {
        return _cnt_controller_snapshot_successful_uploads;
    }
    uint64_t get_controller_snapshot_failed_uploads() const {
        return _cnt_controller_snapshot_failed_uploads;
    }
    uint64_t get_controller_snapshot_upload_backoffs() const {
        return _cnt_controller_snapshot_upload_backoffs;
    }

private:
    /// Number of topic manifest uploads
    uint64_t _cnt_topic_manifest_uploads{0};
    /// Number of manifest (re)uploads
    uint64_t _cnt_partition_manifest_uploads{0};
    /// Number of topic manifest downloads
    uint64_t _cnt_topic_manifest_downloads{0};
    /// Number of manifest downloads
    uint64_t _cnt_partition_manifest_downloads{0};
    /// Number of cluster metadata manifest uploads..
    uint64_t _cnt_cluster_metadata_manifest_uploads{0};
    /// Number of cluster metadata manifest downloads.
    uint64_t _cnt_cluster_metadata_manifest_downloads{0};
    /// Number of times backoff was applied during manifest upload
    uint64_t _cnt_manifest_upload_backoff{0};
    /// Number of times backoff was applied during manifest download
    uint64_t _cnt_manifest_download_backoff{0};
    /// Number of failed manifest uploads
    uint64_t _cnt_failed_manifest_uploads{0};
    /// Number of failed manifest downloads
    uint64_t _cnt_failed_manifest_downloads{0};
    /// Number of completed log-segment uploads
    uint64_t _cnt_successful_uploads{0};
    /// Number of completed log-segment uploads
    uint64_t _cnt_successful_downloads{0};
    /// Number of failed log-segment uploads
    uint64_t _cnt_failed_uploads{0};
    /// Number of failed log-segment downloads
    uint64_t _cnt_failed_downloads{0};
    /// Number of times backoff  was applied during log-segment uploads
    uint64_t _cnt_upload_backoff{0};
    /// Number of times backoff  was applied during log-segment downloads
    uint64_t _cnt_download_backoff{0};
    /// Number of completed controller snapshot uploads.
    uint64_t _cnt_controller_snapshot_successful_uploads{0};
    /// Number of failed controller snapshot uploads.
    uint64_t _cnt_controller_snapshot_failed_uploads{0};
    /// Number of times backoff was applied during controller snapshot uploads.
    uint64_t _cnt_controller_snapshot_upload_backoffs{0};
    /// Number of bytes being successfully sent to S3
    uint64_t _cnt_bytes_sent{0};
    /// Number of bytes being successfully received from S3
    uint64_t _cnt_bytes_received{0};
    /// Number of tx-range manifest uploads
    uint64_t _cnt_tx_manifest_uploads{0};
    /// Number of tx-range manifest downloads
    uint64_t _cnt_tx_manifest_downloads{0};
    /// Number of index uploads
    uint64_t _cnt_index_uploads{0};
    /// Number of index downloads
    uint64_t _cnt_index_downloads{0};
    /// Number of failed index uploads
    uint64_t _cnt_failed_index_uploads{0};
    /// Number of failed index downloads
    uint64_t _cnt_failed_index_downloads{0};
    /// Number of spillover manifest uploads
    uint64_t _cnt_spillover_manifest_uploads{0};
    /// Number of spillover manifest downloads
    uint64_t _cnt_spillover_manifest_downloads{0};
    /// Number of topic_mount manifest uploads
    uint64_t _cnt_topic_mount_manifest_uploads{0};
    /// Number of topic_mount manifest downloads
    uint64_t _cnt_topic_mount_manifest_downloads{0};

    hist_t _client_acquisition_latency;
    hist_t _segment_download_latency;

    metrics::internal_metric_groups _metrics;
    metrics::public_metric_groups _public_metrics;
};

} // namespace cloud_storage
