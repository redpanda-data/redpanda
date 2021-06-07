/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "archival/types.h"
#include "model/fundamental.h"
#include "seastarx.h"

#include <seastar/core/metrics_registration.hh>

#include <cstdint>

namespace cloud_storage {

/// Service level probe
class service_probe {
public:
    service_probe() = default;

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

    /// Register manifest download
    void partition_manifest_download() { _cnt_partition_manifest_downloads++; }

    /// Get manifest download
    uint64_t get_partition_manifest_downloads() const {
        return _cnt_partition_manifest_downloads;
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
    void successful_upload(size_t n) { _cnt_successful_uploads += n; }

    /// Get successfull uploads
    uint64_t get_successful_uploads() const { return _cnt_successful_uploads; }

    /// Register successfull downloads
    void successful_download(size_t n) { _cnt_successful_downloads += n; }

    /// Get successfull downloads
    uint64_t get_successful_downloads() const {
        return _cnt_successful_downloads;
    }

    /// Register failed uploads
    void failed_upload(size_t n) { _cnt_failed_uploads += n; }

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

private:
    /// Number of topic manifest uploads
    uint64_t _cnt_topic_manifest_uploads;
    /// Number of manifest (re)uploads
    uint64_t _cnt_partition_manifest_uploads;
    /// Number of topic manifest downloads
    uint64_t _cnt_topic_manifest_downloads;
    /// Number of manifest downloads
    uint64_t _cnt_partition_manifest_downloads;
    /// Number of times backoff was applied during manifest upload
    uint64_t _cnt_manifest_upload_backoff;
    /// Number of times backoff was applied during manifest download
    uint64_t _cnt_manifest_download_backoff;
    /// Number of failed manifest uploads
    uint64_t _cnt_failed_manifest_uploads;
    /// Number of failed manifest downloads
    uint64_t _cnt_failed_manifest_downloads;
    /// Number of completed log-segment uploads
    uint64_t _cnt_successful_uploads;
    /// Number of completed log-segment uploads
    uint64_t _cnt_successful_downloads;
    /// Number of failed log-segment uploads
    uint64_t _cnt_failed_uploads;
    /// Number of failed log-segment downloads
    uint64_t _cnt_failed_downloads;
    /// Number of times backoff  was applied during log-segment uploads
    uint64_t _cnt_upload_backoff;
    /// Number of times backoff  was applied during log-segment downloads
    uint64_t _cnt_download_backoff;

    ss::metrics::metric_groups _metrics;
};

} // namespace cloud_storage
