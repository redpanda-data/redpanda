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
#include "cluster/archival/types.h"
#include "metrics/metrics.h"
#include "model/fundamental.h"

#include <seastar/core/metrics_registration.hh>

#include <cstdint>

namespace cluster {
class archival_metadata_stm;
}

namespace archival {

/// \brief Per-ntp archval service probe
///
/// For every NTP we need to track how much log we've already uploaded,
/// how much data we're missing (e.g. because it was compacted before upload)
/// and how much work remains.
///
/// The unit of measure is offset delta.
class ntp_level_probe {
public:
    ntp_level_probe(
      per_ntp_metrics_disabled disabled,
      const model::ntp& ntp,
      ss::shared_ptr<const cluster::archival_metadata_stm> stm);
    ntp_level_probe(const ntp_level_probe&) = delete;
    ntp_level_probe& operator=(const ntp_level_probe&) = delete;
    ntp_level_probe(ntp_level_probe&&) = delete;
    ntp_level_probe& operator=(ntp_level_probe&&) = delete;
    ~ntp_level_probe() = default;

    void setup_ntp_metrics(const model::ntp& ntp);

    void setup_public_metrics(const model::ntp& ntp);

    /// Register log-segment upload
    void uploaded(model::offset offset_delta) { _uploaded += offset_delta; }

    void uploaded_bytes(uint64_t bytes) { _uploaded_bytes += bytes; }

    /// Register gap
    void gap_detected(model::offset offset_delta) { _missing += offset_delta; }

    /// Register the offset the ought to be uploaded
    void upload_lag(model::offset offset_delta) { _pending = offset_delta; }

    void segments_deleted(int64_t deleted_count) {
        _segments_deleted += deleted_count;
    };

    void compacted_replaced_bytes(size_t bytes) {
        _compacted_replaced_bytes = bytes;
    }

private:
    /// Uploaded offsets
    uint64_t _uploaded = 0;
    /// Total uploaded bytes
    uint64_t _uploaded_bytes = 0;
    /// Missing offsets due to gaps
    int64_t _missing = 0;
    /// Width of the offset range yet to be uploaded
    int64_t _pending = 0;
    /// Number of segments deleted by garbage collection
    int64_t _segments_deleted = 0;
    /// cloud bytes "removed" due to compaction operation
    size_t _compacted_replaced_bytes = 0;

    metrics::internal_metric_groups _metrics;
    metrics::public_metric_groups _public_metrics;

    ss::shared_ptr<const cluster::archival_metadata_stm> _stm;
};

/// Metrics probe for upload housekeeping service
/// and its jobs. It tracks the following metrics:
///
/// - Service metrics:
///   - total number of housekeeping rounds (counter)
///   - total number of housekeeping jobs (counter)
///   - total number of failed jobs (counter)
///   - total number of skipped jobs (counter)
/// - Service state transitions:
///   - number of pauses (counter)
///   - number of resumes (counter)
///   - number of drains (counter)
/// - Job metrics:
///   - number of segment reuploads from local data(counter)
///   - number of segment reuploads from cloud data(counter)
///   - number of manifest reuploads (counter)
///   - number of deletions (counter)
///   - number of metadata sync requests (counter)
class upload_housekeeping_probe {
    // the only user of this metrics probe
    friend class upload_housekeeping_service;

public:
    upload_housekeeping_probe();
    upload_housekeeping_probe(const upload_housekeeping_probe&) = delete;
    upload_housekeeping_probe& operator=(const upload_housekeeping_probe&)
      = delete;
    upload_housekeeping_probe(upload_housekeeping_probe&&) = delete;
    upload_housekeeping_probe& operator=(upload_housekeeping_probe&&) = delete;
    ~upload_housekeeping_probe() = default;

    // These metrics are updated by the service
    void housekeeping_rounds(uint64_t add) { _housekeeping_rounds += add; }
    void housekeeping_jobs(uint64_t add) { _housekeeping_jobs += add; }
    void housekeeping_jobs_failed(uint64_t add) {
        _housekeeping_jobs_failed += add;
    }
    void housekeeping_jobs_skipped(uint64_t add) {
        _housekeeping_jobs_skipped += add;
    }

    void housekeeping_resumes(uint64_t add) { _housekeeping_resumes += add; }
    void housekeeping_pauses(uint64_t add) { _housekeeping_pauses += add; }
    void housekeeping_drains(uint64_t add) { _housekeeping_drains += add; }

    // These metrics are updated by housekeeping jobs
    void job_local_segment_reuploads(uint64_t add) {
        _local_segment_reuploads += add;
    }
    void job_cloud_segment_reuploads(uint64_t add) {
        _cloud_segment_reuploads += add;
    }
    void job_metadata_syncs(uint64_t add) { _metadata_syncs += add; }
    void job_metadata_reuploads(uint64_t add) { _manifest_reuploads += add; }
    void job_segment_deletions(uint64_t add) { _segment_deletions += add; }
    void requests_throttled_average_rate(double avg_rate) {
        _requests_throttled_average_rate = avg_rate;
    }

private:
    // service metrics
    uint64_t _housekeeping_rounds{0};
    uint64_t _housekeeping_jobs{0};
    uint64_t _housekeeping_jobs_failed{0};
    uint64_t _housekeeping_jobs_skipped{0};
    double _requests_throttled_average_rate{0};
    // service state transitions
    uint64_t _housekeeping_resumes{0};
    uint64_t _housekeeping_pauses{0};
    uint64_t _housekeeping_drains{0};
    // housekeeping job metrics
    uint64_t _local_segment_reuploads{0};
    uint64_t _cloud_segment_reuploads{0};
    uint64_t _manifest_reuploads{0};
    uint64_t _segment_deletions{0};
    uint64_t _metadata_syncs{0};

    metrics::public_metric_groups _service_metrics;
    metrics::public_metric_groups _jobs_metrics;
};

} // namespace archival
