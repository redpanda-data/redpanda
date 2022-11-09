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

#include "archival/types.h"
#include "cloud_storage/probe.h"
#include "model/fundamental.h"
#include "seastarx.h"
#include "ssx/metrics.h"

#include <seastar/core/metrics_registration.hh>

#include <cstdint>

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
    ntp_level_probe(per_ntp_metrics_disabled disabled, const model::ntp& ntp);

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

    void segments_in_manifest(int64_t count) { _segments_in_manifest = count; };

    void segments_to_delete(int64_t count) { _segments_to_delete = count; };

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
    /// Number of accounted segments in the cloud
    int64_t _segments_in_manifest = 0;
    /// Number of segments awaiting deletion
    int64_t _segments_to_delete = 0;

    ss::metrics::metric_groups _metrics;
    ss::metrics::metric_groups _public_metrics{
      ssx::metrics::public_metrics_handle};
};

/// Service level probe
class service_probe {
public:
    explicit service_probe(service_metrics_disabled disabled);

    /// Count new ntp archiving event
    void start_archiving_ntp() { _cnt_start_archiving_ntp++; }

    /// Count the removal (from the archival subsystem on this shard)
    /// of the ntp event
    void stop_archiving_ntp() { _cnt_stop_archiving_ntp++; }

private:
    /// Start archiving npt event counter
    uint64_t _cnt_start_archiving_ntp = 0;
    /// Stop archiving npt event counter
    uint64_t _cnt_stop_archiving_ntp = 0;

    ss::metrics::metric_groups _metrics;
};

} // namespace archival
