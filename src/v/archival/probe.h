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
#include "cloud_storage/probe.h"
#include "model/fundamental.h"
#include "seastarx.h"

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

    /// Register log-segment upload
    void uploaded(model::offset offset_delta) { _uploaded += offset_delta; }

    /// Register gap
    void gap_detected(model::offset offset_delta) { _missing += offset_delta; }

    /// Register the offset the ought to be uploaded
    void upload_lag(model::offset offset_delta) { _pending = offset_delta; }

private:
    /// Uploaded offsets
    int64_t _uploaded;
    /// Missing offsets due to gaps
    int64_t _missing;
    /// Width of the offset range yet to be uploaded
    int64_t _pending;

    ss::metrics::metric_groups _metrics;
};

/// Service level probe
class service_probe {
public:
    explicit service_probe(service_metrics_disabled disabled);

    /// Increment gap counter (global)
    void add_gap() { _cnt_gaps++; }

    /// Count new ntp archiving event
    void start_archiving_ntp() { _cnt_start_archiving_ntp++; }

    /// Count the removal (from the archival subsystem on this shard)
    /// of the ntp event
    void stop_archiving_ntp() { _cnt_stop_archiving_ntp++; }

    /// Count reconciliation loop iterations (liveliness probe)
    void reconciliation() { _cnt_reconciliations++; }

private:
    /// Number of gaps dected
    uint64_t _cnt_gaps;
    /// Start archiving npt event counter
    uint64_t _cnt_start_archiving_ntp;
    /// Stop archiving npt event counter
    uint64_t _cnt_stop_archiving_ntp;
    /// Number of reconciliation loop iterations
    uint64_t _cnt_reconciliations;

    ss::metrics::metric_groups _metrics;
};

} // namespace archival
