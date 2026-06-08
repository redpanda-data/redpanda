/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/seastarx.h"
#include "lsm/lsm.h"
#include "metrics/metrics.h"
#include "model/fundamental.h"

#include <seastar/core/shared_ptr.hh>

namespace cloud_topics::l1 {

/// Per-metastore-partition wrapper around lsm::probe that registers the
/// underlying counters and histograms with the seastar metrics system.
///
/// One instance per replicated_database; lifetime matches the database, so
/// leadership churn re-registers the metric set.
class metastore_lsm_probe {
public:
    explicit metastore_lsm_probe(model::partition_id metastore_partition);

    metastore_lsm_probe(const metastore_lsm_probe&) = delete;
    metastore_lsm_probe& operator=(const metastore_lsm_probe&) = delete;
    metastore_lsm_probe(metastore_lsm_probe&&) = delete;
    metastore_lsm_probe& operator=(metastore_lsm_probe&&) = delete;
    ~metastore_lsm_probe() = default;

    /// The shared probe pointer to pass to lsm::options.probe.
    ss::lw_shared_ptr<lsm::probe> probe() const { return _probe; }

    /// Eagerly unregister the metric group. Callers should invoke this at the
    /// well-defined teardown point (e.g. replicated_database::close()) rather
    /// than relying on the destructor, so a new probe for the same
    /// metastore_partition can be registered while the old shared_ptr to the
    /// owning replicated_database is still alive (held by an in-flight RPC
    /// fiber).
    void deregister_metrics() { _metrics.clear(); }

private:
    ss::lw_shared_ptr<lsm::probe> _probe;
    metrics::internal_metric_groups _metrics;
};

} // namespace cloud_topics::l1
