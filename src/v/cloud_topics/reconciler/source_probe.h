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

#include "metrics/metrics.h"
#include "model/fundamental.h"

#include <cstdint>

namespace cloud_topics::reconciler {

class source;

class source_probe {
public:
    source_probe(const model::ntp& ntp, source& src);

    void clear() { _metrics.clear(); }

private:
    source& _source;
    metrics::internal_metric_groups _metrics;
};

} // namespace cloud_topics::reconciler
