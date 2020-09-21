#pragma once
#include "model/fundamental.h"

#include <seastar/core/metrics_registration.hh>

#include <cstdint>

namespace cluster {

class partition;

class partition_probe {
public:
    explicit partition_probe(partition& partition)
      : _partition(partition) {}

    void setup_metrics(const model::ntp&);

private:
    partition& _partition;
    ss::metrics::metric_groups _metrics;
};
} // namespace cluster
