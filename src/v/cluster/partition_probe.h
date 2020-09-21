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

    void add_records_produced(uint64_t num_records) {
        _records_produced += num_records;
    }

    void add_records_fetched(uint64_t num_records) {
        _records_fetched += num_records;
    }

private:
    partition& _partition;
    uint64_t _records_produced = 0;
    uint64_t _records_fetched = 0;
    ss::metrics::metric_groups _metrics;
};
} // namespace cluster
