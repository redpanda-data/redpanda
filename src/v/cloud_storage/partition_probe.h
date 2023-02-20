/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "model/fundamental.h"

#include <seastar/core/metrics_registration.hh>

namespace cloud_storage {

class partition_probe {
public:
    explicit partition_probe(const model::ntp& ntp);

    void add_bytes_read(uint64_t read) { _bytes_read += read; }
    void add_records_read(uint64_t read) { _records_read += read; }

    void segment_materialized() { ++_cur_materialized_segments; }
    void segment_offloaded() { --_cur_materialized_segments; }

    void reader_created() { ++_cur_readers; }
    void reader_destroyed() { --_cur_readers; }
    void segment_reader_created() { ++_cur_segment_readers; }
    void segment_reader_destroyed() { --_cur_segment_readers; }

private:
    uint64_t _bytes_read = 0;
    uint64_t _records_read = 0;

    int32_t _cur_materialized_segments = 0;

    int32_t _cur_readers = 0;
    int32_t _cur_segment_readers = 0;

    ss::metrics::metric_groups _metrics;
};

} // namespace cloud_storage
