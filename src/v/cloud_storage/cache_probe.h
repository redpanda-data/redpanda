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

#include "metrics/metrics.h"
#include "model/fundamental.h"

#include <seastar/core/metrics_registration.hh>

namespace cloud_storage {

class cache_probe {
public:
    cache_probe();

    void put() { ++_num_puts; }
    void get() { ++_num_gets; }
    void cached_get() { ++_num_cached_gets; }
    void miss_get() { ++_num_miss_gets; }

    void set_size(uint64_t size) {
        _cur_size_bytes = size;
        _hwm_size_bytes = std::max(_cur_size_bytes, _hwm_size_bytes);
    }
    void set_num_files(uint64_t num_files) {
        _cur_num_files = num_files;
        _hwm_num_files = std::max(_cur_num_files, _hwm_num_files);
    }
    void put_started() { ++_cur_in_progress_files; }
    void put_ended() { --_cur_in_progress_files; }

    void fast_trim() { ++_fast_trims; }
    void exhaustive_trim() { ++_exhaustive_trims; }
    void carryover_trim() { ++_carryover_trims; }
    void failed_trim() { ++_failed_trims; }

private:
    uint64_t _num_puts = 0;
    uint64_t _num_gets = 0;
    uint64_t _num_cached_gets = 0;
    uint64_t _num_miss_gets = 0;

    int64_t _cur_size_bytes = 0;
    int64_t _hwm_size_bytes = 0;
    int64_t _cur_num_files = 0;
    int64_t _hwm_num_files = 0;
    int64_t _cur_in_progress_files = 0;

    int64_t _fast_trims{0};
    int64_t _exhaustive_trims{0};
    int64_t _carryover_trims{0};
    int64_t _failed_trims{0};

    metrics::internal_metric_groups _metrics;
    metrics::public_metric_groups _public_metrics;
};

} // namespace cloud_storage
