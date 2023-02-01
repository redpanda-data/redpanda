/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "model/fundamental.h"
#include "ssx/metrics.h"
#include "storage/fwd.h"
#include "storage/logger.h"
#include "storage/types.h"

#include <seastar/core/metrics_registration.hh>
#include <seastar/core/shared_ptr.hh>

#include <cstdint>

namespace storage {
struct disk_metrics {
    uint64_t total_bytes = 0;
    uint64_t free_bytes = 0;
    disk_space_alert space_alert = disk_space_alert::ok;
};

// Per-node storage probe (metrics).
class node_probe {
public:
    void setup_node_metrics();
    void set_disk_metrics(
      uint64_t total_bytes, uint64_t free_bytes, disk_space_alert alert);

    const disk_metrics& get_disk_metrics() const { return _disk; }

private:
    disk_metrics _disk;
    ss::metrics::metric_groups _public_metrics{
      ssx::metrics::public_metrics_handle};
};

// Per-NTP probe.
class probe {
public:
    void add_bytes_written(uint64_t written) {
        _partition_bytes += written;
        _bytes_written += written;
    }

    void add_bytes_read(uint64_t read) { _bytes_read += read; }
    void add_cached_bytes_read(uint64_t read) { _cached_bytes_read += read; }

    void batch_written() { ++_batches_written; }

    void corrupted_compaction_index() { ++_corrupted_compaction_index; }

    void segment_created() {
        ++_log_segments_created;
        ++_log_segments_active;
    }

    void segment_removed() {
        ++_log_segments_removed;
        --_log_segments_active;
    }

    void initial_segments_count(size_t cnt) { _log_segments_active = cnt; }

    void segment_compacted() { ++_segment_compacted; }

    void batch_write_error(const std::exception_ptr& e) {
        stlog.error("Error writing record batch {}", e);
        ++_batch_write_errors;
    }

    void add_batches_read(uint32_t batches) { _batches_read += batches; }
    void add_cached_batches_read(uint32_t batches) {
        _cached_batches_read += batches;
    }

    void batch_parse_error() { ++_batch_parse_errors; }

    void setup_metrics(const model::ntp&);

    void delete_segment(const segment&);

    size_t partition_size() const { return _partition_bytes; }
    void add_initial_segment(const segment&);
    void remove_partition_bytes(size_t remove) { _partition_bytes -= remove; }
    void set_compaction_ratio(double r) { _compaction_ratio = r; }

    int64_t get_batch_parse_errors() const { return _batch_parse_errors; }
    /**
     * Clears all probe related metrics
     */
    void clear_metrics() { _metrics.clear(); }

private:
    uint64_t _partition_bytes = 0;
    uint64_t _bytes_written = 0;
    uint64_t _bytes_read = 0;
    uint64_t _cached_bytes_read = 0;

    uint64_t _batches_written = 0;
    uint64_t _batches_read = 0;
    uint64_t _cached_batches_read = 0;

    uint32_t _segment_compacted = 0;
    uint32_t _corrupted_compaction_index = 0;
    uint32_t _log_segments_created = 0;
    uint32_t _log_segments_removed = 0;
    uint32_t _log_segments_active = 0;
    uint32_t _batch_parse_errors = 0;
    uint32_t _batch_write_errors = 0;
    double _compaction_ratio = 1.0;
    ss::metrics::metric_groups _metrics;
};
} // namespace storage
