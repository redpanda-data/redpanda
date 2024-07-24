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
#include "utils/log_hist.h"

#include <seastar/core/metrics_registration.hh>

namespace cloud_storage {

class partition_probe {
public:
    explicit partition_probe(const model::ntp& ntp);

    void add_bytes_read(uint64_t read) { _bytes_read += read; }
    void add_bytes_skip(uint64_t skip) { _bytes_skip += skip; }
    void add_bytes_accept(uint64_t accept) { _bytes_accept += accept; }
    void add_records_read(uint64_t read) { _records_read += read; }
    void chunk_size(uint64_t size) { _chunk_size = size; }

    uint64_t get_bytes_read() const noexcept { return _bytes_read; }
    uint64_t get_bytes_skip() const noexcept { return _bytes_skip; }
    uint64_t get_bytes_accept() const noexcept { return _bytes_accept; }
    uint64_t get_records_read() const noexcept { return _records_read; }
    uint64_t get_chunk_size() const noexcept { return _chunk_size; }

    void clear_metrics() { _metrics.clear(); }

private:
    // Number of bytes that partition reader have received
    uint64_t _bytes_read = 0;
    // This is used in tests and not exposed to the metrics endpoint
    // Number of bytes skipped by the segment batch reader
    uint64_t _bytes_skip = 0;
    // This is used in tests and not exposed to the metrics endpoint
    // Number of bytes accepted by the segment batch reader
    uint64_t _bytes_accept = 0;
    uint64_t _records_read = 0;
    uint64_t _chunk_size = 0;

    metrics::internal_metric_groups _metrics;
};

class ts_read_path_probe {
public:
    using hist_t = log_hist_internal;

    explicit ts_read_path_probe();

    void segment_materialized() { ++_cur_materialized_segments; }
    void segment_offloaded() { --_cur_materialized_segments; }

    void reader_created() { ++_cur_readers; }
    void reader_destroyed() { --_cur_readers; }
    void segment_reader_created() { ++_cur_segment_readers; }
    void segment_reader_destroyed() { --_cur_segment_readers; }

    void set_spillover_manifest_bytes(int64_t total) {
        _spillover_manifest_bytes = total;
    }

    void set_spillover_manifest_instances(int32_t num) {
        _spillover_manifest_instances = num;
    }

    void on_spillover_manifest_hydration() { _spillover_manifest_hydrated++; }

    void on_spillover_manifest_materialization() {
        _spillover_manifest_materialized++;
    }

    auto spillover_manifest_latency() {
        return _spillover_mat_latency.auto_measure();
    }

    void on_chunks_hydration(size_t num) { _chunks_hydrated += num; }

    auto chunk_hydration_latency() {
        return _chunk_hydration_latency.auto_measure();
    }

    void download_throttled(size_t value) { _downloads_throttled_sum += value; }

    auto get_downloads_throttled_sum() const noexcept {
        return _downloads_throttled_sum;
    }

    void hydration_started() { _hydrations_in_progress++; }
    void hydration_finished() { _hydrations_in_progress--; }

private:
    int32_t _cur_materialized_segments = 0;

    int32_t _cur_readers = 0;
    int32_t _cur_segment_readers = 0;

    int64_t _spillover_manifest_bytes = 0;
    int64_t _spillover_manifest_instances = 0;
    int64_t _spillover_manifest_materialized = 0;
    int64_t _spillover_manifest_hydrated = 0;
    /// Spillover manifest materialization latency
    hist_t _spillover_mat_latency;

    size_t _chunks_hydrated = 0;
    hist_t _chunk_hydration_latency;
    size_t _downloads_throttled_sum = 0;
    size_t _hydrations_in_progress = 0;

    metrics::internal_metric_groups _metrics;
    metrics::public_metric_groups _public_metrics;
};

class track_hydration {
public:
    explicit track_hydration(ts_read_path_probe&);
    ~track_hydration();

    track_hydration(const track_hydration&) = delete;
    track_hydration& operator=(const track_hydration&) = delete;
    track_hydration(track_hydration&&) = delete;
    track_hydration& operator=(track_hydration&&) = delete;

private:
    ts_read_path_probe& _probe;
};

} // namespace cloud_storage
