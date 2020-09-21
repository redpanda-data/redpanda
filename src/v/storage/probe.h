#pragma once
#include "model/fundamental.h"
#include "storage/logger.h"
#include "storage/segment.h"

#include <seastar/core/metrics_registration.hh>
#include <seastar/core/shared_ptr.hh>

#include <cstdint>

namespace storage {
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

    void segment_created() { ++_log_segments_created; }

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
    uint32_t _batch_parse_errors = 0;
    uint32_t _batch_write_errors = 0;
    ss::metrics::metric_groups _metrics;
};
} // namespace storage
