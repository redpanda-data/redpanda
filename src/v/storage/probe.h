#pragma once
#include "model/fundamental.h"
#include "storage/logger.h"

#include <seastar/core/metrics_registration.hh>

#include <cstdint>

namespace storage {
class probe {
public:
    void add_bytes_written(uint64_t written) { _bytes_written += written; }

    void add_bytes_read(uint64_t read) { _bytes_written += read; }

    void batch_written() { ++_batches_written; }

    void segment_created() { ++_log_segments_created; }

    void batch_write_error(const std::exception_ptr& e) {
        stlog.error("Error writing record batch {}", e);
        ++_batch_write_errors;
    }

    void add_batches_read(uint32_t batches) { _batches_read += batches; }

    void batch_parse_error() { ++_batch_parse_errors; }

    void setup_metrics(const model::ntp&);

private:
    uint64_t _bytes_written = 0;
    uint64_t _batches_written = 0;
    uint64_t _bytes_read = 0;
    uint64_t _log_segments_created = 0;
    uint64_t _batches_read = 0;
    uint32_t _batch_parse_errors = 0;
    uint32_t _batch_write_errors = 0;
    ss::metrics::metric_groups _metrics;
};
} // namespace storage
