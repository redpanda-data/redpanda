#pragma once

#include "seastarx.h"

#include "filesystem/wal_writer_utils.h"

#include <seastar/core/sstring.hh>
#include <seastar/core/timer.hh>

struct wal_opts {
    explicit wal_opts(
      sstring log_directory,
      timer<>::duration flush_period = std::chrono::seconds(10),
      timer<>::duration max_retention_period = std::chrono::hours(168),
      int64_t max_retention_size = -1 /*infinite*/,
      int32_t max_bytes_in_memory_per_writer = 1024 * 1024,
      int64_t max_log_segment_size = wal_file_size_aligned());
    wal_opts(wal_opts&& o) noexcept;
    wal_opts(const wal_opts& o);

    /// \brief root dir of the WAL
    const sstring directory;
    const timer<>::duration writer_flush_period;
    const timer<>::duration max_retention_period;
    const int64_t max_retention_size;
    const int32_t max_bytes_in_writer_cache;
    const int64_t max_log_segment_size;

    enum class validation_status {
        ok,
        invalid_empty_log_directory,
        invalid_log_segment_4096_multiples,
        invalid_log_segment_size,    // <100MB
        invalid_writer_cache_size,   //
        invalid_retention_period,    // must be >1hr
        invalid_writer_flush_period, // min flush period 2ms
    };
    /// \brief validates the wal_opts
    ///
    static validation_status validate(const wal_opts&);
};

std::ostream& operator<<(std::ostream& o, const wal_opts& opts);
