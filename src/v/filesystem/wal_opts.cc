#include "wal_opts.h"

#include <iomanip>

#include <boost/filesystem.hpp>
#include <seastar/core/align.hh>
#include <smf/human_bytes.h>

namespace v {

wal_opts::wal_opts(seastar::sstring log,
                   seastar::timer<>::duration flush_period,
                   seastar::timer<>::duration retention_period,
                   int64_t retention_size, int8_t max_writer_pages,
                   int32_t max_bytes_in_memory_per_writer,
                   int64_t max_segment_size)
  : directory(std::move(log)), writer_flush_period(std::move(flush_period)),
    max_retention_period(std::move(retention_period)),
    max_retention_size(retention_size),
    max_writer_concurrency_pages(max_writer_pages),
    max_bytes_in_writer_cache(max_bytes_in_memory_per_writer),
    max_log_segment_size(
      seastar::align_up(max_segment_size, system_page_size())) {}

wal_opts::wal_opts(wal_opts &&o) noexcept
  : directory(std::move(o.directory)),
    writer_flush_period(std::move(o.writer_flush_period)),
    max_retention_period(std::move(o.max_retention_period)),
    max_retention_size(o.max_retention_size),
    max_writer_concurrency_pages(o.max_writer_concurrency_pages),
    max_bytes_in_writer_cache(o.max_bytes_in_writer_cache),
    max_log_segment_size(o.max_log_segment_size) {}

wal_opts::wal_opts(const wal_opts &o)
  : directory(o.directory), writer_flush_period(o.writer_flush_period),
    max_retention_period(o.max_retention_period),
    max_retention_size(o.max_retention_size),
    max_writer_concurrency_pages(o.max_writer_concurrency_pages),
    max_bytes_in_writer_cache(o.max_bytes_in_writer_cache),
    max_log_segment_size(o.max_log_segment_size) {}

wal_opts::validation_status
wal_opts::validate(const wal_opts &o) {
  if ((o.max_log_segment_size % 4096) != 0) {
    return validation_status::invalid_log_segment_4096_multiples;
  }
  if (o.max_log_segment_size < 10 * 1024 * 1024) {
    return validation_status::invalid_log_segment_size;
  }
  if (o.max_bytes_in_writer_cache > 100 * 1024 * 1024) {
    return validation_status::invalid_writer_cache_size;
  }
  if (o.max_writer_concurrency_pages > 10) {
    // Bad IO performance for background flush rate per file to "
    // flush more than 40KB concurrently. Likely all queues of "
    // the disk will get full and your latency will be too high."};
    return validation_status::invalid_writer_concurrent_pages;
  }
  if (std::chrono::duration_cast<std::chrono::hours>(o.max_retention_period)
        .count() <= 1) {
    return validation_status::invalid_retention_period;
  }
  if (std::chrono::duration_cast<std::chrono::milliseconds>(
        o.writer_flush_period)
        .count() <= 1) {
    return validation_status::invalid_writer_flush_period;
  }
  if (o.directory.empty()) {
    return validation_status::invalid_empty_log_directory;
  }
  return validation_status::ok;
}

std::ostream &
operator<<(std::ostream &o, const wal_opts &opts) {
  o << "wal_opts{directory=" << opts.directory << ", writer_flush_period="
    << std::chrono::duration_cast<std::chrono::milliseconds>(
         opts.writer_flush_period)
         .count()
    << "ms, max_retention_period="
    << std::chrono::duration_cast<std::chrono::hours>(opts.max_retention_period)
         .count()
    << "hours, max_retention_size=";
  if (opts.max_retention_size > 0) {
    o << smf::human_bytes(opts.max_retention_size);
  } else {
    o << opts.max_retention_size;
  }
  o << ", max_writer_concurrency_pages="
    // bug with uint8_t displays empty char when assignment is 4
    << (int32_t)opts.max_writer_concurrency_pages
    << ", max_bytes_in_writer_cache="
    << smf::human_bytes(opts.max_bytes_in_writer_cache)
    << ", max_log_segment_size=" << smf::human_bytes(opts.max_log_segment_size)
    << "}";
  return o;
}
}  // namespace v
