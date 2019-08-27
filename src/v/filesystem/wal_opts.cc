#include "filesystem/wal_opts.h"

std::ostream& operator<<(std::ostream& o, const wal_opts& opts) {
    o << "wal_opts{directory=" << opts.directory() << ", writer_flush_period="
      << std::chrono::duration_cast<std::chrono::milliseconds>(
           opts.writer_flush_period())
           .count()
      << "ms, max_retention_period="
      << std::chrono::duration_cast<std::chrono::hours>(
           opts.max_retention_period())
           .count()
      << "hours, max_retention_size=";
    if (opts.max_retention_size() > 0) {
        o << smf::human_bytes(opts.max_retention_size());
    } else {
        o << opts.max_retention_size();
    }
    o << ", max_bytes_in_writer_cache="
      << smf::human_bytes(opts.max_bytes_in_writer_cache())
      << ", max_log_segment_size="
      << smf::human_bytes(opts.max_log_segment_size()) << "}";
    return o;
}
