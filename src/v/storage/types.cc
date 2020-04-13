#include "storage/types.h"

#include <fmt/core.h>

namespace storage {
std::ostream& operator<<(std::ostream& o, const log_reader_config& cfg) {
    o << "{start_offset:" << cfg.start_offset
      << ", max_offset:" << cfg.max_offset << ", min_bytes:" << cfg.min_bytes
      << ", max_bytes:" << cfg.max_bytes << ", type_filter:";
    if (cfg.type_filter) {
        o << *cfg.type_filter;
    } else {
        o << "nullopt";
    }
    o << ", first_timestamp:";
    if (cfg.first_timestamp) {
        o << *cfg.first_timestamp;
    } else {
        o << "nullopt";
    }
    return o << "}";
}

std::ostream& operator<<(std::ostream& o, const append_result& a) {
    return o << "{append_time:"
             << std::chrono::duration_cast<std::chrono::milliseconds>(
                  a.append_time.time_since_epoch())
                  .count()
             << ", base_offset:" << a.base_offset
             << ", last_offset:" << a.last_offset
             << ", byte_size:" << a.byte_size << "}";
}
std::ostream& operator<<(std::ostream& o, const timequery_result& a) {
    return o << "{offset:" << a.offset << ", time:" << a.time << "}";
}
std::ostream& operator<<(std::ostream& o, const timequery_config& a) {
    return o << "{max_offset:" << a.max_offset << ", time:" << a.time << "}";
}

std::ostream& operator<<(std::ostream& o, const ntp_config& a) {
    o << "{ntp:" << a.ntp << ", compaction:" << a.cstrategy
      << ", segment_size:";
    if (a.segment_size) {
        o << *a.segment_size;
    } else {
        o << "nullopt";
    }
    return o << "}";
}

} // namespace storage
