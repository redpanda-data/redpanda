#include "storage/types.h"

#include "utils/to_string.h"

#include <fmt/core.h>
#include <fmt/ostream.h>

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

std::ostream&
operator<<(std::ostream& o, const ntp_config::default_overrides& v) {
    fmt::print(
      o,
      "{{compaction_strategy: {}, cleanup_policy_bitflags: {}, segment_size: "
      "{}, retention_bytes: {}, retention_time_ms: {}}}",
      v.compaction_strategy,
      v.cleanup_policy_bitflags,
      v.segment_size,
      v.retention_bytes,
      v.retention_time);

    return o;
}

std::ostream& operator<<(std::ostream& o, const ntp_config& v) {
    o << "{ntp:" << v.ntp() << ", base_dir:" << v.base_directory()
      << ", overrides:";
    if (v.has_overrides()) {
        o << v.get_overrides();
    } else {
        o << "nullptr";
    }
    return o << "}";
}

std::ostream& operator<<(std::ostream& o, const truncate_config& cfg) {
    fmt::print(o, "{{base_offset:{}}}", cfg.base_offset);
    return o;
}
std::ostream& operator<<(std::ostream& o, const truncate_prefix_config& cfg) {
    fmt::print(
      o, "{{max_offset:{}, sloppy_prefix:{}}}", cfg.max_offset, cfg.sloppy);
    return o;
}

std::ostream& operator<<(std::ostream& o, const offset_stats& s) {
    fmt::print(
      o,
      "{{start_offset:{}, start_offset_term:{}, committed_offset:{}, "
      "committed_offset_term:{}, dirty_offset:{}, dirty_offset_term:{}, "
      "last_term_start_offset:{}}}",
      s.start_offset,
      s.start_offset_term,
      s.committed_offset,
      s.committed_offset_term,
      s.dirty_offset,
      s.dirty_offset_term,
      s.last_term_start_offset);
    return o;
}
} // namespace storage
