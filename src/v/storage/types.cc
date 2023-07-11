// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/types.h"

#include "storage/compacted_index.h"
#include "storage/logger.h"
#include "storage/ntp_config.h"
#include "utils/human.h"
#include "utils/to_string.h"
#include "vlog.h"

#include <fmt/core.h>
#include <fmt/ostream.h>

namespace storage {

model::offset stm_manager::max_collectible_offset() {
    model::offset result = model::offset::max();
    for (const auto& stm : _stms) {
        auto mco = stm->max_collectible_offset();
        result = std::min(result, mco);
        vlog(stlog.trace, "max_collectible_offset[{}] = {}", stm->name(), mco);
    }
    return result;
}

std::ostream& operator<<(std::ostream& o, const disk& d) {
    fmt::print(
      o,
      "{{path: {}, free: {}, total: {}, alert: {}, fsid: {}}}",
      d.path,
      human::bytes(d.free),
      human::bytes(d.total),
      d.alert,
      d.fsid);
    return o;
}

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
    o << ", bytes_consumed:" << cfg.bytes_consumed;
    o << ", over_budget:" << cfg.over_budget;
    o << ", strict_max_bytes:" << cfg.strict_max_bytes;
    o << ", skip_batch_cache:" << cfg.skip_batch_cache;
    o << ", abortable:" << cfg.abort_source.has_value();
    o << ", aborted:"
      << (cfg.abort_source.has_value()
            ? cfg.abort_source.value().get().abort_requested()
            : false);
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
    o << "{max_offset:" << a.max_offset << ", time:" << a.time
      << ", type_filter:";
    if (a.type_filter) {
        o << *a.type_filter;
    } else {
        o << "nullopt";
    }
    o << "}";
    return o;
}

std::ostream&
operator<<(std::ostream& o, const ntp_config::default_overrides& v) {
    fmt::print(
      o,
      "{{compaction_strategy: {}, cleanup_policy_bitflags: {}, segment_size: "
      "{}, retention_bytes: {}, retention_time_ms: {}, recovery_enabled: {}, "
      "retention_local_target_bytes: {}, retention_local_target_ms: {}, "
      "remote_delete: {}, segment_ms: {}}}",
      v.compaction_strategy,
      v.cleanup_policy_bitflags,
      v.segment_size,
      v.retention_bytes,
      v.retention_time,
      v.recovery_enabled,
      v.retention_local_target_bytes,
      v.retention_local_target_ms,
      v.remote_delete,
      v.segment_ms);

    return o;
}

std::ostream& operator<<(std::ostream& o, const ntp_config& v) {
    if (v.has_overrides()) {
        fmt::print(
          o,
          "{{ntp: {}, base_dir: {}, overrides: {}, revision: {}, "
          "initial_revision: {}}}",
          v.ntp(),
          v.base_directory(),
          v.get_overrides(),
          v.get_revision(),
          v.get_initial_revision());
    } else {
        fmt::print(
          o,
          "{{ntp: {}, base_dir: {}, overrides: nullptr, revision: {}, "
          "initial_revision: {}}}",
          v.ntp(),
          v.base_directory(),
          v.get_revision(),
          v.get_initial_revision());
    }
    return o;
}

std::ostream& operator<<(std::ostream& o, const truncate_config& cfg) {
    fmt::print(o, "{{base_offset:{}}}", cfg.base_offset);
    return o;
}
std::ostream& operator<<(std::ostream& o, const truncate_prefix_config& cfg) {
    fmt::print(o, "{{start_offset:{}}}", cfg.start_offset);
    return o;
}

std::ostream& operator<<(std::ostream& o, const offset_stats& s) {
    fmt::print(
      o,
      "{{start_offset:{}, committed_offset:{}, "
      "committed_offset_term:{}, dirty_offset:{}, dirty_offset_term:{}}}",
      s.start_offset,
      s.committed_offset,
      s.committed_offset_term,
      s.dirty_offset,
      s.dirty_offset_term);
    return o;
}

std::ostream& operator<<(std::ostream& os, const gc_config& cfg) {
    fmt::print(
      os,
      "{{eviction_time:{}, max_bytes:{}}}",
      cfg.eviction_time,
      cfg.max_bytes.value_or(-1));
    return os;
}

std::ostream& operator<<(std::ostream& o, const compaction_config& c) {
    fmt::print(
      o,
      "{{max_collectible_offset:{}, "
      "should_sanitize:{}}}",
      c.max_collectible_offset,
      c.sanitizer_config);
    return o;
}

std::ostream& operator<<(std::ostream& os, const housekeeping_config& cfg) {
    fmt::print(os, "{{compact:{}, gc:{}}}", cfg.compact, cfg.gc);
    return os;
}

std::ostream& operator<<(std::ostream& o, const compaction_result& r) {
    fmt::print(
      o,
      "{{executed_compaction: {}, size_before: {}, size_after: {}}}",
      r.executed_compaction,
      r.size_before,
      r.size_after);
    return o;
}

std::ostream&
operator<<(std::ostream& o, compacted_index::recovery_state state) {
    switch (state) {
    case compacted_index::recovery_state::index_missing:
        return o << "index_missing";
    case compacted_index::recovery_state::already_compacted:
        return o << "already_compacted";
    case compacted_index::recovery_state::index_needs_rebuild:
        return o << "index_needs_rebuild";
    case compacted_index::recovery_state::index_recovered:
        return o << "index_recovered";
    }
    __builtin_unreachable();
}

} // namespace storage
