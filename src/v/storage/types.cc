// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/types.h"

#include "base/format_to.h"
#include "base/vlog.h"
#include "storage/compacted_index.h"
#include "storage/logger.h"
#include "storage/ntp_config.h"

#include <fmt/core.h>

namespace storage {

model::offset stm_hookset::max_removable_local_log_offset() {
    if (!has_started()) {
        return model::offset::min();
    }
    model::offset result = model::offset::max();
    for (const auto& stm : _stms) {
        auto mco = stm->max_removable_local_log_offset();
        result = std::min(result, mco);
        vlog(
          stlog.trace,
          "max_removable_local_log_offset[{}] = {}",
          stm->name(),
          mco);
    }
    return result;
}

model::offset stm_hookset::tx_snapshot_offset() const {
    if (!has_started()) {
        vlog(
          stlog.debug,
          "attempt to get tx_snapshot_offset before stm_hookset is started");
        return model::offset::min();
    }
    if (_tx_stm) {
        return _tx_stm->last_locally_snapshotted_offset();
    }
    return model::offset::max();
}

std::optional<kafka::offset> stm_hookset::lowest_pinned_data_offset() const {
    if (!has_started()) {
        vlog(
          stlog.debug,
          "attempt to get lowest_pinned_data_offset before stm_hookset is "
          "started");
        return kafka::offset::min();
    }
    std::optional<kafka::offset> result;
    for (const auto& stm : _stms) {
        auto pinned = stm->lowest_pinned_data_offset();
        if (pinned) {
            result = std::min(*pinned, result.value_or(kafka::offset::max()));
        }
    }
    return result;
}

model::offset stm_hookset::max_tombstone_remove_offset() const {
    return _max_tombstone_remove_offset;
}

void stm_hookset::set_max_tombstone_remove_offset(model::offset o) {
    _max_tombstone_remove_offset = o;
}

model::offset stm_hookset::max_tx_end_remove_offset() const {
    return _max_tx_end_remove_offset;
}

void stm_hookset::set_max_tx_end_remove_offset(model::offset o) {
    _max_tx_end_remove_offset = o;
}

bool stm_hookset::is_batch_in_idempotent_window(
  const model::record_batch_header& hdr) const {
    check_status("is_batch_in_idempotent_window");
    if (!_tx_stm) {
        return false;
    }
    return _tx_stm->is_batch_in_idempotent_window(hdr);
}

fmt::iterator local_log_reader_config::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "start_offset:{}, max_offset:{}, max_bytes:{}, "
      "strict_max_bytes:{}, type_filter: {}, first_timestamp:{}, "
      "bytes_consumed:{}, over_budget:{}, skip_batch_cache:{}, "
      "skip_readers_cache:{}, abortable:{}, client_address:{}",
      start_offset,
      max_offset,
      max_bytes,
      strict_max_bytes,
      type_filter,
      timestamp,
      bytes_consumed,
      over_budget,
      skip_batch_cache,
      skip_readers_cache,
      abort_source.has_value(),
      client_address);
}

fmt::iterator append_result::format_to(fmt::iterator it) const {
    auto append_dur = std::chrono::duration_cast<std::chrono::milliseconds>(
      log_clock::now() - append_time);
    return fmt::format_to(
      it,
      "{{time_since_append: {}, base_offset: {}, last_offset: {}, last_term: "
      "{}, "
      "byte_size: {}}}",
      append_dur,
      base_offset,
      last_offset,
      last_term,
      byte_size);
}
fmt::iterator timequery_result::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{term:{}, offset:{}, time:{}}}", term, offset, time);
}
fmt::iterator timequery_config::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{min_offset: {}, max_offset: {}, time:{}, type_filter:{}}}",
      min_offset,
      max_offset,
      time,
      type_filter);
}

fmt::iterator ntp_config::default_overrides::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{compaction_strategy: {}, cleanup_policy_bitflags: {}, segment_size: "
      "{}, retention_bytes: {}, retention_time_ms: {}, recovery_enabled: {}, "
      "retention_local_target_bytes: {}, retention_local_target_ms: {}, "
      "remote_delete: {}, segment_ms: {}, "
      "initial_retention_local_target_bytes: {}, "
      "initial_retention_local_target_ms: {}, write_caching: {}, flush_ms: {}, "
      "flush_bytes: {}, iceberg_mode: {}, remote_allow_gaps: {}, "
      "delete_retention_ms: {}, min_cleanable_dirty_ratio: {}, "
      "min_compaction_lag_ms: {}, max_compaction_lag_ms: {}, storage_mode: {}, "
      "migrated_from: {} "
      "}}",
      compaction_strategy,
      cleanup_policy_bitflags,
      segment_size,
      retention_bytes,
      retention_time,
      recovery_enabled,
      retention_local_target_bytes,
      retention_local_target_ms,
      remote_delete,
      segment_ms,
      initial_retention_local_target_bytes,
      initial_retention_local_target_ms,
      write_caching,
      flush_ms,
      flush_bytes,
      iceberg_mode,
      remote_allow_gaps,
      delete_retention_ms,
      min_cleanable_dirty_ratio,
      min_compaction_lag_ms,
      max_compaction_lag_ms,
      storage_mode,
      migrated_from);
}

fmt::iterator ntp_config::format_to(fmt::iterator it) const {
    if (has_overrides()) {
        return fmt::format_to(
          it,
          "{{ntp: {}, base_dir: {}, overrides: {}, revision: {}, "
          "topic_revision: {}, remote_revision: {}}}",
          ntp(),
          base_directory(),
          get_overrides(),
          get_revision(),
          get_topic_revision(),
          get_remote_revision());
    } else {
        return fmt::format_to(
          it,
          "{{ntp: {}, base_dir: {}, overrides: nullptr, revision: {}, "
          "topic_revision: {}, remote_revision: {}}}",
          ntp(),
          base_directory(),
          get_revision(),
          get_topic_revision(),
          get_remote_revision());
    }
}

fmt::iterator truncate_config::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{base_offset:{}}}", base_offset);
}
fmt::iterator truncate_prefix_config::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{start_offset:{}, force_truncate_delta:{}}}",
      start_offset,
      force_truncate_delta);
}

fmt::iterator offset_stats::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{start_offset:{}, committed_offset:{}, "
      "committed_offset_term:{}, dirty_offset:{}, dirty_offset_term:{}}}",
      start_offset,
      committed_offset,
      committed_offset_term,
      dirty_offset,
      dirty_offset_term);
}

fmt::iterator gc_config::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{eviction_time:{}, max_bytes:{}}}",
      eviction_time,
      max_bytes.value_or(-1));
}

fmt::iterator housekeeping_config::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{compact:{}, gc:{}}}", compact, gc);
}

fmt::iterator compaction_result::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{executed_compaction: {}, size_before: {}, size_after: {}}}",
      executed_compaction,
      size_before,
      size_after);
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
