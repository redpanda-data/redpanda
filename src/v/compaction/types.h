// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "compaction/key_offset_map.h"
#include "model/fundamental.h"
#include "storage/file_sanitizer_types.h"
#include "storage/scoped_file_tracker.h"

#include <seastar/core/abort_source.hh>

#include <chrono>
#include <optional>

namespace compaction {

struct compaction_config {
    compaction_config(
      model::offset max_collect_offset,
      model::offset max_tombstone_remove_offset,
      model::offset max_tx_end_remove_offset,
      std::optional<std::chrono::milliseconds> tombstone_ret_ms,
      std::optional<std::chrono::milliseconds> tx_ret_ms,
      ss::abort_source& as,
      std::optional<storage::ntp_sanitizer_config> san_cfg = std::nullopt,
      std::optional<size_t> max_keys = std::nullopt,
      std::chrono::milliseconds min_lag_ms = std::chrono::milliseconds{0},
      hash_key_offset_map* key_map = nullptr,
      storage::scoped_file_tracker::set_t* to_clean = nullptr)
      : max_removable_local_log_offset(max_collect_offset)
      , max_tombstone_remove_offset(max_tombstone_remove_offset)
      , max_tx_end_remove_offset(max_tx_end_remove_offset)
      , tombstone_retention_ms(tombstone_ret_ms)
      , tx_retention_ms(tx_ret_ms)
      , sanitizer_config(std::move(san_cfg))
      , key_offset_map_max_keys(max_keys)
      , min_lag_ms(min_lag_ms)
      , hash_key_map(key_map)
      , files_to_cleanup(to_clean)
      , asrc(&as) {}

    // Cannot delete or compact past this offset (i.e. for unresolved txn
    // records): that is, only offsets <= this may be compacted.
    model::offset max_removable_local_log_offset;

    // Cannot remove tombstones past this offset
    model::offset max_tombstone_remove_offset;

    // Cannot remove transaction end markers (commit/abort) past this offset
    model::offset max_tx_end_remove_offset;

    // The retention time for tombstones. Tombstone removal occurs only for
    // "clean" compacted segments past the tombstone deletion horizon timestamp,
    // which is a segment's `clean_compact_timestamp + tombstone_retention_ms`.
    // This means tombstones take at least two rounds of compaction to remove a
    // tombstone: at least one pass to make a segment clean, and another pass
    // some time after tombstone_retention_ms to remove tombstones.
    //
    // Tombstone removal is only supported for topics with remote writes
    // disabled. As a result, this field will only have a value for compaction
    // ran on non-archival topics.
    std::optional<std::chrono::milliseconds> tombstone_retention_ms;

    // The retention time for transactional markers. Tombstone removal occurs
    // for self compacted segments past the transaction deletion horizon
    // timestamp, which is a segment's self_compact_timestamp +
    // tombstone_retention_ms. Similar to tombstone removal, there are two
    // passes involved in removing transaction batches:
    // 1. The first pass removes the `tx_fence` batch and unsets all the
    // transactional bit in all raft data batch headers, and sets
    // `self_compact_timestamp`.
    // 2. The second pass removes the `commit/abort` control batch after the
    // time horizon mentioned above is passed.
    std::optional<std::chrono::milliseconds> tx_retention_ms;

    // use proxy fileops with assertions and/or failure injection
    std::optional<storage::ntp_sanitizer_config> sanitizer_config;

    // Limit the number of keys stored by a compaction's key-offset map.
    std::optional<size_t> key_offset_map_max_keys;

    // The value of min.compaction.lag.ms for this compaction.
    std::chrono::milliseconds min_lag_ms;

    // Hash key-offset map to reuse across compactions.
    hash_key_offset_map* hash_key_map;

    // Set of intermediary files added by compactions that need to be removed,
    // e.g. because they were leftover from an aborted compaction.
    storage::scoped_file_tracker::set_t* files_to_cleanup;

    // abort source for compaction task
    ss::abort_source* asrc;

    friend std::ostream& operator<<(std::ostream&, const compaction_config&);
};

// POD containing compaction statistics from a `compaction::filter`'s run.
struct stats {
    // Total number of batches passed to this reducer.
    size_t batches_processed{0};
    // Number of batches that were completely removed.
    size_t batches_discarded{0};
    // Number of records removed by this reducer, including batches that
    // were entirely removed.
    size_t records_discarded{0};
    // Number of batches that were ignored because they are not
    // of a compactible type.
    size_t non_compactible_batches{0};
    // Number of transactional control batches that were removed.
    // This is only relevant for local storage compaction.
    size_t control_batches_discarded{0};
    // Number of tombstone records that were removed due to expiration (not
    // including those removed by de-duplication)
    size_t expired_tombstones_discarded{0};

    // Returns whether any data was removed by this reducer.
    bool has_removed_data() const {
        return batches_discarded > 0 || records_discarded > 0
               || control_batches_discarded > 0
               || expired_tombstones_discarded > 0;
    }

    friend std::ostream& operator<<(std::ostream& os, const stats& s);
};

} // namespace compaction
