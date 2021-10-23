/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "model/fundamental.h"
#include "raft/types.h"
#include "storage/fwd.h"
#include "storage/log.h"
#include "utils/mutex.h"
#include "utils/prefix_logger.h"

#include <seastar/util/bool_class.hh>

#include <absl/container/btree_map.h>

namespace raft {

/// Provides offset translation between raw log offsets and offsets counting
/// only batches with type not in the `filtered_types` set (basically our
/// internal batch types that we write into the data partitions). This is needed
/// because even though our internal batch types are filtered out when sent to
/// clients, kafka clients are not prepared for these batches to occupy offset
/// space (see https://github.com/vectorizedio/redpanda/issues/1184 for
/// details).
///
/// It works by maintaining an in-memory map map of all filtered batch offsets.
/// This map allows us to quickly find a delta between the raw log offset and
/// corresponding translated offset. To avoid reading the whole log at startup,
/// the map is periodically checkpointed to the kvstore. As log truncations/raft
/// snapshots happen, the map is truncated/prefix-truncated along with the log.
///
/// Concurrency note: `start`, `sync_with_log` and `remove_persistent_state`
/// methods can't be called concurrently with other non-const methods and
/// require external synchronization. Other methods are safe to call
/// concurrently with each other.
class offset_translator {
public:
    offset_translator(
      std::vector<model::record_batch_type> filtered_types,
      raft::group_id group,
      model::ntp ntp,
      storage::api& storage_api);

    offset_translator(const offset_translator&) = delete;
    offset_translator& operator=(const offset_translator&) = delete;

    offset_translator(offset_translator&&) noexcept = default;

    /// Translate log offset into kafka offset.
    model::offset from_log_offset(model::offset) const;

    /// Translate kafka offset into log offset.
    model::offset to_log_offset(
      model::offset data_offset, model::offset hint = model::offset{}) const;

    using must_reset = ss::bool_class<struct must_reset_tag>;

    struct bootstrap_state {
        absl::btree_map<model::offset, int64_t> offset2delta;
        model::offset highest_known_offset;
    };

    /// Load persistent state from kvstore. If `reset` is true, resets to an
    /// empty state and persists it. If persistent state is not found, uses
    /// information from `bootstrap_state` and persists it.
    ss::future<> start(must_reset reset, bootstrap_state&&);

    /// Searches for non-data batches up to the tip of the log. After this
    /// method succeeds, offset translator is usable.
    ss::future<> sync_with_log(storage::log, storage::opt_abort_source_t);

    /// Process the batch and add it to offset translation state if it is not
    /// a data batch.
    void process(const model::record_batch&);

    /// Checkpoints offset translation state to the kvstore if enough batches
    /// were processed (to ensure that we have to read only a small amount of
    /// data during the startup)
    ss::future<> maybe_checkpoint();

    /// Removes the offset translation state starting from the offset
    /// (inclusive).
    ss::future<> truncate(model::offset);

    /// Removes the offset translation state up to and including the offset. The
    /// offset delta for the next offsets is preserved.
    ss::future<> prefix_truncate(model::offset);

    /// Removes the offset translation state up to and including the offset. The
    /// offset delta for the next offsets is set to `delta`. If there is offset
    /// translation state for the next offsets, it must be consistent with
    /// `delta`.
    ss::future<> prefix_truncate_reset(model::offset, int64_t delta);

    ss::future<> remove_persistent_state();

    int64_t delta(model::offset) const;

private:
    void do_add(const model::record_batch_header&);

    bytes offsets_map_key() const;
    bytes highest_known_offset_key() const;

    struct batch_info {
        model::offset base_offset;
        int64_t next_delta;
    };

    // Map from the last offset of non-data batches to the corresponding batch
    // info. next_delta in the batch info is active in the log offset interval
    // (last offset; next last offset] (left end exclusive, right end
    // inclusive). As prefix truncations happen, we maintain an invariant that
    // there is always an element of the map with the key prev_offset(start of
    // the log) - this way we can calculate delta for any offset in the log.
    using batches_map_t = absl::btree_map<model::offset, batch_info>;

    iobuf serialize_batches_map(const batches_map_t&);
    batches_map_t deserialize_batches_map(iobuf buf);

    ss::future<> do_checkpoint();

private:
    std::vector<model::record_batch_type> _filtered_types;
    raft::group_id _group;
    model::ntp _ntp;

    prefix_logger _logger;

    batches_map_t _last_offset2batch;

    // The last offset for which we have offset translation data (inclusive).
    model::offset _highest_known_offset;

    size_t _bytes_processed = 0;
    size_t _map_version = 0;

    mutex _checkpoint_lock;

    size_t _bytes_processed_at_checkpoint = 0;
    size_t _map_version_at_checkpoint = 0;

    storage::api& _storage_api;
};

} // namespace raft
