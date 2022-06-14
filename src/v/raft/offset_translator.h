/*
 * Copyright 2021 Redpanda Data, Inc.
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
#include "storage/offset_translator_state.h"
#include "utils/mutex.h"
#include "utils/prefix_logger.h"

#include <seastar/util/bool_class.hh>

#include <absl/container/btree_map.h>

namespace raft {

/// See also comments for storage::offset_translator_state.
///
/// This class maintains offset translation state for the raft log.
/// Filtered batches are those with type not in the `filtered_types` set.
///
/// To avoid reading the whole log at startup, the translation state is
/// periodically checkpointed to the kvstore. As log truncations/raft snapshots
/// happen, the map is truncated/prefix-truncated along with the log.
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

    ss::lw_shared_ptr<const storage::offset_translator_state> state() const {
        return _state;
    }

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

    /// Moves offset translator persistent state from source to target shard.
    /// Note when move state on the source shard is deleted
    static ss::future<> move_persistent_state(
      raft::group_id,
      ss::shard_id source_shard,
      ss::shard_id target_shard,
      ss::sharded<storage::api>&);

    /// Get offset translator storage::kvstore keys. Used only for testing
    bytes offsets_map_key() const;
    bytes highest_known_offset_key() const;

    /// Generate kv-store offset-map key
    static bytes kvstore_offsetmap_key(raft::group_id group);

    /// Generate kv-store highest-known-offset key
    static bytes kvstore_highest_known_offset_key(raft::group_id group);

private:
    ss::future<> do_checkpoint();

private:
    std::vector<model::record_batch_type> _filtered_types;
    ss::lw_shared_ptr<storage::offset_translator_state> _state;

    raft::group_id _group;
    prefix_logger _logger;

    // The last offset for which we have offset translation state (inclusive).
    model::offset _highest_known_offset;

    size_t _bytes_processed = 0;

    // Units issued by the storage resource manager to track how many bytes
    // of data is currently pending checkpoint.
    ss::semaphore_units<> _bytes_processed_units;

    // If true, the storage resource manager has asked us to checkpoint at the
    // next opportunity.
    bool _checkpoint_hint{false};

    size_t _map_version = 0;

    mutex _checkpoint_lock;

    size_t _bytes_processed_at_checkpoint = 0;
    size_t _map_version_at_checkpoint = 0;

    storage::api& _storage_api;
};

} // namespace raft
