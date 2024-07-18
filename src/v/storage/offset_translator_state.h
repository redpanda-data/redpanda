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

#include <absl/container/btree_map.h>

namespace storage {

/// Provides offset translation between raw log offsets and offsets not counting
/// filtered batches (basically our internal batch types that we write into the
/// data partitions). This is needed because even though our internal batch
/// types are filtered out when sent to clients, kafka clients are not prepared
/// for these batches to occupy offset space (see
/// https://github.com/redpanda-data/redpanda/issues/1184 for details).
///
/// It works by maintaining an in-memory map of all filtered batch offsets.
/// This map allows us to quickly find a delta between the raw log offset and
/// corresponding translated offset.
class offset_translator_state {
public:
    /// Create an empty translator - the delta between log and kafka offsets is
    /// always 0
    offset_translator_state(model::ntp ntp)
      : _ntp(std::move(ntp)) {}

    /// Create a translator with delta `base_delta` for all offsets strictly
    /// greater than `base_offset`.
    offset_translator_state(
      model::ntp ntp, model::offset base_offset, int64_t base_delta)
      : _ntp(std::move(ntp)) {
        _last_offset2batch.emplace(
          base_offset,
          batch_info{.base_offset = base_offset, .next_delta = base_delta});
    }

    offset_translator_state(const offset_translator_state&) = delete;
    offset_translator_state& operator=(const offset_translator_state&) = delete;
    offset_translator_state(offset_translator_state&&) = default;
    offset_translator_state& operator=(offset_translator_state&&) = default;

    const model::ntp& ntp() const { return _ntp; }

    bool empty() const { return _last_offset2batch.empty(); }

    /// Difference between the log offset and the kafka offset.
    int64_t delta(model::offset) const;

    /// Returns the difference between the log offset and the Kafka offset one
    /// offset past the input.
    model::offset_delta next_offset_delta(model::offset) const;

    /// Translate log offset into kafka offset.
    model::offset from_log_offset(model::offset) const;

    /// Translate kafka offset into log offset.
    model::offset to_log_offset(
      model::offset data_offset, model::offset hint = model::offset{}) const;

    /// Precondition: !empty()
    int64_t last_delta() const;
    model::offset last_gap_offset() const;

    /// Add a filtered batch to the end of the offset translation state.
    void add_gap(model::offset base_offset, model::offset last_offset);

    /// Amend the offset translation state (by appending an artificial batch to
    /// the map) so that delta at `offset` equals to `delta`. Returns true if
    /// the map changed.
    bool add_absolute_delta(model::offset offset, int64_t delta);

    /// Remove all gaps from offset translator
    void reset();

    /// Removes the offset translation state starting from the offset
    /// (inclusive). Returns true if the map changed.
    bool truncate(model::offset);

    /// Removes the offset translation state up to and including the offset. The
    /// offset delta for the next offsets is preserved. Returns true if the map
    /// changed.
    bool prefix_truncate(model::offset);

    iobuf serialize_map() const;
    static offset_translator_state
    from_serialized_map(model::ntp ntp, iobuf buf);

    /// Bootstrap offset translator state from the raft configuration manager
    /// state.
    static offset_translator_state from_bootstrap_state(
      model::ntp, const absl::btree_map<model::offset, int64_t>&);

    friend std::ostream&
    operator<<(std::ostream&, const offset_translator_state&);

private:
    // Represents a non-data batch in the log - a batch that contributes to the
    // difference (aka delta) between log (redpanda) and data (kafka) offset.
    struct batch_info {
        model::offset base_offset;
        // The difference between log and data offsets that we want to find by
        // querying the offset translator. `next_delta` of a _last_offset2batch
        // map element is active for log offsets in the interval (last offset;
        // next last offset] (left end exclusive, right end inclusive).
        int64_t next_delta;
    };

    // Map from the last offset of non-data batches to the corresponding batch
    // info.
    //
    // As prefix truncations happen, we remove elements with keys less than
    // log_start and substitute them with a single element with the key
    // prev_offset(log_start) and next_delta equal to delta(log_start) - this
    // way we can calculate delta for any offset starting from log_start.
    using batches_map_t = absl::btree_map<model::offset, batch_info>;

private:
    model::ntp _ntp;
    batches_map_t _last_offset2batch;
};

} // namespace storage
