/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "cloud_storage/offset_index.h"
#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/object_handle.h"
#include "model/fundamental.h"
#include "model/record.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>

#include <expected>
#include <memory>
#include <optional>

namespace cloud_topics::l1 {

/// object_index over a tiered-storage segment's offset_index. Translates a
/// target Kafka offset/timestamp into a byte position plus the offset
/// translation delta at that position using the segment's downloaded .index.
/// At an index entry the delta is the entry's (log - Kafka) offset; a seek
/// before the first index entry (or with no index at all) returns the segment
/// start, whose delta is the segment base delta (delta_base). Returning the
/// base delta -- rather than the base Kafka offset for the reader to infer the
/// delta from the first batch -- keeps translation correct when compaction has
/// removed the segment's leading records. (find_kaf_offset/find_timestamp are
/// non-const on offset_index -- they flush a write buffer internally -- so
/// _index is mutable.)
///
/// Seeks are not upper-bounded by the segment's last offset: the caller only
/// dispatches a target the segment's extent range covers (the metastore read
/// path discards objects whose last_offset is below the target), so the bound
/// would never reject anything.
///
/// A timestamp seek assumes the segment's batch timestamps rise. Each entry
/// records only its own batch's maximum, so a higher timestamp earlier in the
/// segment leaves no trace the search can find, and the seek can resolve past
/// the batch that matches. This is inherited rather than introduced: it is how
/// cloud_storage reads the same index (remote_segment::maybe_get_offsets has no
/// monotonicity guard), while local storage records the condition in
/// index_state's batch_timestamps_are_monotonic and scans instead of seeking
/// when it is false. A native L1 footer entry carries a running maximum and so
/// does not have the problem.
class ts_segment_index final : public object_index {
public:
    ts_segment_index(
      cloud_storage::offset_index index,
      model::offset_delta delta_base,
      size_t segment_size);

    std::optional<seek_result> seek_offset_le(
      model::topic_id_partition, kafka::offset target) const override;

    std::optional<seek_result> seek_timestamp_le(
      model::topic_id_partition, model::timestamp) const override;

private:
    mutable cloud_storage::offset_index _index;
    model::offset_delta _delta_base;
    size_t _segment_size;
};

/// object_handle for an imported tiered-storage segment. The byte transport is
/// injected as a fetch callback so the seek/index dispatch and the
/// tiered_storage_object_reader wiring (offset delta, term, aborted-range
/// strip) are shared across backends: file_io supplies a download-from-bucket
/// fetch; fake_io supplies an in-memory one. Any chunking of the transport is
/// baked into `fetch` by open_object; the handle just streams what it returns.
class ts_object_handle final : public object_handle {
public:
    ts_object_handle(
      std::unique_ptr<object_index> index,
      model::term_id term,
      aborted_transactions aborted,
      fetch_range_fn fetch);

    const object_index& index() const override { return *_index; }

    ss::future<std::expected<std::unique_ptr<object_reader>, io::errc>>
    open_reader(const seek_result& seek, ss::abort_source* as) override;

private:
    std::unique_ptr<object_index> _index;
    model::term_id _term;
    aborted_transactions _aborted;
    fetch_range_fn _fetch;
};

} // namespace cloud_topics::l1
