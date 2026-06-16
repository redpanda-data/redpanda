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

#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>

#include <expected>
#include <functional>
#include <memory>
#include <optional>

namespace cloud_topics::l1 {

/// Result of an index seek. `file_position` is always a byte offset within
/// the object. `delta`, when set, is the log-to-Kafka offset-translation delta
/// at `file_position`, letting the reader translate log offsets to Kafka
/// offsets directly rather than inferring the delta from the first batch; it is
/// `nullopt` when the index already reports positions in Kafka-offset space.
struct seek_result {
    size_t file_position{0};
    /// Bytes from file_position to the end of the readable range.
    /// Always non-zero for a valid seek.
    size_t length{0};
    std::optional<model::offset_delta> delta;
};

/// An index over a single L1 object.
///
/// Abstracts seeking to a byte position by Kafka offset or timestamp,
/// independent of the object's on-disk index format.
class object_index {
public:
    virtual ~object_index() = default;

    /// Return the highest indexed file position such that all prior records
    /// have kafka offsets less than the specified offset.
    ///
    /// If the implementation knows the offset to be greater than all
    /// offsets in the given extent, it may return std::nullopt.
    /// The nullopt is an optimization -- a scan is required otherwise.
    virtual std::optional<seek_result>
      seek_offset_le(model::topic_id_partition, kafka::offset) const = 0;

    /// Return the highest indexed file position such that all prior records
    /// have timestamps less than the specified timestamp.
    ///
    /// If the implementation knows the timestamp to be greater than all
    /// timestamps in the given extent, it may return std::nullopt.
    /// The nullopt is an optimization -- a scan is required otherwise.
    virtual std::optional<seek_result>
      seek_timestamp_le(model::topic_id_partition, model::timestamp) const = 0;
};

/// An open reference to a single L1 object.
///
/// Obtained from l1::open_object. Holds the object's index and can open readers
/// positioned at any seek result.
class object_handle {
public:
    virtual ~object_handle() = default;

    /// Return a reference to the index for this object.
    virtual const object_index& index() const = 0;

    /// Open a reader starting at the seek result returned by index().
    /// `seek.file_position` is always a byte offset; `seek.delta`, when set,
    /// carries the offset delta at the seek point.
    virtual ss::future<std::expected<std::unique_ptr<object_reader>, io::errc>>
    open_reader(const seek_result& seek, ss::abort_source*) = 0;
};

/// Fetches a stream over an object's raw bytes [file_position,
/// file_position+length).
///
/// Used by object_handle implementations to read from the underlying extent.
/// The handle just streams whatever this returns; any chunking (bounding peak
/// memory / cache-chunk granularity) is baked into the fetch by open_object
/// when it builds the handle, not applied by the handle itself. Copyable: the
/// reader (and its data source's copy of this fetch) outlives the handle, so it
/// must not capture the handle.
using fetch_range_fn
  = std::function<ss::future<std::expected<ss::input_stream<char>, io::errc>>(
    size_t file_position, size_t length, ss::abort_source*)>;

/// object_index over a native L1 object's footer.
class l1_footer_index final : public object_index {
public:
    explicit l1_footer_index(footer f);

    std::optional<seek_result>
      seek_offset_le(model::topic_id_partition, kafka::offset) const override;
    std::optional<seek_result> seek_timestamp_le(
      model::topic_id_partition, model::timestamp) const override;

private:
    footer _footer;
};

/// object_handle for a native L1 object. Byte ranges are read through the
/// `fetch` callback supplied by open_object.
class l1_native_object_handle final : public object_handle {
public:
    l1_native_object_handle(footer f, fetch_range_fn fetch);

    const object_index& index() const override { return _index; }

    ss::future<std::expected<std::unique_ptr<object_reader>, io::errc>>
    open_reader(const seek_result& seek, ss::abort_source* as) override;

private:
    l1_footer_index _index;
    fetch_range_fn _fetch;
};

} // namespace cloud_topics::l1
