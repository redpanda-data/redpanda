/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/vassert.h"
#include "cluster/partition.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/shared_ptr.hh>

#include <boost/iterator/counting_iterator.hpp>
#include <boost/iterator/transform_iterator.hpp>

#include <utility>

namespace archival {

namespace detail {
inline model::offset cast_to_offset(int64_t i) noexcept {
    return model::offset(i);
}
} // namespace detail

/// Representation of the inclusive monotonic offset range
struct inclusive_offset_range {
    model::offset base;
    model::offset last;

    inclusive_offset_range() = default;

    inclusive_offset_range(model::offset base, model::offset last)
      : base(base)
      , last(last) {
        vassert(base <= last, "Invalid offset range {} - {}", base, last);
    }

    bool operator<=>(const inclusive_offset_range&) const = default;

    int64_t size() const noexcept { return last() - base() + 1; }

    bool contains(const model::record_batch_header& hdr) const noexcept {
        return base <= hdr.base_offset && hdr.last_offset() <= last;
    }

    auto begin() const noexcept {
        auto cit = boost::make_counting_iterator<int64_t>(base());
        return boost::make_transform_iterator(cit, &detail::cast_to_offset);
    }

    auto end() const noexcept {
        auto cit = boost::make_counting_iterator<int64_t>(last() + 1);
        return boost::make_transform_iterator(cit, &detail::cast_to_offset);
    }
};

/// Representation of the inclusive monotonic offset range limited by
/// on-disk size.
struct size_limited_offset_range {
    model::offset base;
    size_t min_size;
    size_t max_size;

    size_limited_offset_range(
      model::offset base,
      size_t max_size,
      std::optional<size_t> min_size = std::nullopt)
      : base(base)
      , min_size(min_size.value_or(0))
      , max_size(max_size) {}

    bool operator<=>(const size_limited_offset_range&) const = default;
};

inline std::ostream& operator<<(std::ostream& o, inclusive_offset_range r) {
    fmt::print(o, "[{}-{}]", r.base, r.last);
    return o;
}

/// Result of the upload size calculation.
/// Contains size of the region in bytes and locks.
struct upload_reconciliation_result {
    /// Size of the upload in bytes
    size_t size_bytes;
    /// True if at least one segment is compacted
    bool is_compacted;
    /// Offset range of the segment
    inclusive_offset_range offsets;
};

/// Individual segment upload
class segment_upload {
public:
    segment_upload(const segment_upload&) = delete;
    segment_upload(segment_upload&&) noexcept = delete;
    segment_upload& operator=(const segment_upload&) = delete;
    segment_upload& operator=(segment_upload&&) noexcept = delete;

    /// Close upload and free resources
    ss::future<> close();

    /// Get input stream or throw
    ///
    /// The method detaches the input stream and returns it. It's also closes
    /// the 'segment_upload' object (which should be a no-op at this point since
    /// all async operations are completed). If 'detach_stream' was called the
    /// caller no longer has to call 'close' on the 'segment_upload' object. If
    /// the method was never called the caller has to call 'close' explicitly.
    ss::future<ss::input_stream<char>> detach_stream() && {
        throw_if_not_initialized("get_stream");
        auto tmp = std::exchange(_stream, std::nullopt);
        co_await close();
        co_return std::move(tmp.value());
    }

    /// Get precise content length for the upload
    size_t get_size_bytes() const {
        throw_if_not_initialized("get_size_bytes");
        return _params->size_bytes;
    }

    /// Get upload metadata
    ///
    /// \returns cref of the metadata
    const upload_reconciliation_result get_meta() const noexcept {
        throw_if_not_initialized("get_meta");
        upload_reconciliation_result p{
          .size_bytes = _params.value().size_bytes,
          .is_compacted = _params.value().is_compacted,
          .offsets = _params.value().offsets,
        };
        return p;
    }

    /// \brief Make new segment upload
    ///
    /// \note Make an upload that begins and ends at precise offsets.
    /// \param part is a partition
    /// \param range is an offset range to upload
    /// \param read_buffer_size is a buffer size used to upload data
    /// \param sg is a scheduling group used to upload the data
    /// \param deadline is a deadline for the upload object to be created (not
    ///                 for the actual upload to happen)
    /// \return initialized segment_upload object
    static ss::future<result<std::unique_ptr<segment_upload>>>
    make_segment_upload(
      ss::lw_shared_ptr<cluster::partition> part,
      inclusive_offset_range range,
      size_t read_buffer_size,
      ss::scheduling_group sg,
      model::timeout_clock::time_point deadline);

    /// \brief Make new segment upload
    ///
    /// \note Make an upload that begins at certain offset and has certain size.
    /// This method is supposed to be used when the new upload is started.
    /// E.g. when we want to upload 1GiB segments we will use this code:
    ///
    /// \code
    /// auto upl = co_await segment_upload::make_segment_upload(
    ///                         _part,
    ///                         size_limited_offset_range(last_offset, 1_GiB),
    ///                         0x8000,
    ///                         _sg,
    ///                         10s);
    /// \endcode
    ///
    /// Note that the precision of this method is limited by index sampling
    /// rate. End of every upload created this way will be aligned with the
    /// index.
    ///
    /// \param part is a partition
    /// \param range is an offset range to upload
    /// \param read_buffer_size is a buffer size used to upload data
    /// \param sg is a scheduling group used to upload the data
    /// \param deadline is a deadline for the upload object to be created (not
    ///                 for the actual upload to happen)
    /// \return initialized segment_upload object
    static ss::future<result<std::unique_ptr<segment_upload>>>
    make_segment_upload(
      ss::lw_shared_ptr<cluster::partition> part,
      size_limited_offset_range range,
      size_t read_buffer_size,
      ss::scheduling_group sg,
      model::timeout_clock::time_point deadline);

private:
    explicit segment_upload(
      ss::lw_shared_ptr<cluster::partition> part,
      size_t read_buffer_size,
      ss::scheduling_group sg);

    /// Initialize segment upload using offset range
    ss::future<result<void>> initialize(
      inclusive_offset_range offsets,
      model::timeout_clock::time_point deadline);

    /// Initialize segment upload using offset range
    ss::future<result<void>> initialize(
      size_limited_offset_range range,
      model::timeout_clock::time_point deadline);

    void throw_if_not_initialized(std::string_view caller) const;

    /// Calculate upload size using segment indexes
    ss::future<result<upload_reconciliation_result>> compute_upload_parameters(
      std::variant<inclusive_offset_range, size_limited_offset_range> range);

    model::ntp _ntp;
    ss::lw_shared_ptr<cluster::partition> _part;
    size_t _rd_buffer_size;
    ss::scheduling_group _sg;
    std::optional<ss::input_stream<char>> _stream;
    std::optional<upload_reconciliation_result> _params;
    ss::gate _gate;
    ss::abort_source _as;
    retry_chain_node _rtc;
    retry_chain_logger _ctxlog;
};

} // namespace archival
