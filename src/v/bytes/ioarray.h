/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "absl/container/fixed_array.h"
#include "base/seastarx.h"
#include "base/units.h"
#include "bytes/iobuf.h"

#include <seastar/core/future.hh>
#include <seastar/core/temporary_buffer.hh>

#include <compare>
#include <ranges>

struct iovec;

// A fixed size chunked array of data normally for IO operations.
//
// Namely this class gives a way to represent a large array of data that is
// randomly accessable, but is not necessarily contiguous in memory.
class ioarray {
public:
    // The max size of a chunk. The last chunk may be smaller than this.
    constexpr static size_t max_chunk_size = 128_KiB;

    // Create an empty ioarray.
    ioarray()
      : ioarray(0) {}
    ioarray(const ioarray&) = delete;
    ioarray& operator=(const ioarray&) = delete;
    ioarray(ioarray&&) noexcept = default;
    ioarray& operator=(ioarray&& o) noexcept;
    ~ioarray() = default;

    // Create an ioarray with a specified size, note that the data is
    // uninitialized.
    explicit ioarray(size_t size);

    // String-like view of a portion of the ioarray.
    //
    // Since strings must be less than 128_KiB (because they are normally
    // contiguous), this object can hold a view of any chunk of data in the
    // ioarray that is less than 128_KiB. Since chunks are all 128_KiB (except
    // the last buffer, which may be smaller), this means that 2 continugous
    // chunks can represent any string-like view within contents.
    class string_view {
    public:
        string_view() = default;
        explicit string_view(std::string_view s) { _views[0] = s; }
        explicit string_view(std::string_view a, std::string_view b)
          : _views(std::to_array({a, b})) {}

        explicit operator ss::sstring() const;
        bool operator==(const string_view& other) const = default;
        auto operator<=>(const string_view& other) const = default;
        bool operator==(std::string_view other) const;
        std::strong_ordering operator<=>(std::string_view other) const;
        size_t size() const { return _views[0].size() + _views[1].size(); }
        bool empty() const { return _views[0].empty() && _views[1].empty(); }
        auto as_range() const { return _views | std::views::join; }
        auto begin() const { return _views.begin(); }
        auto end() const { return _views.end(); }

    private:
        std::array<std::string_view, 2> _views;
    };

    // Create an ioarray by sharing the passed in to be buffers into
    // a new ioarray.
    //
    // REQUIRES: All but the last buffer must be `max_chunk_size` in length.
    static ioarray from_sized_buffers(std::span<ss::temporary_buffer<char>>);

    // Create an uninitialized ioarray where all the chunks are aligned.
    // It's assumed that the alignment is a power of two less than
    // `max_chunk_size` and that size is a multiple of alignment.
    static ioarray aligned(size_t alignment, size_t size);

    // Concatenate two ioarrays. Both inputs are consumed.
    //
    // Fast path (zero-copy): when a's data ends on a chunk boundary
    // (offset + size is a multiple of max_chunk_size) and b has offset 0,
    // the underlying buffers are moved directly.
    //
    // Slow path (copy): otherwise, both arrays are copied byte-by-byte into
    // a fresh ioarray.
    static ioarray concat(ioarray a, ioarray b);

    // Create the ioarray from copying out of an iobuf.
    //
    // NOTE: This intended to be used for testing.
    static ioarray copy_from(const iobuf&);

    // Share a portion of ioarray.
    //
    // NOTE: This ioarray lengthens the lifetime of the data backing contents,
    // so it's recommended to use this only for a short time, or to make a copy.
    //
    // If this ioarray was allocated aligned, the returned array is no longer
    // guaranteed to be aligned.
    ioarray share(size_t pos, size_t length);

    // Convert the ioarray to an iobuf.
    //
    // The underlying buffers are shared.
    iobuf as_iobuf();

    // Get a view of the contents as a string-like object.
    //
    // REQUIRES: length <= max_buffer_size
    string_view read_string(size_t pos, size_t length) const;

    char& operator[](size_t i);
    char operator[](size_t i) const;

    // Read a little endian fixed32 bit integer from the ioarray.
    //
    // REQUIRES: i + sizeof(uint32_t) < size
    uint32_t read_fixed32(size_t i) const;

    // Remove the last n bytes from this ioarray.
    void trim_back(size_t n);

    // The size of the ioarray.
    size_t size() const { return _size; }
    // If the ioarray is empty.
    bool empty() const { return _size == 0; }
    // Turn the ioarray into a std::range for usage in algorithms.
    auto as_range() const {
        return _buffers | std::views::join | std::views::drop(_offset)
               | std::views::take(_size);
    }
    // Return this ioarray for scatter/gather IO.
    std::vector<::iovec> as_iovec();

private:
    // An internal constructor for creating an ioarray without initializing data
    // in _buffers (the tmp bufs are always uninitialized).
    struct uninitialized_t {};
    ioarray(uninitialized_t, size_t size);

    size_t _offset;
    size_t _size;
    absl::FixedArray<ss::temporary_buffer<char>> _buffers;
};
