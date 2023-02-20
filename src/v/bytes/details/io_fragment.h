/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/details/out_of_range.h"
#include "seastarx.h"
#include "utils/intrusive_list_helpers.h"

#include <seastar/core/temporary_buffer.hh>

namespace details {
class io_fragment {
public:
    struct full {};
    struct empty {};

    io_fragment(ss::temporary_buffer<char> buf, full)
      : _buf(std::move(buf))
      , _used_bytes(_buf.size()) {}
    io_fragment(ss::temporary_buffer<char> buf, empty)
      : _buf(std::move(buf))
      , _used_bytes(0) {}
    io_fragment(io_fragment&& o) noexcept = delete;
    io_fragment& operator=(io_fragment&& o) noexcept = delete;
    io_fragment(const io_fragment& o) = delete;
    io_fragment& operator=(const io_fragment& o) = delete;
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
    ~io_fragment() noexcept = default;
#pragma GCC diagnostic pop

    bool operator==(const io_fragment& o) const {
        return _used_bytes == o._used_bytes && _buf == o._buf;
    }
    bool operator!=(const io_fragment& o) const { return !(*this == o); }
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
    bool is_empty() const { return _used_bytes == 0; }
#pragma GCC diagnostic pop
    size_t available_bytes() const { return _buf.size() - _used_bytes; }
    void reserve(size_t reservation) {
        check_out_of_range(reservation, available_bytes());
        _used_bytes += reservation;
    }
    size_t size() const { return _used_bytes; }
    size_t capacity() const { return _buf.size(); }

    const char* get() const {
        // required for the networking layer to conver to
        // scattered message without copying data
        return _buf.get();
    }
    size_t append(const char* src, size_t len) {
        const size_t sz = std::min(len, available_bytes());
        std::copy_n(src, sz, get_current());
        _used_bytes += sz;
        return sz;
    }
    ss::temporary_buffer<char> share() {
        // needed for output_stream<char> wrapper
        return _buf.share(0, _used_bytes);
    }
    ss::temporary_buffer<char> share(size_t pos, size_t len) {
        return _buf.share(pos, len);
    }

    /// destructive move. place special care when calling this method
    /// on a shared iobuf. most of the time you want share() instead of release
    ss::temporary_buffer<char> release() && {
        trim();
        return std::move(_buf);
    }
    void trim() {
        if (_used_bytes == _buf.size()) {
            return;
        }
        size_t half = _buf.size() / 2;
        if (_used_bytes <= half) {
            // this is an important optimization. often times during RPC
            // serialization we append some small controll bytes, _right_
            // before we append a full new chain of iobufs
            _buf = ss::temporary_buffer<char>(_buf.get(), _used_bytes);
        } else {
            _buf.trim(_used_bytes);
        }
    }
    void trim(size_t len) { _used_bytes = std::min(len, _used_bytes); }
    void trim_front(size_t pos);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    char* get_current() { return _buf.get_write() + _used_bytes; }
    char* get_write() { return _buf.get_write(); }

    // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes,misc-non-private-member-variables-in-classes)
    safe_intrusive_list_hook hook;

private:
    ss::temporary_buffer<char> _buf;
    size_t _used_bytes;
};

inline void __attribute__((noinline)) dispose_io_fragment(io_fragment* f) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wfree-nonheap-object"
    delete f; // NOLINT
#pragma GCC diagnostic pop
}

} // namespace details
