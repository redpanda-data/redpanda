// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf.h"

#include "base/vassert.h"
#include "bytes/details/io_allocation_size.h"

#include <seastar/core/bitops.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/smp.hh>

#include <cstddef>
#include <iostream>
#include <limits>

std::ostream& operator<<(std::ostream& o, const iobuf& io) {
    return o << "{bytes=" << io.size_bytes()
             << ", fragments=" << std::distance(io.cbegin(), io.cend()) << "}";
}

iobuf iobuf::copy() const {
    auto in = iobuf::iterator_consumer(cbegin(), cend());
    return iobuf_copy(in, _size);
}

iobuf iobuf_copy(iobuf::iterator_consumer& in, size_t len) {
    iobuf ret;

    int bytes_left = len;
    while (bytes_left) {
        ss::temporary_buffer<char> buf(
          details::io_allocation_size::ss_next_allocation_size(bytes_left));

        size_t offset = 0;
        in.consume(buf.size(), [&buf, &offset](const char* src, size_t size) {
            // NOLINTNEXTLINE
            std::copy_n(src, size, buf.get_write() + offset);
            offset += size;
            return ss::stop_iteration::no;
        });

        bytes_left -= buf.size();

        auto f = std::make_unique<iobuf::fragment>(std::move(buf));
        ret.append(std::move(f));
    }

    vassert(bytes_left == 0, "Bytes remaining to be copied");
    return ret;
}

iobuf iobuf::share(size_t pos, size_t len) {
    iobuf ret;
    size_t left = len;
    for (auto& frag : _frags) {
        if (left == 0) {
            break;
        }
        if (pos >= frag.size()) {
            pos -= frag.size();
            continue;
        }
        size_t left_in_frag = frag.size() - pos;
        if (left >= left_in_frag) {
            left -= left_in_frag;
        } else {
            left_in_frag = left;
            left = 0;
        }
        auto f = std::make_unique<fragment>(frag.share(pos, left_in_frag));
        ret.append(std::move(f));
        pos = 0;
    }
    return ret;
}

bool iobuf::operator==(const iobuf& o) const {
    if (_size != o._size) {
        return false;
    }
    auto lhs_begin = byte_iterator(cbegin(), cend());
    auto lhs_end = byte_iterator(cend(), cend());
    auto rhs = byte_iterator(o.cbegin(), o.cend());
    auto rhs_end = byte_iterator(o.cend(), o.cend());
    while (lhs_begin != lhs_end && rhs != rhs_end) {
        char l = *lhs_begin;
        char r = *rhs;
        if (l != r) {
            return false;
        }
        ++lhs_begin;
        ++rhs;
    }
    return true;
}

bool iobuf::operator<(const iobuf& o) const {
    auto lhs = byte_iterator(cbegin(), cend());
    auto lhs_end = byte_iterator(cend(), cend());
    auto rhs = byte_iterator(o.cbegin(), o.cend());
    auto rhs_end = byte_iterator(o.cend(), o.cend());
    while (lhs != lhs_end && rhs != rhs_end) {
        char l = *lhs;
        char r = *rhs;
        if (l < r) {
            return true;
        }
        if (l > r) {
            return false;
        }
        ++lhs;
        ++rhs;
    }
    if (rhs != rhs_end) {
        // lhs is a prefix of rhs.
        return true;
    }
    return false;
}

bool iobuf::operator==(std::string_view o) const {
    if (_size != o.size()) {
        return false;
    }
    bool are_equal = true;
    std::string_view::size_type n = 0;
    auto in = iobuf::iterator_consumer(cbegin(), cend());
    (void)in.consume(
      size_bytes(), [&are_equal, &o, &n](const char* src, size_t fg_sz) {
          /// Both strings are equiv in total size, so its safe to assume the
          /// next chunk to compare is the remaining to cmp or the fragment size
          const auto size = std::min((o.size() - n), fg_sz);
          std::string_view a_view(src, size);
          std::string_view b_view(o.cbegin() + n, size);
          n += size;
          are_equal &= (a_view == b_view);
          return !are_equal ? ss::stop_iteration::yes : ss::stop_iteration::no;
      });
    return are_equal;
}

/**
 * For debugging, string-ize the iobuf in a format like "hexdump -C"
 *
 * This is useful if you are handling a parse error and would like to
 * safely log the unparseable content.  Set an appropriate `limit` to avoid
 * your log being too verbose.
 *
 * @param limit maximum number of bytes to read.
 * @return a string populated with the following format:
 *
00000000 | 7b 22 76 65 72 73 69 6f  6e 22 3a 31 2c 22 6e 61  | {"version":1,"na
00000010 | 6d 65 73 70 61 63 65 22  3a 22 74 65 73 74 2d 6e  | mespace":"test-n
00000020 | 73 22 2c 22 74 6f 70 69  63 22 3a 22 74 65 73 74  | s","topic":"test
00000030 | 2d 74 6f 70 69 63 22 2c  22 70 61 72 74 69 74 69  | -topic","partiti
 */
std::string iobuf::hexdump(size_t limit) const {
    constexpr size_t line_length = 16;
    auto result = std::ostringstream();
    size_t total = 0;
    std::string trail;
    for (const auto& frag : *this) {
        auto data = frag.get();
        for (size_t i = 0; i < frag.size(); ++i) {
            if (total % line_length == 0) {
                if (trail.size()) {
                    result << " | " << trail;
                    trail.erase();
                }
                result << "\n  " << fmt::format("{:08x}", total) << " | ";
            }

            auto c = data[i];
            result << fmt::format("{:02x} ", uint8_t(c));

            if (std::isprint(c) && c != '\n') {
                trail.push_back(c);
            } else {
                trail.push_back('.');
            }

            if (trail.size() == 8) {
                result << " ";
            }

            if (total >= limit) {
                return result.str();
            } else {
                total++;
            }
        }
    }

    if (trail.size()) {
        auto padding = line_length - trail.size();
        if (padding) {
            if (trail.size() < 8) {
                result << " ";
            }
            while (padding--) {
                result << "   ";
            }
        }
        result << " | " << trail;
    }

    return result.str();
}

void details::io_fragment::trim_front(size_t pos) {
    // required by input_stream<char> converter
    vassert(
      pos <= _used_bytes,
      "trim_front requested {} bytes but io_fragment have only {}",
      pos,
      _used_bytes);
    _used_bytes -= pos;
    _buf.trim_front(pos);
}

iobuf::placeholder iobuf::reserve(size_t sz) {
    oncore_debug_verify(_verify_shard);
    vassert(sz, "zero length reservations are unsupported");
    reserve_memory(sz);
    _size += sz;
    auto it = std::prev(_frags.end());
    placeholder p(it, it->size(), sz);
    it->reserve(sz);
    return p;
}
