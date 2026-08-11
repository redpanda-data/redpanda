// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf.h"

#include "base/units.h"
#include "base/vassert.h"
#include "bytes/details/io_allocation_size.h"

#include <seastar/core/bitops.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/smp.hh>

#include <algorithm>
#include <compare>
#include <cstddef>
#include <iostream>
#include <limits>
#include <string_view>

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

iobuf iobuf::share() { return share(0, size_bytes()); }
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

iobuf iobuf::tail(size_t size) {
    if (size > _size) [[unlikely]] {
        throw std::out_of_range(
          fmt::format(
            "iobuf::tail requested size {} larger than iobuf size {}",
            size,
            _size));
    }
    iobuf out;
    for (auto it = rbegin(); it != rend() && size > 0; ++it) {
        size_t amt = std::min(size, it->size());
        size -= amt;
        out.prepend(it->share(it->size() - amt, amt));
    }
    return out;
}

bool iobuf::operator==(const iobuf& o) const {
    if (_size != o._size) {
        return false;
    }

    return (*this <=> o) == std::strong_ordering::equal;
}

bool iobuf::operator<(const iobuf& o) const {
    return (*this <=> o) == std::strong_ordering::less;
}

std::strong_ordering iobuf::operator<=>(const iobuf& o) const {
    if (empty() || o.empty()) {
        return _size <=> o._size;
    }

    // Always check the first few bytes using byte for byte comparison,
    // this allows the case of relatively randomized data to be done quickly
    // but we still preserve the chunked checks that are faster if there is
    // a matching prefix.
    std::string_view lhs{_frags.front()};
    std::string_view rhs{o._frags.front()};
    constexpr static size_t max_byte_for_byte_cmp = 4;
    const auto n = std::min({lhs.size(), rhs.size(), max_byte_for_byte_cmp});
    for (size_t i = 0; i < n; ++i) {
        const auto cmp = static_cast<unsigned char>(lhs[i])
                         <=> static_cast<unsigned char>(rhs[i]);
        if (cmp != std::strong_ordering::equal) {
            return cmp;
        }
    }

    const auto next_view_fn = [](const iobuf& c) {
        return [c_it = c.cbegin(), end_it = c.cend()] mutable {
            while (c_it != end_it && c_it->is_empty()) {
                ++c_it;
            }
            if (c_it == end_it) {
                return std::string_view{};
            }
            std::string_view s{*c_it};
            ++c_it;
            return s;
        };
    };
    auto this_next_view{next_view_fn(*this)};
    auto other_next_view{next_view_fn(o)};

    lhs = this_next_view();
    rhs = other_next_view();
    for (;;) {
        const auto n = std::min(lhs.size(), rhs.size());
        const auto cmp = std::memcmp(lhs.data(), rhs.data(), n) <=> 0;
        if (cmp != std::strong_ordering::equal) {
            return cmp;
        }

        lhs.remove_prefix(n);
        if (lhs.empty()) {
            lhs = this_next_view();
            if (lhs.empty()) {
                break;
            }
        }
        rhs.remove_prefix(n);
        if (rhs.empty()) {
            rhs = other_next_view();
            if (rhs.empty()) {
                break;
            }
        }
    }

    return _size <=> o._size;
}

bool iobuf::operator==(std::string_view o) const {
    return size_bytes() == o.size()
           && (*this <=> o) == std::strong_ordering::equal;
}

std::strong_ordering iobuf::operator<=>(std::string_view o) const {
    auto other = o;
    // first compare the common length prefix
    for (const auto& frag : *this) {
        auto compare_len = std::min(frag.size(), other.size());
        std::strong_ordering cmp = std::string_view(frag.get(), compare_len)
                                   <=> other.substr(0, compare_len);
        if (cmp != std::strong_ordering::equal) {
            return cmp;
        }
        other.remove_prefix(compare_len);
        if (other.empty()) {
            break;
        }
    }
    // prefix was equal, only size matters now
    return size_bytes() <=> o.size();
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
    vassert(sz, "zero length reservations are unsupported");
    reserve_memory(sz);
    _size += sz;
    auto& back = _frags.back();
    placeholder p(back, back.size(), sz);
    back.reserve(sz);
    return p;
}

ss::sstring iobuf::linearize_to_string() const {
    constexpr static size_t max_size = 128_KiB;
    if (size_bytes() > max_size) {
        throw std::runtime_error(
          fmt::format("string too big: {}", size_bytes()));
    }
    ss::sstring out{ss::sstring::initialized_later{}, size_bytes()};
    auto it = out.begin();
    for (const auto& frag : *this) {
        it = std::copy_n(frag.get(), frag.size(), it);
    }
    return out;
}
