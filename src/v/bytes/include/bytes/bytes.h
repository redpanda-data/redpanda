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

#include "bytes/iobuf.h"
#include "seastarx.h"

#include <seastar/core/sstring.hh>

#include <absl/hash/hash.h>

#include <cstdint>
#include <iosfwd>

// cannot be a `std::byte` because that's not sizeof(char)
constexpr size_t bytes_inline_size = 31;
using bytes = ss::basic_sstring<
  uint8_t,  // Must be different from char to not leak to std::string_view
  uint32_t, // size type - 4 bytes - 4GB max - don't use a size_t or any 64-bit
  bytes_inline_size, // short string optimization size
  false              // not null terminated
  >;

using bytes_view = std::basic_string_view<uint8_t>;
using bytes_opt = std::optional<bytes>;

struct bytes_type_hash {
    using is_transparent = std::true_type;
    // NOTE: you hash a fragmented buffer with a linearized buffer
    //       unless you make a copy and linearize first. Our fragmented buffer
    //       correctly implements boost::hash_combine between fragments which
    //       would be missing altogether from a linearize buffer which is simply
    //       the std::hash<std::string_view>()
    //
    //   size_t operator()(const iobuf& k) const;
    //
    size_t operator()(const bytes& k) const;
    size_t operator()(const bytes_view&) const;
};

template<typename R, R (*HashFunction)(bytes::const_pointer, size_t)>
requires requires(bytes::const_pointer data, size_t len) {
    { HashFunction(data, len) } -> std::same_as<R>;
}
struct bytes_hasher {
    using is_transparent = std::true_type;

    R operator()(bytes_view b) const {
        return HashFunction(b.data(), b.size());
    }
    R operator()(const bytes& bb) const { return operator()(bytes_view(bb)); }
};

struct bytes_type_eq {
    using is_transparent = std::true_type;
    bool operator()(const bytes& lhs, const bytes_view& rhs) const;
    bool operator()(const bytes& lhs, const bytes& rhs) const;
    bool operator()(const bytes& lhs, const iobuf& rhs) const;
};

ss::sstring to_hex(bytes_view b);
ss::sstring to_hex(const bytes& b);

template<typename Char, size_t Size>
inline bytes_view to_bytes_view(const std::array<Char, Size>& data) {
    static_assert(sizeof(Char) == 1, "to_bytes_view only accepts bytes");
    return bytes_view(
      reinterpret_cast<const uint8_t*>(data.data()), Size); // NOLINT
}

template<typename Char, size_t Size>
inline ss::sstring to_hex(const std::array<Char, Size>& data) {
    return to_hex(to_bytes_view(data));
}

std::ostream& operator<<(std::ostream& os, const bytes& b);
std::ostream& operator<<(std::ostream& os, const bytes_opt& b);

inline bytes iobuf_to_bytes(const iobuf& in) {
    auto out = ss::uninitialized_string<bytes>(in.size_bytes());
    {
        iobuf::iterator_consumer it(in.cbegin(), in.cend());
        it.consume_to(in.size_bytes(), out.data());
    }
    return out;
}

inline iobuf bytes_to_iobuf(const bytes& in) {
    iobuf out;
    // NOLINTNEXTLINE
    out.append(reinterpret_cast<const char*>(in.data()), in.size());
    return out;
}

// NOLINTNEXTLINE(cert-dcl58-cpp): hash<> specialization
namespace std {
template<>
struct hash<bytes_view> {
    size_t operator()(bytes_view v) const {
        return hash<std::string_view>()(
          // NOLINTNEXTLINE
          {reinterpret_cast<const char*>(v.data()), v.size()});
    }
};
} // namespace std

// FIXME: remove overload from std::
// NOLINTNEXTLINE(cert-dcl58-cpp)
namespace std {
std::ostream& operator<<(std::ostream& os, const bytes_view& b);
}

inline size_t bytes_type_hash::operator()(const bytes_view& k) const {
    return absl::Hash<bytes_view>{}(k);
}

inline size_t bytes_type_hash::operator()(const bytes& k) const {
    return absl::Hash<bytes>{}(k);
}

inline bool
bytes_type_eq::operator()(const bytes& lhs, const bytes& rhs) const {
    return lhs == rhs;
}
inline bool
bytes_type_eq::operator()(const bytes& lhs, const bytes_view& rhs) const {
    return bytes_view(lhs) == rhs;
}
inline bool
bytes_type_eq::operator()(const bytes& lhs, const iobuf& rhs) const {
    if (lhs.size() != rhs.size_bytes()) {
        return false;
    }
    auto iobuf_end = iobuf::byte_iterator(rhs.cend(), rhs.cend());
    auto iobuf_it = iobuf::byte_iterator(rhs.cbegin(), rhs.cend());
    size_t bytes_idx = 0;
    const size_t max = lhs.size();
    while (iobuf_it != iobuf_end && bytes_idx < max) {
        const char r_c = *iobuf_it;
        // NOLINTNEXTLINE(bugprone-narrowing-conversions,cppcoreguidelines-narrowing-conversions)
        const char l_c = lhs[bytes_idx];
        if (l_c != r_c) {
            return false;
        }
        // the equals case
        ++bytes_idx;
        ++iobuf_it;
    }
    return true;
}

inline bytes operator^(bytes_view a, bytes_view b) {
    if (unlikely(a.size() != b.size())) {
        throw std::runtime_error(
          "Cannot compute xor for different size byte strings");
    }
    bytes res(bytes::initialized_later{}, a.size());
    std::transform(
      a.cbegin(), a.cend(), b.cbegin(), res.begin(), std::bit_xor<>());
    return res;
}

template<size_t Size>
inline std::array<char, Size>
operator^(const std::array<char, Size>& a, const std::array<char, Size>& b) {
    std::array<char, Size> out; // NOLINT
    std::transform(
      a.begin(), a.end(), b.begin(), out.begin(), std::bit_xor<>());
    return out;
}
