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

#include "base/seastarx.h"
#include "bytes/iobuf.h"

#include <seastar/core/sstring.hh>

#include <absl/container/inlined_vector.h>

#include <algorithm>
#include <cstdint>
#include <iosfwd>
#include <span>

class bytes_view;

constexpr size_t bytes_inline_size = 31;

class bytes {
    using container_type = absl::InlinedVector<uint8_t, bytes_inline_size>;

public:
    using value_type = container_type::value_type;
    using size_type = container_type::size_type;
    using reference = container_type::reference;
    using const_reference = container_type::const_reference;
    using pointer = container_type::pointer;
    using const_pointer = container_type::const_pointer;
    using iterator = container_type::iterator;
    using const_iterator = container_type::const_iterator;

    static bytes from_string(std::string_view s) {
        return {s.begin(), s.end()};
    }

    bytes() = default;
    bytes(const bytes&) = default;
    bytes& operator=(const bytes&) = default;
    bytes(bytes&&) noexcept = default;
    bytes& operator=(bytes&&) noexcept = default;
    ~bytes() = default;

    struct initialized_later {};
    bytes(initialized_later, size_t size)
      : data_(size) {}

    struct initialized_zero {};
    bytes(initialized_zero, size_t size)
      : data_(size, 0) {}

    bytes(const value_type* data, size_t size)
      : data_(data, data + size) {}

    bytes(std::initializer_list<uint8_t> x)
      : data_(x) {}

    template<typename InputIterator>
    bytes(InputIterator begin, InputIterator end)
      : data_(begin, end) {}

    explicit bytes(bytes_view);

    reference operator[](size_type pos) noexcept { return data_[pos]; }
    const_reference operator[](size_type pos) const noexcept {
        return data_[pos];
    }

    pointer data() noexcept { return data_.data(); }
    const_pointer data() const noexcept { return data_.data(); }

    iterator begin() noexcept { return data_.begin(); }
    const_iterator begin() const noexcept { return data_.begin(); }
    const_iterator cbegin() const noexcept { return data_.cbegin(); }

    iterator end() noexcept { return data_.end(); }
    const_iterator end() const noexcept { return data_.end(); }
    const_iterator cend() const noexcept { return data_.cend(); }

    size_type size() const noexcept { return data_.size(); }
    bool empty() const noexcept { return data_.empty(); }

    void resize(size_type size) { data_.resize(size); }
    void reserve(size_type size) { data_.reserve(size); }
    void push_back(value_type v) { data_.push_back(v); }

    friend bool operator==(const bytes&, const bytes&) = default;

    friend bool operator<(const bytes& a, const bytes& b) {
        return a.data_ < b.data_;
    }

    friend std::ostream& operator<<(std::ostream& os, const bytes& b);

private:
    container_type data_;
};

class bytes_view {
    using container_type = std::span<const uint8_t>;

public:
    using value_type = container_type::value_type;
    using size_type = container_type::size_type;
    using pointer = container_type::pointer;
    using iterator = container_type::iterator;

    bytes_view() = default;
    bytes_view(const bytes_view&) = default;
    bytes_view& operator=(const bytes_view&) = default;
    bytes_view(bytes_view&&) noexcept = default;
    bytes_view& operator=(bytes_view&&) noexcept = default;
    ~bytes_view() = default;

    bytes_view(const bytes& bytes)
      : data_(bytes.begin(), bytes.end()) {}

    bytes_view(const uint8_t* data, size_t size)
      : data_(data, size) {}

    pointer data() const noexcept { return data_.data(); }

    iterator begin() const noexcept { return data_.begin(); }
    iterator cbegin() const noexcept { return data_.begin(); }

    iterator end() const noexcept { return data_.end(); }
    iterator cend() const noexcept { return data_.end(); }

    size_type size() const noexcept { return data_.size(); }
    bool empty() const noexcept { return data_.empty(); }

    const value_type& operator[](size_t pos) const noexcept {
        return data_[pos];
    }

    bool starts_with(bytes_view v) const noexcept {
        return size() >= v.size() && std::equal(v.begin(), v.end(), begin());
    }

    bytes_view substr(size_t offset) const {
        return bytes_view(data_.subspan(offset));
    }

    friend bool operator==(const bytes_view& a, const bytes_view& b) {
        return std::equal(a.begin(), a.end(), b.begin(), b.end());
    }

    friend bool operator<(const bytes_view& a, const bytes_view& b) {
        return std::lexicographical_compare(
          a.begin(), a.end(), b.begin(), b.end());
    }

    friend std::ostream& operator<<(std::ostream& os, const bytes_view& b);

private:
    explicit bytes_view(container_type data)
      : data_(data) {}

    container_type data_;
};

inline bytes::bytes(bytes_view v)
  : data_(v.begin(), v.end()) {}

template<std::size_t Extent = std::dynamic_extent>
using bytes_span = std::span<bytes::value_type, Extent>;

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

inline bytes iobuf_to_bytes(const iobuf& in) {
    bytes out(bytes::initialized_later{}, in.size_bytes());
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

inline iobuf bytes_to_iobuf(bytes_view in) {
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

template<>
struct hash<bytes> {
    size_t operator()(const bytes& v) const { return hash<bytes_view>()(v); }
};
} // namespace std

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
