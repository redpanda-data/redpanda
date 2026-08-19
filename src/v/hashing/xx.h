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

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <span>
#include <string_view>
#include <xxhash.h>

inline uint64_t xxhash_64(const unsigned char* data, size_t length) {
    return XXH64(data, length, 0);
}
inline uint32_t xxhash_32(const unsigned char* data, size_t length) {
    return XXH32(data, length, 0);
}

inline uint64_t xxhash_64(const char* data, const size_t& length) {
    return XXH64(data, length, 0);
}

inline uint64_t xxh3_64(const unsigned char* data, size_t length) {
    return XXH3_64bits(data, length);
}
inline uint64_t xxh3_64(const char* data, size_t length) {
    return XXH3_64bits(data, length);
}
inline uint32_t xxhash_32(const char* data, const size_t& length) {
    return XXH32(data, length, 0);
}

namespace detail {

/// xxhash.h is included with XXH_PRIVATE_API, so every XXH function has
/// internal linkage. Naming them through a traits type keeps the
/// specialization below identical in every translation unit, where template
/// arguments of function-pointer type would name a different function in each.
struct xxh64_traits {
    using state = XXH64_state_t;
    static void reset(state* s, uint64_t seed) { XXH64_reset(s, seed); }
    static void update(state* s, const void* src, size_t sz) {
        XXH64_update(s, src, sz);
    }
    static uint64_t digest(const state* s) { return XXH64_digest(s); }
};

struct xxh3_64_traits {
    using state = XXH3_state_t;
    static void reset(state* s, uint64_t seed) {
        XXH3_64bits_reset_withSeed(s, seed);
    }
    static void update(state* s, const void* src, size_t sz) {
        XXH3_64bits_update(s, src, sz);
    }
    static uint64_t digest(const state* s) { return XXH3_64bits_digest(s); }
};

template<typename Traits>
class incremental_xxhash {
public:
    explicit incremental_xxhash(uint64_t seed = 0) {
        Traits::reset(&_state, seed);
    }
    incremental_xxhash(incremental_xxhash&&) noexcept = default;
    incremental_xxhash& operator=(incremental_xxhash&&) noexcept = default;

    void update(const char* src, const std::size_t sz) {
        Traits::update(&_state, src, sz);
    }

    void update(std::span<const std::byte> bytes) {
        Traits::update(&_state, bytes.data(), bytes.size());
    }

    // string override
    void update(std::string_view str) { update(str.data(), str.size()); }

    // named type override
    template<
      typename T,
      typename std::enable_if_t<
        std::is_convertible_v<T, typename T::type>>* = nullptr>
    void update(const T& named_type) {
        update(named_type());
    }

    template<
      typename T,
      typename std::enable_if_t<std::is_integral_v<T>>* = nullptr>
    void update(T t) {
        update((const char*)&t, sizeof(T));
    }
    template<typename... T>
    void update_all(T... t) {
        (update(t), ...);
    }

    uint64_t digest() { return Traits::digest(&_state); }

private:
    typename Traits::state _state{};
};
} // namespace detail

using incremental_xxhash64 = detail::incremental_xxhash<detail::xxh64_traits>;

using incremental_xxh3_64 = detail::incremental_xxhash<detail::xxh3_64_traits>;

template<
  typename T,
  std::size_t N,
  typename = std::enable_if_t<std::is_integral<T>::value>>
inline uint64_t xxhash_64(const std::array<T, N>& arr) {
    return xxhash_64(reinterpret_cast<const char*>(&arr[0]), sizeof(T) * N);
}
template<
  typename T,
  std::size_t N,
  typename = std::enable_if_t<std::is_integral<T>::value>>
inline uint32_t xxhash_32(const std::array<T, N>& arr) {
    return xxhash_32(reinterpret_cast<const char*>(&arr[0]), sizeof(T) * N);
}
template<
  typename T,
  std::size_t N,
  typename = std::enable_if_t<std::is_integral<T>::value>>
inline uint64_t xxh3_64(const std::array<T, N>& arr) {
    return xxh3_64(reinterpret_cast<const char*>(&arr[0]), sizeof(T) * N);
}
inline uint64_t xxhash_64_str(const char* s) {
    return xxhash_64(s, std::strlen(s));
}
inline uint32_t xxhash_32_str(const char* s) {
    return xxhash_32(s, std::strlen(s));
}
