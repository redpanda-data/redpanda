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
#include <array>
#include <cstddef>
#include <cstdint>
#include <string>

/*
 * Fowler / Noll / Vo (FNV) Hash
 *     http://www.isthe.com/chongo/tech/comp/fnv/
 */

const uint32_t FNV_32_HASH_START = 2166136261UL;
const uint64_t FNV_64_HASH_START = 14695981039346656037ULL;
const uint64_t FNVA_64_HASH_START = 14695981039346656037ULL;

inline uint32_t
fnv32(const char* buf, uint32_t hash = FNV_32_HASH_START) noexcept {
    // forcing signed char, since other platforms can use unsigned
    const signed char* s = reinterpret_cast<const signed char*>(buf);

    for (; *s; ++s) {
        hash += (hash << 1) + (hash << 4) + (hash << 7) + (hash << 8)
                + (hash << 24);
        hash ^= *s;
    }
    return hash;
}

inline uint32_t fnv32_buf(
  const void* buf, size_t n, uint32_t hash = FNV_32_HASH_START) noexcept {
    // forcing signed char, since other platforms can use unsigned
    const signed char* char_buf = reinterpret_cast<const signed char*>(buf);

    for (size_t i = 0; i < n; ++i) {
        hash += (hash << 1) + (hash << 4) + (hash << 7) + (hash << 8)
                + (hash << 24);
        hash ^= char_buf[i];
    }

    return hash;
}

inline uint32_t
fnv32(const std::string& str, uint32_t hash = FNV_32_HASH_START) noexcept {
    return fnv32_buf(str.data(), str.size(), hash);
}

inline uint64_t
fnv64(const char* buf, uint64_t hash = FNV_64_HASH_START) noexcept {
    // forcing signed char, since other platforms can use unsigned
    const signed char* s = reinterpret_cast<const signed char*>(buf);

    for (; *s; ++s) {
        hash += (hash << 1) + (hash << 4) + (hash << 5) + (hash << 7)
                + (hash << 8) + (hash << 40);
        hash ^= *s;
    }
    return hash;
}

inline uint64_t fnv64_buf(
  const void* buf, size_t n, uint64_t hash = FNV_64_HASH_START) noexcept {
    // forcing signed char, since other platforms can use unsigned
    const signed char* char_buf = reinterpret_cast<const signed char*>(buf);

    for (size_t i = 0; i < n; ++i) {
        hash += (hash << 1) + (hash << 4) + (hash << 5) + (hash << 7)
                + (hash << 8) + (hash << 40);
        hash ^= char_buf[i];
    }
    return hash;
}

inline uint64_t
fnv64(const std::string& str, uint64_t hash = FNV_64_HASH_START) noexcept {
    return fnv64_buf(str.data(), str.size(), hash);
}

inline uint64_t fnva64_buf(
  const void* buf, size_t n, uint64_t hash = FNVA_64_HASH_START) noexcept {
    const uint8_t* char_buf = reinterpret_cast<const uint8_t*>(buf);

    for (size_t i = 0; i < n; ++i) {
        hash ^= char_buf[i];
        hash += (hash << 1) + (hash << 4) + (hash << 5) + (hash << 7)
                + (hash << 8) + (hash << 40);
    }
    return hash;
}

inline uint64_t
fnva64(const std::string& str, uint64_t hash = FNVA_64_HASH_START) noexcept {
    return fnva64_buf(str.data(), str.size(), hash);
}

// arrays
template<
  typename T,
  std::size_t N,
  typename = std::enable_if_t<std::is_integral<T>::value>>
inline uint64_t fnv64(const std::array<T, N>& arr) {
    return fnv64_buf(reinterpret_cast<const char*>(&arr[0]), sizeof(T) * N);
}
template<
  typename T,
  std::size_t N,
  typename = std::enable_if_t<std::is_integral<T>::value>>
inline uint32_t fnv32(const std::array<T, N>& arr) {
    return fnv32_buf(reinterpret_cast<const char*>(&arr[0]), sizeof(T) * N);
}
