#pragma once

#include "seastarx.h"

#include <seastar/core/sstring.hh>

#include <cstdint>
#include <cstring>
#include <functional>
#include <xxhash.h>

inline uint64_t xxhash_64(const char* data, const size_t& length) {
    return XXH64(data, length, 0);
}
inline uint32_t xxhash_32(const char* data, const size_t& length) {
    return XXH32(data, length, 0);
}

class incremental_xxhash64 {
public:
    explicit incremental_xxhash64(uint64_t seed = 0) {
        XXH64_reset(&_state, seed);
    }
    incremental_xxhash64(incremental_xxhash64&&) noexcept = default;
    incremental_xxhash64& operator=(incremental_xxhash64&&) noexcept = default;

    void update(const char* src, const std::size_t sz) {
        XXH64_update(&_state, src, sz);
    }

    void update(const ss::sstring& str) { update(str.data(), str.size()); }

    template<
      typename T,
      class = typename std::enable_if<std::is_arithmetic_v<T>, T>>
    void update(T t) {
        update((const char*)&t, sizeof(T));
    }

    uint64_t digest() { return XXH64_digest(&_state); }

private:
    XXH64_state_t _state{};
};

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
inline uint64_t xxhash_64_str(const char* s) {
    return xxhash_64(s, std::strlen(s));
}
inline uint32_t xxhash_32_str(const char* s) {
    return xxhash_32(s, std::strlen(s));
}
