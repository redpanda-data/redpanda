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
    incremental_xxhash64() {
        reset();
    }
    incremental_xxhash64(incremental_xxhash64&&) noexcept = default;
    incremental_xxhash64& operator=(incremental_xxhash64&&) noexcept = default;

    [[gnu::always_inline]] inline void reset() {
        // no need to check for error
        // https://gist.github.com/9ea1c9ad4df3bad8b16e4dea4a18018a
        XXH64_reset(&_state, 0);
    }
    [[gnu::always_inline]] inline void
    update(const char* src, const std::size_t& sz) {
        XXH64_update(&_state, src, sz);
    }
    [[gnu::always_inline]] inline void update(const sstring& str) {
        update(str.data(), str.size());
    }
    template<
      typename T,
      class = typename std::enable_if<std::is_integral<T>::value>::type>
    [[gnu::always_inline]] inline void update(T t) {
        XXH64_update(&_state, (const char*)&t, sizeof(T));
    }
    [[gnu::always_inline]] inline uint64_t digest() {
        return XXH64_digest(&_state);
    }

private:
    XXH64_state_t _state;
};

class incremental_xxhash32 {
public:
    incremental_xxhash32() {
        reset();
    }
    incremental_xxhash32(incremental_xxhash32&&) noexcept = default;
    incremental_xxhash32& operator=(incremental_xxhash32&&) noexcept = default;

    [[gnu::always_inline]] inline void reset() { // no need to check for error
        // https://gist.github.com/9ea1c9ad4df3bad8b16e4dea4a18018a
        XXH32_reset(&_state, 0);
    }
    [[gnu::always_inline]] inline void
    update(const char* src, const std::size_t& sz) {
        XXH32_update(&_state, src, sz);
    }
    template<
      typename T,
      class = typename std::enable_if<std::is_integral<T>::value>::type>
    [[gnu::always_inline]] inline void update(T t) {
        XXH32_update(&_state, (const char*)&t, sizeof(T));
    }
    [[gnu::always_inline]] inline uint32_t digest() {
        return XXH32_digest(&_state);
    }

private:
    XXH32_state_t _state;
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
