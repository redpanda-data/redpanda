#pragma once
#include <cstdint>
#include <cstring>

#include <smf/log.h>
#include <smf/macros.h>
#include <xxhash.h>

namespace v {

inline uint64_t
xxhash_64(const char *data, const size_t &length) {
  return XXH64(data, length, 0);
}
inline uint32_t
xxhash_32(const char *data, const size_t &length) {
  return XXH32(data, length, 0);
}

class incremental_xxhash64 {
 public:
  SMF_DISALLOW_COPY_AND_ASSIGN(incremental_xxhash64);

  incremental_xxhash64() { reset(); }
  [[gnu::always_inline]] inline void
  reset() {
    // no need to check for error
    // https://gist.github.com/9ea1c9ad4df3bad8b16e4dea4a18018a
    XXH64_reset(&state_, 0);
  }
  [[gnu::always_inline]] inline void
  update(const char *src, const std::size_t &sz) {
    XXH64_update(&state_, src, sz);
  }
  template <typename T,
            class = typename std::enable_if<std::is_integral<T>::value>::type>
  [[gnu::always_inline]] inline void
  update(T t) {
    XXH64_update(&state_, (const char *)&t, sizeof(T));
  }
  [[gnu::always_inline]] inline uint64_t
  digest() {
    return XXH64_digest(&state_);
  }

 private:
  XXH64_state_t state_;
};

class incremental_xxhash32 {
 public:
  SMF_DISALLOW_COPY_AND_ASSIGN(incremental_xxhash32);

  incremental_xxhash32() { reset(); }
  [[gnu::always_inline]] inline void
  reset() {
    // no need to check for error
    // https://gist.github.com/9ea1c9ad4df3bad8b16e4dea4a18018a
    XXH32_reset(&state_, 0);
  }
  [[gnu::always_inline]] inline void
  update(const char *src, const std::size_t &sz) {
    XXH32_update(&state_, src, sz);
  }
  template <typename T,
            class = typename std::enable_if<std::is_integral<T>::value>::type>
  [[gnu::always_inline]] inline void
  update(T t) {
    XXH32_update(&state_, (const char *)&t, sizeof(T));
  }
  [[gnu::always_inline]] inline uint32_t
  digest() {
    return XXH32_digest(&state_);
  }

 private:
  XXH32_state_t state_;
};

template <typename T, std::size_t N,
          typename = std::enable_if_t<std::is_integral<T>::value>>
inline uint64_t
xxhash_64(const std::array<T, N> &arr) {
  return xxhash_64(reinterpret_cast<const char *>(&arr[0]), sizeof(T) * N);
}
template <typename T, std::size_t N,
          typename = std::enable_if_t<std::is_integral<T>::value>>
inline uint32_t
xxhash_32(const std::array<T, N> &arr) {
  return xxhash_32(reinterpret_cast<const char *>(&arr[0]), sizeof(T) * N);
}
inline uint64_t
xxhash_64_str(const char *s) {
  return xxhash_64(DTHROW_IFNULL(s), std::strlen(s));
}
inline uint32_t
xxhash_32_str(const char *s) {
  return xxhash_32(DTHROW_IFNULL(s), std::strlen(s));
}

}  // namespace v
