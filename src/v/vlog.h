#pragma once

#include <cstdint>

namespace vlog_internal {
constexpr int32_t basename_index(
  const char* const path,
  const int32_t index = 0,
  const int32_t slash_index = -1) {
    // NOLINTNEXTLINE
    const char c = path[index];
    if (c == '\0') {
        return slash_index + 1;
    }
    if (c == '/' || c == '\\') {
        return basename_index(path, index + 1, index);
    }
    return basename_index(path, index + 1, slash_index);
}

template<int32_t V>
struct log_basename_start {
    static constexpr const int32_t value = V;
};
} // namespace vlog_internal

// NOLINTNEXTLINE
#define vlog(method, fmt, args...)                                             \
    method(                                                                    \
      "{}:{} " fmt,                                                            \
      (const char*)&__FILE__[vlog_internal::log_basename_start<                \
        vlog_internal::basename_index(__FILE__)>::value],                      \
      __LINE__,                                                                \
      ##args)
