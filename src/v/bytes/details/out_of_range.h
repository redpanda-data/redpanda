#pragma once
#include "likely.h"

#include <fmt/format.h>

namespace details {
[[noreturn]] [[gnu::cold]] static void
throw_out_of_range(const char* fmt, size_t A, size_t B) {
    throw std::out_of_range(fmt::format(fmt, A, B));
}
static inline void check_out_of_range(size_t sz, size_t capacity) {
    if (unlikely(sz > capacity)) {
        throw_out_of_range("iobuf op: size:{} > capacity:{}", sz, capacity);
    }
}
} // namespace details
