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
#include "base/likely.h"

#include <fmt/format.h>

namespace details {
[[maybe_unused]] [[noreturn]] [[gnu::cold]] static void
throw_out_of_range(const char* fmt, size_t A, size_t B) {
    throw std::out_of_range(fmt::format(fmt::runtime(fmt), A, B));
}
inline void check_out_of_range(size_t sz, size_t capacity) {
    if (unlikely(sz > capacity)) {
        throw_out_of_range("iobuf op: size:{} > capacity:{}", sz, capacity);
    }
}
} // namespace details
