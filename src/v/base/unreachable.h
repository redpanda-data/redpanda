/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include <seastar/util/backtrace.hh>
#include <seastar/util/log.hh>

namespace details {
// NOLINTNEXTLINE
inline seastar::logger _g_unreachable_assert_logger("assert-unreachable");
} // namespace details

// NOLINTNEXTLINE
#define unreachable()                                                          \
    /* NOLINTNEXTLINE(cppcoreguidelines-avoid-do-while) */                     \
    do {                                                                       \
        ::details::_g_unreachable_assert_logger.error(                         \
          "This code should not be reached ({}:{})", __FILE__, __LINE__);      \
        ::details::_g_unreachable_assert_logger.error(                         \
          "Backtrace below:\n{}", seastar::current_backtrace());               \
        __builtin_trap();                                                      \
    } while (0)
