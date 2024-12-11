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

#include "base/vassert.h"

#include <seastar/util/backtrace.hh>

// NOLINTNEXTLINE
#define unreachable()                                                          \
    /* NOLINTNEXTLINE(cppcoreguidelines-avoid-do-while) */                     \
    do {                                                                       \
        ::detail::g_assert_log.l.error(                                        \
          "This code should not be reached ({}:{})", __FILE__, __LINE__);      \
        ::detail::g_assert_log.l.error(                                        \
          "Backtrace below:\n{}", seastar::current_backtrace());               \
        __builtin_trap();                                                      \
    } while (0)
