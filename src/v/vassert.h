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

#include "likely.h"
#include "seastarx.h"

#include <seastar/util/backtrace.hh>
#include <seastar/util/log.hh>

namespace detail {
struct dummyassert {
    static inline ss::logger l{"assert"};
};
static dummyassert g_assert_log;
} // namespace detail

/** Meant to be used in the same way as assert(condition, msg);
 * which means we use the negative conditional.
 * i.e.:
 *
 * open_fileset::~open_fileset() noexcept {
 *   vassert(_closed, "fileset not closed");
 * }
 *
 */
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define vassert(x, msg, args...)                                               \
    do {                                                                       \
        /*The !(x) is not an error. see description above*/                    \
        if (unlikely(!(x))) {                                                  \
            ::detail::g_assert_log.l.error(                                    \
              "Assert failure: ({}:{}) '{}' " msg,                             \
              __FILE__,                                                        \
              __LINE__,                                                        \
              #x,                                                              \
              ##args);                                                         \
            ::detail::g_assert_log.l.error(                                    \
              "Backtrace below:\n{}", ss::current_backtrace());                \
            __builtin_trap();                                                  \
        }                                                                      \
    } while (0)
