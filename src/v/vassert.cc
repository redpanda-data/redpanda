/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "vassert.h"

#include "seastarx.h"

#include <seastar/util/backtrace.hh>
#include <seastar/util/log.hh>

namespace detail {
struct dummyassert {
    static inline ss::logger l{"assert"};
};
dummyassert g_assert_log;

// implements abort behavior
[[noreturn]] void vassert_hook(std::string msg) {
    g_assert_log.l.error("{}", msg);
    g_assert_log.l.error("Backtrace below:\n{}", ss::current_backtrace());
    __builtin_trap();
}
} // namespace detail
