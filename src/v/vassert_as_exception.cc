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

#include <seastar/util/backtrace.hh>

// implements abort behavior
[[noreturn]] void detail::vassert_hook(std::string msg) {
    ss::throw_with_backtrace<std::logic_error>(std::move(msg));
}
