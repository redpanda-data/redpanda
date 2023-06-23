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
#include <fmt/format.h>

// Workaround for arm64 coroutine crash. See
// https://groups.google.com/g/seastar-dev/c/Nt6LizrwE9U for details.
template<typename S, typename... Args>
static __attribute__((noinline)) std::string
vformat(const S& format_str, Args&&... args) {
    return fmt::format(format_str, std::forward<Args>(args)...);
}
