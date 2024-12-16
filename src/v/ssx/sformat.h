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

#include <seastar/core/sstring.hh>

#include <fmt/format.h>
#include <fmt/ostream.h>

#if FMT_VERSION < 90100
#error fmt library version is too old, must be >= 90100
#endif

namespace ssx {

template<typename... Args>
seastar::sstring
sformat(fmt::format_string<Args...> format_str, Args&&... args) {
    fmt::memory_buffer buf;
    fmt::format_to(
      std::back_inserter(buf), format_str, std::forward<Args>(args)...);
    return {buf.begin(), buf.end()};
}

} // namespace ssx
