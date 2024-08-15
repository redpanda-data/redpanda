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

template<>
struct fmt::formatter<seastar::sstring> final : fmt::formatter<string_view> {
    template<typename FormatContext>
    auto format(const seastar::sstring& s, FormatContext& ctx) const {
        return formatter<string_view>::format(
          string_view(s.data(), s.size()), ctx);
    }
};

namespace fmt::detail {

// TODO(BenP): Remove this hack with next libfmt upgrade
//
// If a type that has both operator<< and formatter<>, the former is chosen,
// which is not what is desired. This is a regression betwen 7.0.3 and 8.1.1
//
// This hack disables picking operator<<, enabling the above formatter.
//
// A future version of fmtlib will require explicit opt-in:
// template<> struct formatter<test_string> : ostream_formatter {};
//
// Without this hack:
// test               iterations      median         mad         min         max
// std_std_fmt_1K.join     11739    75.312us     1.073us    74.156us    97.341us
// ss_ss_fmt_1K.join        5855   155.362us   925.138ns   154.437us   160.463us
// ss_ss_ssx_1K.join        6642   148.931us     1.317us   147.613us   155.382us
//
// With this hack:
// test               iterations      median         mad         min         max
// std_std_fmt_1K.join     11684    71.891us   218.588ns    71.652us    72.854us
// ss_ss_fmt_1K.join       10300    86.037us   444.582ns    84.775us    88.985us
// ss_ss_ssx_1K.join       12311    72.840us     2.224us    70.616us   101.076us
template<>
struct is_streamable<seastar::sstring, char> : std::false_type {};

} // namespace fmt::detail

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
