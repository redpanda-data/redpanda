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
#include "security/mtls_rule.h"

#include <fmt/format.h>
#include <fmt/ostream.h>

#include <regex>

namespace {
std::regex make_regex(std::string_view sv) {
    return std::regex{
      sv.begin(),
      sv.length(),
      std::regex_constants::ECMAScript | std::regex_constants::optimize};
}
} // namespace

namespace security::tls {

rule::rule(
  std::string_view pattern,
  std::optional<std::string_view> replacement,
  make_lower to_lower,
  make_upper to_upper)
  : _regex{make_regex(pattern)}
  , _pattern{pattern}
  , _replacement{replacement}
  , _is_default{false}
  , _to_lower{to_lower}
  , _to_upper{to_upper} {}

std::ostream& operator<<(std::ostream& os, const rule& r) {
    fmt::print(os, "{}", r);
    return os;
}

} // namespace security::tls

template<>
typename fmt::basic_format_context<fmt::appender, char>::iterator
fmt::formatter<security::tls::rule, char, void>::format<
  fmt::basic_format_context<fmt::appender, char>>(
  const security::tls::rule& r,
  fmt::basic_format_context<fmt::appender, char>& ctx) const {
    if (r._is_default) {
        return fmt::format_to(ctx.out(), "DEFAULT");
    }
    fmt::format_to(ctx.out(), "RULE:");
    if (r._pattern.has_value()) {
        fmt::format_to(ctx.out(), "{}", *r._pattern);
    }
    if (r._replacement.has_value()) {
        fmt::format_to(ctx.out(), "/{}", *r._replacement);
    }
    if (r._to_lower) {
        fmt::format_to(ctx.out(), "/L");
    } else if (r._to_upper) {
        fmt::format_to(ctx.out(), "/U");
    }
    return ctx.out();
}
