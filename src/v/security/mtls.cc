/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "security/mtls.h"

#include <fmt/format.h>
#include <fmt/ostream.h>
#include <fmt/ranges.h>

#include <regex>
#include <stdexcept>

namespace security::tls {

namespace detail {

std::vector<rule>
parse_rules(std::optional<std::vector<ss::sstring>> unparsed_rules);

} // namespace detail

std::ostream& operator<<(std::ostream& os, const rule& r) {
    fmt::print(os, "{}", r);
    return os;
}

std::ostream& operator<<(std::ostream& os, const principal_mapper& p) {
    fmt::print(os, "{}", p);
    return os;
}

principal_mapper::principal_mapper(
  config::binding<std::optional<std::vector<ss::sstring>>> cb)
  : _binding(std::move(cb))
  , _rules{detail::parse_rules(_binding())} {
    _binding.watch([this]() { _rules = detail::parse_rules(_binding()); });
}

std::optional<ss::sstring> principal_mapper::apply(std::string_view sv) const {
    for (const auto& r : _rules) {
        if (auto p = r.apply(sv); p.has_value()) {
            return {std::move(p).value()};
        }
    }
    return std::nullopt;
}

} // namespace security::tls

// explicit instantiations so as to avoid bringing in <fmt/ranges.h> in the
// header, whch breaks compilation in another part of the codebase.
template<>
typename fmt::basic_format_context<fmt::appender, char>::iterator
fmt::formatter<security::tls::rule, char, void>::format<
  fmt::basic_format_context<fmt::appender, char>>(
  security::tls::rule const& r,
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

template<>
typename fmt::basic_format_context<fmt::appender, char>::iterator
fmt::formatter<security::tls::principal_mapper, char, void>::format<
  fmt::basic_format_context<fmt::appender, char>>(
  security::tls::principal_mapper const& r,
  fmt::basic_format_context<fmt::appender, char>& ctx) const {
    return fmt::format_to(ctx.out(), "[{}]", fmt::join(r._rules, ", "));
}
