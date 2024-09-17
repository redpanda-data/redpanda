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
fmt::formatter<security::tls::principal_mapper, char, void>::format<
  fmt::basic_format_context<fmt::appender, char>>(
  const security::tls::principal_mapper& r,
  fmt::basic_format_context<fmt::appender, char>& ctx) const {
    return fmt::format_to(ctx.out(), "[{}]", fmt::join(r._rules, ", "));
}
