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

#include "base/seastarx.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/bool_class.hh>

#include <fmt/core.h>

#include <optional>
#include <regex>
#include <string_view>

namespace security::tls {

class rule {
public:
    using make_lower = ss::bool_class<struct make_lower_tag>;
    using make_upper = ss::bool_class<struct make_upper_tag>;

    rule() = default;

    rule(
      std::string_view pattern,
      std::optional<std::string_view> replacement,
      make_lower to_lower,
      make_upper to_upper);

    std::optional<ss::sstring> apply(std::string_view dn) const;

private:
    friend struct fmt::formatter<rule>;

    friend std::ostream& operator<<(std::ostream& os, const rule& r);

    std::regex _regex;
    std::optional<ss::sstring> _pattern;
    std::optional<ss::sstring> _replacement;
    bool _is_default{true};
    make_lower _to_lower{false};
    make_upper _to_upper{false};
};

} // namespace security::tls

template<>
struct fmt::formatter<security::tls::rule> {
    using type = security::tls::rule;

    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    template<typename FormatContext>
    typename FormatContext::iterator
    format(const type& r, FormatContext& ctx) const;
};
