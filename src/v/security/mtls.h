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

#pragma once

#include "config/property.h"
#include "seastarx.h"
#include "security/acl.h"

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

class principal_mapper {
public:
    explicit principal_mapper(
      config::binding<std::optional<std::vector<ss::sstring>>> cb);
    std::optional<ss::sstring> apply(std::string_view sv) const;

private:
    friend struct fmt::formatter<principal_mapper>;

    friend std::ostream&
    operator<<(std::ostream& os, const principal_mapper& p);

    config::binding<std::optional<std::vector<ss::sstring>>> _binding;
    std::vector<rule> _rules;
};

class mtls_state {
public:
    explicit mtls_state(ss::sstring principal)
      : _principal{principal_type::user, std::move(principal)} {}

    const acl_principal& principal() { return _principal; }

private:
    acl_principal _principal;
};

std::optional<ss::sstring>
validate_rules(const std::optional<std::vector<ss::sstring>>& r) noexcept;

} // namespace security::tls

template<>
struct fmt::formatter<security::tls::rule> {
    using type = security::tls::rule;

    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    template<typename FormatContext>
    typename FormatContext::iterator
    format(const type& r, FormatContext& ctx) const;
};

template<>
struct fmt::formatter<security::tls::principal_mapper> {
    using type = security::tls::principal_mapper;

    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    template<typename FormatContext>
    typename FormatContext::iterator
    format(const type& r, FormatContext& ctx) const;
};
