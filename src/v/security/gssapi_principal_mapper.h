/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "config/property.h"
#include "security/config.h"
#include "security/gssapi_rule.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/bool_class.hh>

#include <fmt/core.h>

namespace security {

class gssapi_name {
public:
    gssapi_name(
      std::string_view primary,
      std::string_view host_name,
      std::string_view realm);
    static std::optional<gssapi_name> parse(std::string_view principal_name);

    const ss::sstring& primary() const noexcept;
    const ss::sstring& host_name() const noexcept;
    const ss::sstring& realm() const noexcept;

private:
    friend struct fmt::formatter<gssapi_name>;

    friend std::ostream& operator<<(std::ostream& os, const gssapi_name& n);

    ss::sstring _primary;
    ss::sstring _host_name;
    ss::sstring _realm;
};

class gssapi_principal_mapper {
public:
    explicit gssapi_principal_mapper(
      config::binding<std::vector<ss::sstring>> principal_to_local_rules_cb);
    std::optional<ss::sstring>
    apply(std::string_view default_realm, const gssapi_name& name) const;
    static std::optional<ss::sstring> apply(
      std::string_view default_realm,
      const gssapi_name& name,
      const std::vector<gssapi_rule>& rules);

    const std::vector<gssapi_rule>& rules() const noexcept { return _rules; }

private:
    friend struct fmt::formatter<gssapi_principal_mapper>;

    friend std::ostream&
    operator<<(std::ostream& os, const gssapi_principal_mapper& p);

    config::binding<std::vector<ss::sstring>> _principal_to_local_rules_binding;
    std::vector<gssapi_rule> _rules;
};

} // namespace security

template<>
struct fmt::formatter<security::gssapi_name> {
    using type = security::gssapi_name;

    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    template<typename FormatContext>
    typename FormatContext::iterator
    format(const type& r, FormatContext& ctx) const;
};

template<>
struct fmt::formatter<security::gssapi_principal_mapper> {
    using type = security::gssapi_principal_mapper;

    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    template<typename FormatContext>
    typename FormatContext::iterator
    format(const type& r, FormatContext& ctx) const;
};
