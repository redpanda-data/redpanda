/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "security/gssapi_principal_mapper.h"

#include "base/vassert.h"
#include "base/vlog.h"
#include "security/logger.h"

#include <boost/algorithm/string/case_conv.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <re2/re2.h>

#include <charconv>

/*
 * some older versions of re2 don't have operator for implicit cast to
 * string_view so add this helper to support older re2.
 */
static std::string_view spv(const re2::StringPiece& sp) {
    return {sp.data(), sp.size()};
}

namespace security {
namespace gssapi {
static constexpr std::string_view gssapi_name_pattern
  = R"(([^/@]*)(/([^/@]*))?@([^/@]*))";

namespace detail {
std::vector<gssapi_rule>
parse_rules(const std::vector<ss::sstring>& unparsed_rules);
}
} // namespace gssapi

gssapi_name::gssapi_name(
  std::string_view primary, std::string_view host_name, std::string_view realm)
  : _primary(primary)
  , _host_name(host_name)
  , _realm(realm) {
    if (_primary.empty()) {
        throw std::invalid_argument("primary must be provided");
    }
}

std::optional<gssapi_name> gssapi_name::parse(std::string_view principal_name) {
    static thread_local const re2::RE2 gssapi_name_regex(
      gssapi::gssapi_name_pattern, re2::RE2::Quiet);
    vassert(
      gssapi_name_regex.ok(),
      "Invalid name pattern: {}",
      gssapi_name_regex.error());

    re2::StringPiece primary, host_name, realm;

    if (re2::RE2::FullMatch(
          principal_name,
          gssapi_name_regex,
          &primary,
          nullptr,
          &host_name,
          &realm)) {
        return gssapi_name(
          ss::sstring(spv(primary)),
          ss::sstring(spv(host_name)),
          ss::sstring(spv(realm)));
    } else {
        if (principal_name.find('@') != std::string_view::npos) {
            vlog(seclog.warn, "Malformed gssapi name: {}", principal_name);
            return std::nullopt;
        } else {
            return gssapi_name(ss::sstring(principal_name), "", "");
        }
    }
}

const ss::sstring& gssapi_name::primary() const noexcept { return _primary; }

const ss::sstring& gssapi_name::host_name() const noexcept {
    return _host_name;
}

const ss::sstring& gssapi_name::realm() const noexcept { return _realm; }

gssapi_principal_mapper::gssapi_principal_mapper(
  config::binding<std::vector<ss::sstring>> principal_to_local_rules_cb)
  : _principal_to_local_rules_binding(std::move(principal_to_local_rules_cb))
  , _rules{gssapi::detail::parse_rules(_principal_to_local_rules_binding())} {
    _principal_to_local_rules_binding.watch([this]() {
        _rules = gssapi::detail::parse_rules(
          _principal_to_local_rules_binding());
    });
}

std::optional<ss::sstring> gssapi_principal_mapper::apply(
  std::string_view default_realm, const gssapi_name& name) const {
    return apply(default_realm, name, _rules);
}

std::optional<ss::sstring> gssapi_principal_mapper::apply(
  std::string_view default_realm,
  const gssapi_name& name,
  const std::vector<gssapi_rule>& rules) {
    std::vector<std::string_view> params;
    if (name.host_name().empty()) {
        if (name.realm().empty()) {
            return name.primary();
        }
        params = {name.realm(), name.primary()};
    } else {
        params = {name.realm(), name.primary(), name.host_name()};
    }

    for (const auto& r : rules) {
        if (auto result = r.apply(default_realm, params); result.has_value()) {
            return result;
        }
    }

    vlog(seclog.warn, "No rules apply to {}, rules: {}", name, rules);
    return std::nullopt;
}

std::ostream& operator<<(std::ostream& os, const gssapi_name& n) {
    fmt::print(os, "{}", n);
    return os;
}

std::ostream& operator<<(std::ostream& os, const gssapi_principal_mapper& m) {
    fmt::print(os, "{}", m);
    return os;
}

} // namespace security

// explicit instantiations so as to avoid bringing in <fmt/ranges.h> in the
// header, which breaks compilation in another part of the codebase.

template<>
typename fmt::basic_format_context<fmt::appender, char>::iterator
fmt::formatter<security::gssapi_name, char, void>::format<
  fmt::basic_format_context<fmt::appender, char>>(
  const security::gssapi_name& r,
  fmt::basic_format_context<fmt::appender, char>& ctx) const {
    fmt::format_to(ctx.out(), "{}", r._primary);
    if (!r._host_name.empty()) {
        fmt::format_to(ctx.out(), "/{}", r._host_name);
    }
    if (!r._realm.empty()) {
        fmt::format_to(ctx.out(), "@{}", r._realm);
    }

    return ctx.out();
}

template<>
typename fmt::basic_format_context<fmt::appender, char>::iterator
fmt::formatter<security::gssapi_principal_mapper, char, void>::format<
  fmt::basic_format_context<fmt::appender, char>>(
  const security::gssapi_principal_mapper& r,
  fmt::basic_format_context<fmt::appender, char>& ctx) const {
    return fmt::format_to(ctx.out(), "[{}]", fmt::join(r._rules, ", "));
}
