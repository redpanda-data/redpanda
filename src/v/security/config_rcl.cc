/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "security/config.h"
#include "security/gssapi_rule.h"
#include "security/mtls_rule.h"
#include "security/oidc_error.h"
#include "security/oidc_principal_mapping.h"
#include "security/oidc_url_parser.h"
#include "ssx/sformat.h"
#include "thirdparty/ada/ada.h"

#include <boost/algorithm/string/case_conv.hpp>
#include <re2/re2.h>

#include <charconv>
#include <optional>
#include <system_error>

namespace security {

namespace gssapi::detail {

static constexpr std::string_view rule_pattern
  = R"(((DEFAULT)|((RULE:\[(\d*):([^\]]*)](\(([^)]*)\))?(s\/([^\/]*)\/([^\/]*)\/(g)?)?\/?(L|U)?))))";

/*
 * some older versions of re2 don't have operator for implicit cast to
 * string_view so add this helper to support older re2.
 */
static std::string_view spv(const re2::StringPiece& sp) {
    return {sp.data(), sp.size()};
}

std::vector<gssapi_rule>
parse_rules(const std::vector<ss::sstring>& unparsed_rules) {
    static thread_local const re2::RE2 rule_parser(
      rule_pattern, re2::RE2::Quiet);

    vassert(
      rule_parser.ok(),
      "Failed to build rule pattern regex: {}",
      rule_parser.error());

    if (unparsed_rules.empty()) {
        return {gssapi_rule()};
    }

    std::vector<gssapi_rule> rv;
    re2::StringPiece default_;
    re2::StringPiece num_components_str;
    re2::StringPiece format;
    re2::StringPiece match_regex;
    re2::StringPiece from_pattern;
    re2::StringPiece to_pattern;
    re2::StringPiece repeat;
    re2::StringPiece upper_lower;
    for (const auto& rule : unparsed_rules) {
        const re2::StringPiece rule_piece(rule.data(), rule.size());
        if (!re2::RE2::FullMatch(
              rule_piece,
              rule_parser,
              nullptr,
              &default_,
              nullptr,
              nullptr,
              &num_components_str,
              &format,
              nullptr,
              &match_regex,
              nullptr,
              &from_pattern,
              &to_pattern,
              &repeat,
              &upper_lower)) {
            throw std::runtime_error("GSSAPI: Invalid rule: " + rule);
        }
        if (!default_.empty()) {
            rv.emplace_back();
        } else {
            int num_components = std::numeric_limits<int>::max();
            auto conv_rc = std::from_chars(
              num_components_str.begin(),
              num_components_str.end(),
              num_components);
            if (conv_rc.ec != std::errc()) {
                throw std::runtime_error(
                  "Invalid rule - Invalid value for number of components: "
                  + std::string(num_components_str));
            }
            gssapi_rule::case_change_operation case_change
              = gssapi_rule::case_change_operation::noop;

            if (upper_lower == "L") {
                case_change = gssapi_rule::case_change_operation::make_lower;
            } else if (upper_lower == "U") {
                case_change = gssapi_rule::case_change_operation::make_upper;
            }
            rv.emplace_back(
              num_components,
              spv(format),
              spv(match_regex),
              spv(from_pattern),
              spv(to_pattern),
              gssapi_rule::repeat{repeat == "g"},
              case_change);
        }
    }

    return rv;
}
} // namespace gssapi::detail

std::optional<ss::sstring>
validate_kerberos_mapping_rules(const std::vector<ss::sstring>& r) noexcept {
    try {
        gssapi::detail::parse_rules(r);
    } catch (const std::exception& e) {
        return e.what();
    }
    return std::nullopt;
}

namespace oidc {
std::ostream& operator<<(std::ostream& os, const parsed_url& url) {
    fmt::print(os, "{}://{}:{}{}", url.scheme, url.host, url.port, url.target);
    return os;
}
result<parsed_url> parse_url(std::string_view url_view) {
    parsed_url result;
    auto url = ada::parse<ada::url_aggregator>(url_view);
    if (!url) {
        return make_error_code(std::errc::invalid_argument);
    }
    auto proto = url->get_protocol();
    result.scheme = ss::sstring{proto.substr(0, proto.length() - 1)};
    if (result.scheme.empty()) {
        result.scheme = "https";
    }

    if (!url->has_hostname()) {
        return make_error_code(std::errc::invalid_argument);
    }
    result.host = ss::sstring{url->get_hostname()};

    if (url->has_port()) {
        auto port_str = url->get_port();
        auto b = port_str.data();
        auto e = b + port_str.length();
        auto res = std::from_chars(b, e, result.port);
        if (res.ec != std::errc{} || res.ptr != e) {
            throw std::runtime_error("invalid url");
        }
    } else {
        result.port = url->scheme_default_port();
    }

    result.target = ssx::sformat(
      "{}{}{}", url->get_pathname(), url->get_search(), url->get_hash());

    return result;
}

static constexpr std::string_view mapping_rule_pattern
  = R"(\/((\\.|[^\\/])*)\/((\\.|[^\\/])*)\/([LU]?).*?|(.*?))";

constexpr std::optional<std::string_view> make_sv(const std::csub_match& sm) {
    return sm.matched
             ? std::string_view{sm.first, static_cast<size_t>(sm.length())}
             : std::optional<std::string_view>{std::nullopt};
}

result<principal_mapping_rule>
parse_principal_mapping_rule(std::string_view mapping) {
    if (!mapping.starts_with("$.")) {
        return errc::invalid_principal_mapping;
    }
    auto slash = mapping.find('/');
    auto path = ss::sstring(mapping.substr(1, slash - 1));
    std::replace(path.begin(), path.end(), '.', '/');

    auto pointer = json::Pointer{path};
    if (!pointer.IsValid()) {
        return errc::invalid_principal_mapping;
    }

    std::regex rule_parser{
      mapping_rule_pattern.begin(),
      mapping_rule_pattern.length(),
      std::regex_constants::ECMAScript | std::regex_constants::optimize};

    tls::rule rule;
    if (slash != std::string_view::npos) {
        auto rule_str = mapping.substr(slash);
        std::cmatch components_match;
        if (!std::regex_search(
              rule_str.begin(),
              rule_str.end(),
              components_match,
              rule_parser,
              std::regex_constants::match_default)) {
            return errc::invalid_principal_mapping;
        }
        if (components_match.prefix().matched) {
            return errc::invalid_principal_mapping;
        }
        if (components_match.suffix().matched) {
            return errc::invalid_principal_mapping;
        }
        if (!components_match[1].matched) {
            return errc::invalid_principal_mapping;
        }
        const auto adjust_case = make_sv(components_match[5]);
        rule = {
          *make_sv(components_match[1]),
          make_sv(components_match[3]),
          tls::rule::make_lower{adjust_case == "L"},
          tls::rule::make_upper{adjust_case == "U"}};
    }

    return principal_mapping_rule{std::move(pointer), std::move(rule)};
}

std::optional<ss::sstring>
validate_principal_mapping_rule(const ss::sstring& rule) {
    auto rule_res = parse_principal_mapping_rule(rule);
    if (rule_res.has_error()) {
        return rule_res.assume_error().message();
    }
    return std::nullopt;
}

} // namespace oidc
} // namespace security
