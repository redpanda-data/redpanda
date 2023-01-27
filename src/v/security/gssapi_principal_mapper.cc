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

#include "security/logger.h"
#include "vassert.h"
#include "vlog.h"

#include <boost/algorithm/string/case_conv.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>

#include <charconv>

/*
 * some older versions of re2 don't have operator for implicit cast to
 * string_view so add this helper to support older re2.
 */
static std::string_view spv(const re2::StringPiece& sp) {
    return {sp.data(), sp.size()};
}

namespace security {
static constexpr std::string_view gssapi_name_pattern
  = R"(([^/@]*)(/([^/@]*))?@([^/@]*))";
static constexpr std::string_view non_simple_pattern = R"([/@])";
static constexpr std::string_view parameter_pattern = R"(([^$]*)(\$(\d*))?)";
static constexpr std::string_view rule_pattern
  = R"(((DEFAULT)|((RULE:\[(\d*):([^\]]*)](\(([^)]*)\))?(s\/([^\/]*)\/([^\/]*)\/(g)?)?\/?(L|U)?))))";

namespace detail {
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
                  + num_components_str.as_string());
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
} // namespace detail

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
      gssapi_name_pattern, re2::RE2::Quiet);
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

gssapi_rule::gssapi_rule(
  int number_of_components,
  std::string_view format,
  std::string_view match,
  std::string_view from_pattern,
  std::string_view to_pattern,
  repeat repeat_,
  case_change_operation case_change)
  : _is_default(false)
  , _number_of_components(number_of_components)
  , _format(format)
  , _match(match)
  , _from_pattern(std::regex{
      from_pattern.begin(),
      from_pattern.length(),
      std::regex_constants::ECMAScript | std::regex_constants::optimize})
  , _from_pattern_str(from_pattern)
  , _to_pattern(to_pattern)
  , _repeat(repeat_)
  , _case_change(case_change) {}

std::optional<ss::sstring> gssapi_rule::apply(
  std::string_view default_realm, std::vector<std::string_view> params) const {
    static thread_local const re2::RE2 non_simple_regex(
      non_simple_pattern, re2::RE2::Quiet);
    const re2::StringPiece match_piece(_match.data(), _match.size());
    const re2::RE2 match_regex(match_piece, re2::RE2::Quiet);
    vassert(
      non_simple_regex.ok(),
      "Invalid non-simple-regex: {}",
      non_simple_regex.error());
    // Even if _match is empty, the RE2 will be 'ok'
    vassert(match_regex.ok(), "Invalid match regex: {}", match_regex.error());

    /*
     * How this works:
     *
     * Take a rule like this:
     *
     * RULE:[2:$1foo$2](Admin\.(.*)s/Admin\.(.*)/$1/
     *
     * And say the GSSAPI principal name is:
     * Admin.site-user/example.com@REALM.com.
     *
     * The name breaks down to:
     * service-name: Admin.site-user
     * host-name: example.com
     * realm: REALM.com
     *
     * First, the "2" at the beginning ensures there are two parameters in the
     * principal name (ignoring realm, we have the service-name and host-name).
     *
     * If we were missing the host-name, then we would only have one parameter
     * and this rule would be ignored.
     *
     * Now, the "$1foo$2".  This is the 'format'.  This formats the GSSAPI name,
     * so it can be fed into the next match regex.
     *
     * $0 - Realm
     * $1 - service-name
     * $2 - host-name
     *
     * So this 'format' takes the name and turns it into
     * "Admin.site-userfooexample.com"
     *
     * Next, we test if this formatted name matches the "match" regex which in
     * this case is "(Admin\.(.*))".  Meaning anything that begins with "Admin".
     *
     * It does, so then we move on to the replacement regex:
     * "s/Admin\.(.*)/$1/".
     *
     * The "Admin\.(.*)" is the from pattern.
     * The "$1" is the to pattern.
     *
     * This rule effectively removes the "Admin." at the beginning of the
     * formatted name:
     * "site-userfooexample.com" is the result.
     */

    ss::sstring result;

    if (_is_default && params.size() >= 2) {
        // If rule is a default rule, check to see if the realm and default
        // realm matches and then use the primary value
        if (default_realm == params.at(0)) {
            result = ss::sstring(params.at(1));
        }
    } else if (
      !params.empty()
      && params.size() - 1 == static_cast<size_t>(_number_of_components)) {
        auto base = replace_parameters(_format, params);
        if (!base) {
            return std::nullopt;
        }
        const re2::StringPiece base_piece(base->data(), base->size());
        if (_match.empty() || re2::RE2::FullMatch(base_piece, match_regex)) {
            if (_from_pattern_str.empty()) {
                result = *base;
            } else {
                result = replace_substitution(
                  *base, *_from_pattern, _to_pattern, _repeat);
            }
        }
    }

    const re2::StringPiece result_piece(result.data(), result.size());
    if (
      !result.empty()
      && re2::RE2::PartialMatch(result_piece, non_simple_regex)) {
        vlog(
          seclog.error,
          "Non-simple name {} after auth_to_local rule {}",
          result,
          *this);
        return std::nullopt;
    }

    if (!result.empty()) {
        switch (_case_change) {
        case case_change_operation::noop:
            break;

        case case_change_operation::make_lower:
            boost::algorithm::to_lower(result, std::locale::classic());
            break;

        case case_change_operation::make_upper:
            boost::algorithm::to_upper(result, std::locale::classic());
            break;
        }
    }

    if (result.empty()) {
        return std::nullopt;
    } else {
        return result;
    }
}

std::optional<ss::sstring> gssapi_rule::replace_parameters(
  std::string_view format, std::vector<std::string_view> params) {
    static thread_local const re2::RE2 parameter_parser(
      parameter_pattern, re2::RE2::Quiet);
    vassert(
      parameter_parser.ok(),
      "Invalid parameter pattern: {}",
      parameter_parser.error());

    ss::sstring result;
    re2::StringPiece format_piece(format.data(), format.size());

    re2::StringPiece text_replace, index_replace;
    while (re2::RE2::Consume(
      &format_piece,
      parameter_parser,
      &text_replace,
      nullptr,
      &index_replace)) {
        if (!text_replace.empty()) {
            result.append(text_replace.data(), text_replace.size());
        }
        if (!index_replace.empty()) {
            std::size_t index = std::numeric_limits<std::size_t>::max();
            auto conv_result = std::from_chars(
              index_replace.begin(), index_replace.end(), index);
            if (conv_result.ec != std::errc()) {
                vlog(
                  seclog.warn,
                  "Bad format in username mapping in {}",
                  std::string_view{index_replace.data(), index_replace.size()});
                return std::nullopt;
            } else if (index >= params.size()) {
                vlog(
                  seclog.warn,
                  "Invalid index provided ({}).  Outside range of "
                  "parameters (length {})",
                  index,
                  params.size());
                return std::nullopt;
            } else {
                const auto& param_index = params[index];
                result.append(param_index.data(), param_index.size());
            }
        }

        // format_piece will be empty if we have parsed all of format_piece
        // text_replace and index_replace will be empty if nothing was grouped
        // into those but re2::RE2::Consume still returns true (and doesn't
        // update format_piece)
        if (
          format_piece.empty()
          || (text_replace.empty() && index_replace.empty())) {
            break;
        }
    }

    return result;
}

ss::sstring gssapi_rule::replace_substitution(
  std::string_view base,
  const std::regex& from,
  std::string_view to,
  repeat repeat_) {
    ss::sstring replace_value;

    std::regex_constants::match_flag_type const flags
      = std::regex_constants::format_default
        | (repeat_ ? std::regex_constants::match_default : std::regex_constants::format_first_only);

    replace_value = std::regex_replace(base.data(), from, to.data(), flags);

    return replace_value;
}

gssapi_principal_mapper::gssapi_principal_mapper(
  config::binding<std::vector<ss::sstring>> principal_to_local_rules_cb)
  : _principal_to_local_rules_binding(std::move(principal_to_local_rules_cb))
  , _rules{detail::parse_rules(_principal_to_local_rules_binding())} {
    _principal_to_local_rules_binding.watch([this]() {
        _rules = detail::parse_rules(_principal_to_local_rules_binding());
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

std::ostream& operator<<(std::ostream& os, const gssapi_rule& r) {
    fmt::print(os, "{}", r);
    return os;
}

std::ostream& operator<<(std::ostream& os, const gssapi_principal_mapper& m) {
    fmt::print(os, "{}", m);
    return os;
}

std::optional<ss::sstring>
validate_kerberos_mapping_rules(const std::vector<ss::sstring>& r) noexcept {
    try {
        detail::parse_rules(r);
    } catch (const std::exception& e) {
        return e.what();
    }
    return std::nullopt;
}
} // namespace security

// explicit instantiations so as to avoid bringing in <fmt/ranges.h> in the
// header, which breaks compilation in another part of the codebase.

template<>
typename fmt::basic_format_context<fmt::appender, char>::iterator
fmt::formatter<security::gssapi_name, char, void>::format<
  fmt::basic_format_context<fmt::appender, char>>(
  security::gssapi_name const& r,
  fmt::basic_format_context<fmt::appender, char>& ctx) const {
    format_to(ctx.out(), "{}", r._primary);
    if (!r._host_name.empty()) {
        format_to(ctx.out(), "/{}", r._host_name);
    }
    if (!r._realm.empty()) {
        format_to(ctx.out(), "@{}", r._realm);
    }

    return ctx.out();
}

template<>
typename fmt::basic_format_context<fmt::appender, char>::iterator
fmt::formatter<security::gssapi_rule, char, void>::format<
  fmt::basic_format_context<fmt::appender, char>>(
  security::gssapi_rule const& r,
  fmt::basic_format_context<fmt::appender, char>& ctx) const {
    if (r._is_default) {
        return format_to(ctx.out(), "DEFAULT");
    }
    format_to(ctx.out(), "RULE:[{}:{}]", r._number_of_components, r._format);
    if (!r._match.empty()) {
        format_to(ctx.out(), "({})", r._match);
    }
    if (r._from_pattern) {
        format_to(ctx.out(), "s/{}/{}/", r._from_pattern_str, r._to_pattern);
        if (r._repeat) {
            format_to(ctx.out(), "g");
        }
    }
    switch (r._case_change) {
    case security::gssapi_rule::noop:
        break;
    case security::gssapi_rule::make_lower:
        format_to(ctx.out(), "/L");
        break;
    case security::gssapi_rule::make_upper:
        format_to(ctx.out(), "/U");
        break;
    }

    return ctx.out();
}

template<>
typename fmt::basic_format_context<fmt::appender, char>::iterator
fmt::formatter<security::gssapi_principal_mapper, char, void>::format<
  fmt::basic_format_context<fmt::appender, char>>(
  security::gssapi_principal_mapper const& r,
  fmt::basic_format_context<fmt::appender, char>& ctx) const {
    return format_to(ctx.out(), "[{}]", fmt::join(r._rules, ", "));
}
