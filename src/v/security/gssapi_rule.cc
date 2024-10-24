/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "security/gssapi_rule.h"

#include "base/vassert.h"
#include "base/vlog.h"
#include "security/logger.h"

#include <boost/algorithm/string/case_conv.hpp>
#include <re2/re2.h>

#include <charconv>
#include <optional>

namespace security {

namespace gssapi {
static constexpr std::string_view non_simple_pattern = R"([/@])";
static constexpr std::string_view parameter_pattern = R"(([^$]*)(\$(\d*))?)";
} // namespace gssapi

gssapi_rule::gssapi_rule(
  int number_of_components,
  // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
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
      gssapi::non_simple_pattern, re2::RE2::Quiet);
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
      gssapi::parameter_pattern, re2::RE2::Quiet);
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

    const std::regex_constants::match_flag_type flags
      = std::regex_constants::format_default
        | (repeat_ ? std::regex_constants::match_default : std::regex_constants::format_first_only);

    replace_value = std::regex_replace(base.data(), from, to.data(), flags);

    return replace_value;
}

std::ostream& operator<<(std::ostream& os, const gssapi_rule& r) {
    fmt::print(os, "{}", r);
    return os;
}

} // namespace security

template<>
typename fmt::basic_format_context<fmt::appender, char>::iterator
fmt::formatter<security::gssapi_rule, char, void>::format<
  fmt::basic_format_context<fmt::appender, char>>(
  const security::gssapi_rule& r,
  fmt::basic_format_context<fmt::appender, char>& ctx) const {
    if (r._is_default) {
        return fmt::format_to(ctx.out(), "DEFAULT");
    }
    fmt::format_to(
      ctx.out(), "RULE:[{}:{}]", r._number_of_components, r._format);
    if (!r._match.empty()) {
        fmt::format_to(ctx.out(), "({})", r._match);
    }
    if (r._from_pattern) {
        fmt::format_to(
          ctx.out(), "s/{}/{}/", r._from_pattern_str, r._to_pattern);
        if (r._repeat) {
            fmt::format_to(ctx.out(), "g");
        }
    }
    switch (r._case_change) {
    case security::gssapi_rule::noop:
        break;
    case security::gssapi_rule::make_lower:
        fmt::format_to(ctx.out(), "/L");
        break;
    case security::gssapi_rule::make_upper:
        fmt::format_to(ctx.out(), "/U");
        break;
    }

    return ctx.out();
}
