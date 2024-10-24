/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/seastarx.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/bool_class.hh>

#include <fmt/core.h>

#include <regex>
#include <string_view>

namespace security {

class gssapi_rule {
public:
    enum case_change_operation { noop, make_lower, make_upper };
    using repeat = ss::bool_class<struct repeat_tag>;

    gssapi_rule() = default;

    gssapi_rule(
      int number_of_components,
      std::string_view format,
      std::string_view match,
      std::string_view from_pattern,
      std::string_view to_pattern,
      repeat repeat_,
      case_change_operation case_change);

    /**
     * Applies the rule to the provided parameters
     * @param default_realm The default realm to use when applying the rules
     * @param params The first element is realm, second and later elements are
     * the components of the name "a/b@FOO" -> {"FOO", "a", "b"}
     * @return The short name if this rule applies or nothing
     * @throws std::invalid_argument If there is something wrong with the rules
     */
    std::optional<ss::sstring> apply(
      std::string_view default_realm,
      std::vector<std::string_view> params) const;

private:
    friend struct fmt::formatter<gssapi_rule>;

    friend std::ostream& operator<<(std::ostream& os, const gssapi_rule& r);

    static std::optional<ss::sstring> replace_parameters(
      std::string_view format, std::vector<std::string_view> params);
    static ss::sstring replace_substitution(
      std::string_view base,
      const std::regex& from,
      std::string_view to,
      repeat repeat_);

    bool _is_default{true};
    int _number_of_components{0};
    ss::sstring _format;
    ss::sstring _match;
    // Reason for using std::regex -
    // This regex is used to replace the selected GSSAPI principal name
    // components using the regex found at the end of the rule.  The regex
    // allows for group selection and replacement using "$n" to select which
    // group to use.  Google's RE2 uses "\n" to select a group.
    // See:
    // https://github.com/google/re2/blob/4be240789d5b322df9f02b7e19c8651f3ccbf205/re2/re2.h#L455
    // So instead of having to write another regex to parse through that and
    // replace the "$" with the "\", I opted to just use std::regex.
    std::optional<std::regex> _from_pattern;
    ss::sstring _from_pattern_str;
    ss::sstring _to_pattern;
    repeat _repeat{false};
    case_change_operation _case_change{case_change_operation::noop};
};

} // namespace security

template<>
struct fmt::formatter<security::gssapi_rule> {
    using type = security::gssapi_rule;

    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    template<typename FormatContext>
    typename FormatContext::iterator
    format(const type& r, FormatContext& ctx) const;
};
