/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "exceptions.h"
#include "outcome.h"
#include "reflection/type_traits.h"
#include "utils/utf8.h"

#include <seastar/core/sstring.hh>

#include <cctype>
#include <charconv>
#include <chrono>
#include <type_traits>

namespace pandaproxy::parse {

namespace detail {

template<typename T>
struct is_duration : std::false_type {};

template<typename Rep, typename Period>
struct is_duration<std::chrono::duration<Rep, Period>> : std::true_type {};

template<typename T>
inline constexpr bool is_duration_v = is_duration<T>::value;

} // namespace detail

// from_chars converts from a string_view using std::from_chars.
//
// Recurses through several well-known types as a convenience. E.g.:
// using timeout_ms = named_type<std::chrono::milliseconds, struct T>;
// parse::from_chars<std::optional<timeout_ms>>{}("42");
//
// Returns outcome::result<type, std::error_code>
//
// If T (or nested T) is constructable from a string_view, no parsing is done.
// If input is empty and T is not optional, returns std::errc::invalid_argument
template<typename T>
class from_chars {
public:
    using type = std::decay_t<T>;
    using result_type = result<type>;

    static constexpr bool is_optional = reflection::is_std_optional<type>;
    static constexpr bool is_named_type = reflection::is_rp_named_type<type>;
    static constexpr bool is_duration = detail::is_duration_v<type>;
    static constexpr bool is_arithmetic = std::is_arithmetic_v<type>;
    static constexpr bool is_ss_bool = reflection::is_ss_bool_class<type>;
    static constexpr bool is_constructible_from_string_view
      = std::is_constructible_v<type, std::string_view>;
    static constexpr bool is_constructible_from_sstring
      = std::is_constructible_v<type, ss::sstring>;

    static_assert(
      is_optional || is_named_type || is_duration || is_arithmetic || is_ss_bool
        || is_constructible_from_string_view || is_constructible_from_sstring,
      "from_chars not defined for T");

    result_type operator()(std::string_view in) noexcept {
        if (unlikely(contains_control_character(in))) {
            return std::errc::invalid_argument;
        }
        if constexpr (is_optional) {
            if (in.empty()) {
                return std::nullopt;
            }
            using value_type = typename type::value_type;
            return wrap(from_chars<value_type>{}(in));
        } else if (in.empty()) {
            return std::errc::invalid_argument;
        } else if constexpr (is_constructible_from_string_view) {
            return type(in);
        } else if constexpr (is_constructible_from_sstring) {
            return type(ss::sstring(in));
        } else if constexpr (is_named_type) {
            using value_type = typename type::type;
            return wrap(from_chars<value_type>{}(in));
        } else if constexpr (is_duration) {
            using value_type = typename type::rep;
            return wrap(from_chars<value_type>{}(in));
        } else if constexpr (is_ss_bool) {
            return type(in == "true" || in == "TRUE" || in == "1");
        } else if constexpr (is_arithmetic) {
            return do_from_chars(in);
        }
        return std::errc::invalid_argument;
    }

private:
    result_type do_from_chars(std::string_view in) {
        type val;
        const auto [ptr, ec] = std::from_chars(
          in.data(), in.data() + in.size(), val);
        if (ec != std::errc()) {
            return std::errc{ec};
        } else if (ptr != in.data() + in.size()) {
            return std::errc::invalid_argument;
        }
        return val;
    }

    template<typename U>
    result_type wrap(U&& inner) {
        if (inner) {
            return type(std::forward<U>(inner).value());
        }
        return inner.error();
    }
};

} // namespace pandaproxy::parse
