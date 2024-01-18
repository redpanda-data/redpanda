/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include <optional>
#include <regex>
#include <stdexcept>
#include <string>

// an adaptation of https://llvm.org/doxygen/StringSwitch_8h_source.html
// with a smaller surface area
// TODO(agallego) - enable the following methods in c++20
// https://en.cppreference.com/w/cpp/string/basic_string_view/starts_with
// when std::string_view adds them
// string_switch& EndsWith(std::string_view S, T value);
// string_switch& starts_with(std::string_view S, T value);

/// A switch()-like statement whose match_all are string literals.
///
/// The string_switch class is a simple form of a switch() statement that
/// determines whether the given string matches one of the given string
/// literals. The template type parameter \p T is the type of the value that
/// will be returned from the string-switch expression. For example,
/// the following code switches on the name of a color in \c argv[i]:
///
/// \code
/// Color color = string_switch<Color>(argv[i])
///   .match("red", Red)
///   .match("orange", Orange)
///   .match("yellow", Yellow)
///   .match("green", Green)
///   .match("blue", Blue)
///   .match("indigo", Indigo)
///   .match_all("violet", "purple", Violet)
///   .default_match(UnknownColor);
/// \endcode
template<typename T, typename R = T>
class string_switch {
    /// The string we are matching.
    const std::string_view view;

    /// The pointer to the result of this switch statement, once known,
    /// null before that.
    std::optional<T> result;

public:
    constexpr explicit string_switch(std::string_view S)
      : view(S)
      , result() {}

    // string_switch is not copyable.
    string_switch(const string_switch&) = delete;

    // string_switch is not assignable due to 'view' being 'const'.
    void operator=(const string_switch&) = delete;
    void operator=(string_switch&& other) = delete;

    string_switch(string_switch&& other)
      : view(other.view)
      , result(std::move(other.result)) {}

    ~string_switch() = default;

    // match-sensitive match matchers
    constexpr string_switch& match(std::string_view S, T value) {
        if (!result && view == S) {
            result = std::move(value);
        }
        return *this;
    }

    constexpr string_switch&
    match_all(std::string_view S0, std::string_view S1, T value) {
        return match(S0, value).match(S1, value);
    }

    constexpr string_switch& match_all(
      std::string_view S0, std::string_view S1, std::string_view S2, T value) {
        return match(S0, value).match_all(S1, S2, value);
    }

    constexpr string_switch& match_all(
      std::string_view S0,
      std::string_view S1,
      std::string_view S2,
      std::string_view S3,
      T value) {
        return match(S0, value).match_all(S1, S2, S3, value);
    }

    constexpr string_switch& match_all(
      std::string_view S0,
      std::string_view S1,
      std::string_view S2,
      std::string_view S3,
      std::string_view S4,
      T value) {
        return match(S0, value).match_all(S1, S2, S3, S4, value);
    }

    constexpr string_switch& match_all(
      std::string_view S0,
      std::string_view S1,
      std::string_view S2,
      std::string_view S3,
      std::string_view S4,
      std::string_view S5,
      T value) {
        return match(S0, value).match_all(S1, S2, S3, S4, S5, value);
    }

    constexpr string_switch& match_all(
      std::string_view S0,
      std::string_view S1,
      std::string_view S2,
      std::string_view S3,
      std::string_view S4,
      std::string_view S5,
      std::string_view S6,
      T value) {
        return match(S0, value).match_all(S1, S2, S3, S4, S5, S6, value);
    }

    constexpr string_switch& match_all(
      std::string_view S0,
      std::string_view S1,
      std::string_view S2,
      std::string_view S3,
      std::string_view S4,
      std::string_view S5,
      std::string_view S6,
      std::string_view S7,
      T value) {
        return match(S0, value).match_all(S1, S2, S3, S4, S5, S6, S7, value);
    }

    constexpr string_switch& match_all(
      std::string_view S0,
      std::string_view S1,
      std::string_view S2,
      std::string_view S3,
      std::string_view S4,
      std::string_view S5,
      std::string_view S6,
      std::string_view S7,
      std::string_view S8,
      T value) {
        return match(S0, value).match_all(
          S1, S2, S3, S4, S5, S6, S7, S8, value);
    }

    constexpr string_switch& match_all(
      std::string_view S0,
      std::string_view S1,
      std::string_view S2,
      std::string_view S3,
      std::string_view S4,
      std::string_view S5,
      std::string_view S6,
      std::string_view S7,
      std::string_view S8,
      std::string_view S9,
      T value) {
        return match(S0, value).match_all(
          S1, S2, S3, S4, S5, S6, S7, S8, S9, value);
    }

    constexpr string_switch& match_expr(std::string_view expr, T value) {
        if (
          !result
          && std::regex_search(
            std::string{view.data(), view.size()},
            std::regex{expr.data(), expr.size()})) {
            result = std::move(value);
        }
        return *this;
    }

    constexpr R default_match(T value) {
        if (result) {
            return std::move(*result);
        }
        return value;
    }

    constexpr operator R() { // NOLINT
        if (!result) {
            throw std::runtime_error(
              std::string(
                "Fell off the end of a string-switch while matching: ")
              + std::string(view));
        }
        return std::move(*result);
    }
};
