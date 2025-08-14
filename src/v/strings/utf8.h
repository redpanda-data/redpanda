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

#include "base/likely.h"
#include "base/seastarx.h"

#include <boost/locale.hpp>
#include <boost/locale/encoding_utf.hpp>
#include <boost/locale/utf.hpp>

#include <string>
#include <string_view>

/*
 * Used to access/modify the flag that permits/prevents reporting an error
 * if a control character is found within a string
 */
class permit_unsafe_log_operation {
public:
    static bool get() { return _flag; }
    static void set(bool flag) { _flag = flag; }

private:
    static thread_local bool _flag;
};

constexpr bool is_lower_control_char(char c) { return 0x00 <= c && c <= 0x1f; }

constexpr bool is_high_control_char(char c) { return c == 0x7f; };

constexpr bool is_control_char(char c) {
    return is_lower_control_char(c) || is_high_control_char(c);
}

constexpr void write_control_char(char c, std::string& out) {
    if (is_lower_control_char(c)) [[unlikely]] {
        // This will convert the control character to the control
        // character picture (https://en.wikipedia.org/wiki/Control_Pictures)
        out.append({char(0xe2), char(0x90), char(0x80 + c)});
    } else if (is_high_control_char(c)) [[unlikely]] {
        // Special case for the delete character
        out.append({char(0xe2), char(0x90), char(0xa1)});
    } else [[likely]] {
        out.push_back(c);
    }
}

inline std::string replace_control_chars_in_string(std::string_view s) {
    std::string rv;
    rv.reserve(s.size());
    std::for_each(
      s.begin(), s.end(), [&rv](char c) { write_control_char(c, rv); });

    return rv;
}

/// Used to report an invalid character
struct invalid_character_exception : public std::runtime_error {
    using std::runtime_error::runtime_error;
};

/// Used to report an invalid UTF8 character
struct invalid_utf8_exception : public invalid_character_exception {
    using invalid_character_exception::invalid_character_exception;
};

template<typename T>
concept ExceptionThrower = requires(const T obj) { obj.conversion_error(); };

struct default_utf8_thrower {
    [[noreturn]] [[gnu::cold]] void conversion_error() const {
        throw invalid_utf8_exception("Cannot decode string as UTF8");
    }
};

/// Used to report a control character being present
struct control_character_present_exception
  : public invalid_character_exception {
    using invalid_character_exception::invalid_character_exception;
};

struct default_control_character_thrower {
    virtual ~default_control_character_thrower() = default;
    [[noreturn]] [[gnu::cold]] virtual void conversion_error() const {
        throw control_character_present_exception(
          "String contains control character");
    }

private:
};

inline bool contains_control_character(std::string_view v) {
    return std::any_of(
      v.begin(), v.end(), [](char c) { return is_control_char(c); });
}

void validate_no_control(std::string_view s, ExceptionThrower auto thrower) {
    if (
      !permit_unsafe_log_operation::get()
      && unlikely(contains_control_character(s))) {
        thrower.conversion_error();
    }
}

inline void validate_no_control(std::string_view s) {
    validate_no_control(s, default_control_character_thrower{});
}

/// \brief Truncates incomplete character sequences from the provided string.
/// Throws on invalid character sequences hence also validates
/// \param s a string view to validate_and_truncate
/// \return the length of the longest valid utf8 substring starting at s.begin()
inline size_t validate_and_truncate(std::string_view s) {
    auto begin = s.cbegin();
    auto end = s.cend();

    size_t valid_length{0};
    while (begin != end) {
        const boost::locale::utf::code_point c
          = boost::locale::utf::utf_traits<char>::decode(begin, end);
        if (c == boost::locale::utf::illegal) {
            throw invalid_utf8_exception("Cannot decode string as UTF8");
        }
        if (c == boost::locale::utf::incomplete) {
            return valid_length;
        }
        valid_length = (begin - s.cbegin());
    }
    return valid_length;
}

inline bool is_valid_utf8(std::string_view s) {
    auto begin = s.cbegin();
    auto end = s.cend();

    while (begin != end) {
        const boost::locale::utf::code_point c
          = boost::locale::utf::utf_traits<char>::decode(begin, end);
        if (!boost::locale::utf::is_valid_codepoint(c)) {
            return false;
        }
        // continue
    }
    return true;
}

template<typename Thrower>
requires ExceptionThrower<Thrower>
inline void validate_utf8(std::string_view s, const Thrower& thrower) {
    if (!is_valid_utf8(s)) {
        thrower.conversion_error();
    }
}

inline void validate_utf8(std::string_view s) {
    validate_utf8(s, default_utf8_thrower{});
}
