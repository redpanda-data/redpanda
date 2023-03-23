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

#include "likely.h"

#include <boost/locale.hpp>
#include <boost/locale/encoding_utf.hpp>

#include <string>
#include <string_view>
#include <vector>

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

template<typename T>
concept ExceptionThrower = requires(T obj) {
    obj.conversion_error();
};

struct default_utf8_thrower {
    [[noreturn]] [[gnu::cold]] void conversion_error() {
        throw std::runtime_error("Cannot decode string as UTF8");
    }
};

struct default_control_character_thrower {
    explicit default_control_character_thrower(
      std::string_view unsanitized_string)
      : sanitized_string(replace_control_chars_in_string(unsanitized_string)) {}
    virtual ~default_control_character_thrower() = default;
    [[noreturn]] [[gnu::cold]] virtual void conversion_error() {
        throw std::runtime_error(
          "String contains control character: " + sanitized_string);
    }

    const std::string& get_sanitized_string() const { return sanitized_string; }

private:
    std::string sanitized_string;
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
    validate_no_control(s, default_control_character_thrower{s});
}

template<typename Thrower>
requires ExceptionThrower<Thrower>
inline void validate_utf8(std::string_view s, Thrower&& thrower) {
    try {
        boost::locale::conv::utf_to_utf<char>(
          s.begin(), s.end(), boost::locale::conv::stop);
    } catch (const boost::locale::conv::conversion_error& ex) {
        thrower.conversion_error();
    }
}

inline void validate_utf8(std::string_view s) {
    validate_utf8(s, default_utf8_thrower{});
}
