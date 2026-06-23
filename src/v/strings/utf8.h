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
#include "bytes/iobuf.h"

#include <optional>
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

/// Returns true iff \p s is valid UTF-8. Rejects surrogates, overlong
/// encodings, and truncated sequences.
bool is_valid_utf8(std::string_view s);

/// Fragment-aware UTF-8 validation with no allocation.
///
/// Returns true iff \p buf is valid UTF-8. Rejects surrogates, overlong
/// encodings, and truncated sequences.
bool is_valid_utf8(const iobuf& buf);

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

/// Returns the longest prefix of `s` ending on a complete UTF-8 code point
/// boundary with at most `max_bytes` bytes.
///
/// Suitable as a truncated lower bound for column statistics: the result is
/// lexicographically ≤ any string that begins with (or equals) the full input.
std::string_view utf8_truncate_min(std::string_view s, size_t max_bytes);

/// Returns a string that is lexicographically strictly greater than every
/// string prefixed by `utf8_truncate_min(s, max_bytes)`.
///
/// Returns std::nullopt when the valid prefix consists entirely of U+10FFFF
/// code points and no incrementable code point exists.
///
/// The result is always valid UTF-8 and suitable as a truncated upper bound
/// for column statistics. It may be up to one byte longer than max_bytes when
/// incrementing the last codepoint changes its encoding length (e.g. U+007F
/// → U+0080 grows 1→2 bytes). This matches the behaviour of the Iceberg Java
/// reference implementation; the Iceberg spec constrains code-point count, not
/// byte length.
std::optional<std::string>
utf8_truncate_max(std::string_view s, size_t max_bytes);

/// Replace invalid UTF-8 byte sequences with U+FFFD (EF BF BD).
///
/// Fast path: if \p input is already valid UTF-8, returns it unchanged
/// (zero copy, no allocation).
/// Slow path: copies replacing each ill-formed byte with U+FFFD
/// (advance-1-on-error strategy); output is at most 3x the input size.
iobuf utf8_sanitize(iobuf input);
