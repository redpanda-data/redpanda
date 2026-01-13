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

#include <boost/container/static_vector.hpp>
#include <boost/locale.hpp>
#include <boost/locale/encoding_utf.hpp>
#include <boost/locale/utf.hpp>

#include <concepts>
#include <iterator>
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

namespace utf {

using code_point_t = boost::locale::utf::code_point;
using utf_traits_t = boost::locale::utf::utf_traits<char>;

template<typename T>
concept char_iterator = std::same_as<std::iter_value_t<T>, char>;

/// \brief Checks whether the bytes in [begin, end) are a valid utf-8 sequence.
/// \param begin a forward iterator to the start of the byte sequence.
/// \param end a forward iterator to the end of the byte sequence.
/// \param max_read_bytes the function will only validated code points that
/// start within `max_read_bytes` of `begin`.
/// \return True if a valid utf-8 sequence, false otherwise.
template<char_iterator T>
bool is_valid_utf8(T begin, T end, size_t max_read_bytes) {
    size_t read_bytes = 0;
    while (begin != end) {
        const boost::locale::utf::code_point c
          = boost::locale::utf::utf_traits<char>::decode(begin, end);
        if (!boost::locale::utf::is_valid_codepoint(c)) {
            return false;
        }
        read_bytes += utf_traits_t::width(c);
        if (read_bytes >= max_read_bytes) {
            return true;
        }
    }
    return true;
}

/// \brief Finds the incomplete codepoint of a truncated utf-8 encoded string if
/// one exists. Assumes that the utf-8 string was valid prior to truncation.
/// \param rbegin the begin reverse iterator to the truncated utf-8 string.
/// \param rend the end reverse iterator to the truncated utf-8 string.
/// \return the number of bytes the incomplete codepoint on the end of the
/// string is. 0 if last codepoint is valid.
template<char_iterator T>
size_t find_incomplete_code_point(T rbegin, T rend) {
    boost::container::static_vector<char, 4> rev_code_units{};
    for (auto curr = rbegin; curr != rend; ++curr) {
        rev_code_units.push_back(*curr);
        if (utf_traits_t::is_lead(*curr)) {
            break;
        }
    }

    auto crbegin = rev_code_units.crbegin();
    const auto c = utf_traits_t::decode(crbegin, rev_code_units.crend());
    if (!boost::locale::utf::is_valid_codepoint(c)) {
        return rev_code_units.size();
    }

    return 0;
}

struct utf32_code_point {
    // `code_point` is default constructed to an invalid state so that a default
    // constructed `utf32_code_point` can be differentiated from one constructed
    // with a valid code_point.
    code_point_t code_point{boost::locale::utf::illegal};

    bool operator==(const utf32_code_point&) const = default;

    /// \brief The length in bytes of this code point's utf-8 encoding.
    unsigned utf8_encoding_length() const {
        return utf_traits_t::width(code_point);
    }

    /// \brief Encodes the utf-32 code point to utf-8
    /// Note that only the [0, utf8_encoding_length()) indices in the returned
    /// array represent the utf-8 encoding. The remaining indices will just be
    /// zero-initialized.
    /// \returns an array containing the utf-8 encoding.
    std::array<char, 4> utf8_encoding() const {
        std::array<char, 4> ret{};
        utf_traits_t::encode(code_point, ret.begin());
        return ret;
    }

    /// \brief Tries to increment the utf-32 code point.
    /// Only increments the code point if the original utf-8 encoding byte
    /// length can be preserved. Result may be larger than (c + 1) in order to
    /// ensure a valid code point.
    /// \return a new code point greater than `c` if possible. Otherwise
    /// std::nullopt is returned.
    std::optional<utf32_code_point> try_increment() const {
        if (code_point >= 0x10FFFF) {
            // value limit for utf-32 code points.
            return {};
        }

        auto new_c = code_point + 1;

        if (new_c >= 0xD800 && new_c <= 0xDFFF) {
            // avoid utf-16 surrogate range.
            new_c = 0xE000;
        }

        if (utf_traits_t::width(new_c) > utf_traits_t::width(code_point)) {
            // ensure utf-8 byte length for the code point remains the same.
            return {};
        }

        return utf32_code_point{new_c};
    }
};

/**
 * Adapts a valid reversed utf-8 byte sequence to a utf-32 code point sequence.
 */
template<char_iterator I>
class utf32_reverse_iterator {
public:
    using value_type = utf32_code_point;
    using difference_type = void;
    using pointer = const value_type*;
    using reference = const value_type&;
    using iterator_category = std::forward_iterator_tag;

    /// Note that this constructor expects reverse iterators to a valid utf-8
    /// byte sequence.
    /// \param start an iterator to the start of a valid reversed utf-8 byte
    /// sequence.
    /// \param end an iterator to the end of a valid reversed utf-8 byte
    /// sequence.
    utf32_reverse_iterator(I start, I end)
      : _curr(start)
      , _end(end)
      , _current() {
        decode_next();
    }

    reference operator*() const noexcept { return _current; }
    pointer operator->() const noexcept { return &_current; }

    utf32_reverse_iterator& operator++() {
        decode_next();
        return *this;
    }
    utf32_reverse_iterator operator++(int) {
        auto tmp = *this;
        ++*this;
        return tmp;
    }

    bool operator==(const utf32_reverse_iterator& o) const noexcept {
        return _curr == o._curr && _current == o._current;
    }
    bool operator!=(const utf32_reverse_iterator& o) const noexcept {
        return !(*this == o);
    }

private:
    I _curr;
    I _end;
    utf32_code_point _current;

    void decode_next() {
        if (_curr == _end) {
            _current = {};
            return;
        }

        boost::container::static_vector<char, 4> rev_code_units{};
        for (; _curr != _end; ++_curr) {
            rev_code_units.push_back(*_curr);
            if (utf_traits_t::is_lead(*_curr)) {
                break;
            }
        }

        ++_curr;

        auto crbegin = rev_code_units.crbegin();
        const auto c = utf_traits_t::decode(crbegin, rev_code_units.crend());
        _current = {c};
    }
};

} // namespace utf
