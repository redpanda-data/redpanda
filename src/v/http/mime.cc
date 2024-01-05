/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "mime.h"

#include "vassert.h"

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <algorithm>
#include <cctype>
#include <iostream>
#include <iterator>
#include <ranges>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>

namespace {

/// tspecials : = "(" / ")" / "<" / ">" / "@" /
///               "," / ";" / ":" / "\" / <">
///               "/" / "[" / "]" / "?" / "="
///               ; Must be in quoted - string,
///               ; to use within parameter values
inline bool is_tspecial(unsigned char c) {
    // clang-format off
    return c == '(' || c == ')' || c == '<' || c == '>'  || c == '@'
        || c == ',' || c == ';' || c == ':' || c == '\\' || c == '"'
        || c == '/' || c == '[' || c == ']' || c == '?'  || c == '=';
    // clang-format on
}

/// token := 1*<any (US-ASCII) CHAR except SPACE, CTLs,
///             or tspecials >
inline bool is_token(unsigned char c) {
    return c > 32 && c < 127 && !is_tspecial(c);
}

bool is_token(const std::string_view s) {
    return std::all_of<std::string_view::const_iterator, bool(unsigned char c)>(
      s.begin(), s.end(), is_token);
}

inline bool is_vchar(unsigned char c) { return c > 32 && c < 127; }

inline bool is_obs_text(unsigned char c) { return 0x80 <= c && c <= 0xFF; }

bool is_valid_type(const std::string_view t) {
    auto sub_sep = t.find('/');
    if (sub_sep == std::string::npos) {
        return !t.empty() && is_token(t);
    } else {
        auto major = std::string_view(t).substr(0, sub_sep);
        auto sub = std::string_view(t).substr(sub_sep + 1, t.size());
        return !major.empty() && is_token(major) && !sub.empty()
               && is_token(sub);
    }
}

[[nodiscard]] std::string normalize_type(const std::string_view t) {
    std::string out;
    out.resize(t.size());

    std::transform(t.begin(), t.end(), out.begin(), [](unsigned char c) {
        return std::tolower(c);
    });

    return out;
}

void ignore_spaces(char const*& p, char const* last) {
    while (p != last && *p == ' ') {
        ++p;
    }
}

void parse_media_parameter(
  char const*& p,
  char const* end,
  std::string_view& name,
  std::string_view& value,
  std::string& value_scratch_buf,
  size_t scratch_size) {
    ignore_spaces(p, end);

    // EOF?
    if (p == end) {
        return;
    }

    // Separator and potential start of a new `name=value`
    if (*p == ';') {
        ++p; // Eat ';'
    } else {
        throw std::runtime_error("expecting parameter separator");
    }

    ignore_spaces(p, end);

    // EOF? The `name=value` is actually optional per
    // https://www.rfc-editor.org/rfc/rfc9110
    if (p == end || *p == ';') {
        return;
    }

    auto name_begin = p;
    while (p != end && is_token(*p)) {
        ++p;
    }
    if (name_begin == p) {
        throw std::runtime_error("empty or invalid parameter name");
    }
    name = {name_begin, p};

    // RFC9110 doesn't allow whitespace here, but other parsers (i.e. Go
    // standard library) are lax about it and we are too.
    ignore_spaces(p, end);

    if (p != end && *p == '=') {
        ++p; // Eat '='
    } else {
        throw std::runtime_error("expecting `=`");
    }

    // See the comment above. Not allowed but we are lax about this.
    ignore_spaces(p, end);

    auto value_start = p;
    if (p != end && *p == '\"') {
        // DQUOTE: start quoted-string
        ++p; // Eat '"'

        auto buf_pos = 0;
        while (p != end) {
            if (static_cast<size_t>(p - value_start) >= scratch_size) {
                throw std::runtime_error(
                  "parameter value exceeds tmp buffer size");
            }

            if (*p == '"') {
                // DQUOTE: end quoted-string
                break;
            } else if (*p == '\\') {
                // quoted-pair = "\" ( HTAB / SP / VCHAR / obs-text )

                ++p; // Eat '\'

                if (p == end) {
                    throw std::runtime_error("expecting quoted-pair octet");
                } else if (
                  *p != '\t' && *p != ' ' && !is_vchar(*p)
                  && !is_obs_text(*p)) {
                    throw std::runtime_error("bad quoted-pair octet");
                }
            } else {
                // qdtext = HTAB / SP / %x21 / %x23-5B / %x5D-7E / obs-text
                if (
                  *p != '\t' && *p != ' ' && *p != 0x21
                  && !(0x23 <= *p && *p <= 0x5B) && !(0x5D <= *p && *p <= 0x7E)
                  && !is_obs_text(*p)) {
                    throw std::runtime_error("bad quoted octet");
                }
            }

            value_scratch_buf[buf_pos] = *p;
            buf_pos += 1;
            ++p; // Advance.
        }

        value = {value_scratch_buf.data(), value_scratch_buf.data() + buf_pos};

        if (p != end && *p == '"') {
            ++p;
        }
        // DQUOTE: end quoted-string
    } else {
        while (p != end && is_token(*p)) {
            ++p;
        }
        if (value_start == p) {
            throw std::runtime_error("empty or invalid parameter value");
        }
        value = {value_start, p};
    }

    // Ignore trailing whitespace.
    ignore_spaces(p, end);
}

} // namespace

namespace http {

std::string format_media_type(const media_type& m) {
    std::ostringstream oss;
    oss.exceptions(std::ios::failbit);
    auto sub_sep = m.type.find('/');
    if (sub_sep == std::string::npos) {
        if (!is_token(m.type)) {
            throw std::runtime_error(
              "media type contains non-token characters");
        } else {
            std::transform(
              m.type.begin(),
              m.type.end(),
              std::ostream_iterator<char>(oss),
              [](unsigned char c) { return std::tolower(c); });
        }
    } else {
        auto major = std::string_view(m.type).substr(0, sub_sep);
        auto sub = std::string_view(m.type).substr(sub_sep + 1, m.type.size());

        if (!is_token(major) || !is_token(sub)) {
            throw std::runtime_error(
              "media type contains non-token characters");
        } else {
            std::transform(
              major.begin(),
              major.end(),
              std::ostream_iterator<char>(oss),
              [](unsigned char c) { return std::tolower(c); });
            oss << '/';
            std::transform(
              sub.begin(),
              sub.end(),
              std::ostream_iterator<char>(oss),
              [](unsigned char c) { return std::tolower(c); });
        }
    }
    for (const auto& [attr, value] : m.params) {
        if (!is_token(attr)) {
            throw std::runtime_error(
              "media attributes contain invalid characters");
        }
        oss << "; ";
        std::transform(
          attr.begin(),
          attr.end(),
          std::ostream_iterator<char>(oss),
          [](unsigned char c) { return std::tolower(c); });
        oss << "=";
        if (!is_token(value)) {
            throw std::runtime_error(
              "media attribute value encoding is not implemented");
        }
        oss << value;
    }
    return oss.str();
}

media_type parse_media_type(const std::string_view in) {
    media_type m;

    const auto type_sep_pos = in.find(';');
    if (type_sep_pos == std::string::npos) {
        throw std::runtime_error("invalid media type");
    }
    const auto type = boost::trim_copy(in.substr(0, type_sep_pos));
    if (!is_valid_type(type)) {
        throw std::runtime_error("invalid media type");
    }
    m.type = normalize_type(type);

    auto p = in.begin() + type_sep_pos;
    std::string value_scratch_buf;

    // https://stackoverflow.com/questions/686217/maximum-on-http-header-values
    const auto scratch_size = 4 * 1024; // 4KiB
    value_scratch_buf.reserve(scratch_size);

    while (p != in.end()) {
        std::string_view key;
        std::string_view value;
        parse_media_parameter(
          p, in.end(), key, value, value_scratch_buf, scratch_size);
        if (!key.empty() && !value.empty()) {
            std::string lc_key = std::string(key);
            boost::to_lower(lc_key);
            m.params.emplace(lc_key, value);
        }
    }
    vassert(p == in.end(), "input not fully consumed");

    return m;
}

} // namespace http
