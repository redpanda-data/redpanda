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

#include "strings/utf8.h"

#include <optional>
#include <string>
#include <string_view>
#include <utf8proc.h>

thread_local bool permit_unsafe_log_operation::_flag = false;

std::string_view utf8_truncate_min(std::string_view s, size_t max_bytes) {
    if (s.size() <= max_bytes) {
        return s;
    }
    const auto* data = reinterpret_cast<const utf8proc_uint8_t*>(s.data());
    auto limit = static_cast<utf8proc_ssize_t>(max_bytes);
    utf8proc_ssize_t pos = 0;
    while (pos < limit) {
        utf8proc_int32_t cp;
        utf8proc_ssize_t n = utf8proc_iterate(data + pos, limit - pos, &cp);
        if (n <= 0) {
            break; // invalid or incomplete sequence
        }
        pos += n;
    }
    return s.substr(0, static_cast<size_t>(pos));
}

std::optional<std::string>
utf8_truncate_max(std::string_view s, size_t max_bytes) {
    std::string_view prefix = utf8_truncate_min(s, max_bytes);
    const auto* data = reinterpret_cast<const utf8proc_uint8_t*>(prefix.data());
    auto len = static_cast<utf8proc_ssize_t>(prefix.size());

    // Walk backward through code points, trying to increment each one.
    // Most strings will succeed on the first attempt (the last code point).
    utf8proc_ssize_t cp_end = len;
    while (cp_end > 0) {
        // Find the start of the last code point in [0, cp_end).
        utf8proc_ssize_t cp_start = cp_end - 1;
        while (cp_start > 0 && (data[cp_start] & 0xC0) == 0x80) {
            --cp_start;
        }
        utf8proc_int32_t cp;
        utf8proc_ssize_t n = utf8proc_iterate(
          data + cp_start, cp_end - cp_start, &cp);
        if (n > 0 && cp >= 0) {
            utf8proc_int32_t next_cp = cp + 1;
            // Skip the surrogate range (U+D800–U+DFFF), which is not valid
            // in UTF-8.
            if (next_cp >= 0xD800 && next_cp <= 0xDFFF) {
                next_cp = 0xE000;
            }
            if (next_cp <= 0x10FFFF) {
                utf8proc_uint8_t encoded[4];
                utf8proc_ssize_t enc_len = utf8proc_encode_char(
                  next_cp, encoded);
                std::string result(
                  prefix.data(), static_cast<size_t>(cp_start));
                result.append(
                  reinterpret_cast<const char*>(encoded),
                  static_cast<size_t>(enc_len));
                return result;
            }
        }
        // This code point is at its maximum; try the previous one.
        cp_end = cp_start;
    }
    return std::nullopt;
}
