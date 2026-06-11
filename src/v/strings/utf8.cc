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

namespace {

constexpr std::string_view replacement_char = "\xEF\xBF\xBD";

// Replace ill-formed bytes in `raw` with U+FFFD (advance-1-on-error strategy:
// one replacement per ill-formed byte).
iobuf replace_invalid_utf8(std::string_view raw) {
    iobuf result;
    const auto* data = reinterpret_cast<const utf8proc_uint8_t*>(raw.data());
    auto len = static_cast<utf8proc_ssize_t>(raw.size());
    utf8proc_ssize_t i = 0;
    while (i < len) {
        utf8proc_int32_t cp;
        utf8proc_ssize_t n = utf8proc_iterate(data + i, len - i, &cp);
        if (n > 0) {
            result.append(raw.data() + i, static_cast<size_t>(n));
            i += n;
        } else {
            result.append(replacement_char.data(), replacement_char.size());
            ++i;
        }
    }
    return result;
}

// Incremental UTF-8 validation state. `pending` counts the continuation
// bytes still expected; `min_cont`/`max_cont` bound the next continuation
// byte (tightened after an E0/ED/F0/F4 lead to reject overlongs, surrogates,
// and code points above U+10FFFF).
struct utf8_scan_state {
    int pending = 0;
    uint8_t min_cont = 0x80;
    uint8_t max_cont = 0xBF;
};

// Advance the state machine by one byte. Returns false if `b` is not valid
// UTF-8 at the current position.
inline bool accept_utf8_byte(utf8_scan_state& s, uint8_t b) {
    if (s.pending > 0) {
        if (b < s.min_cont || b > s.max_cont) {
            return false;
        }
        --s.pending;
        // Range constraints apply only to the first continuation byte.
        s.min_cont = 0x80;
        s.max_cont = 0xBF;
    } else {
        if (b < 0x80) {
            // ASCII
        } else if (b < 0xC2) {
            return false; // bare continuation or overlong 2-byte start
        } else if (b < 0xE0) {
            s.pending = 1;
        } else if (b == 0xE0) {
            s.pending = 2;
            s.min_cont = 0xA0; // exclude overlong
        } else if (b == 0xED) {
            s.pending = 2;
            s.max_cont = 0x9F; // exclude surrogates
        } else if (b < 0xF0) {
            s.pending = 2;
        } else if (b == 0xF0) {
            s.pending = 3;
            s.min_cont = 0x90; // exclude overlong
        } else if (b == 0xF4) {
            s.pending = 3;
            s.max_cont = 0x8F; // exclude > U+10FFFF
        } else if (b < 0xF5) { // 0xF1..0xF3
            s.pending = 3;
        } else {
            return false; // 0xF5..0xFF
        }
    }
    return true;
}

} // namespace

bool is_valid_utf8(const iobuf& buf) {
    utf8_scan_state state;
    for (const auto& frag : buf) {
        for (size_t i = 0; i < frag.size(); ++i) {
            if (!accept_utf8_byte(state, static_cast<uint8_t>(frag.get()[i]))) {
                return false;
            }
        }
    }
    return state.pending == 0;
}

std::expected<iobuf, utf8_sanitize_error>
utf8_sanitize(iobuf input, size_t max_bytes) {
    max_bytes = std::min(max_bytes, iobuf::max_linearize_size);
    if (is_valid_utf8(input)) {
        return std::move(input);
    }
    if (input.size_bytes() > max_bytes) {
        return std::unexpected(utf8_sanitize_error::input_too_large);
    }
    return replace_invalid_utf8(input.linearize_to_string());
}
