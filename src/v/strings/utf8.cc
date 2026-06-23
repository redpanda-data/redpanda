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

#include <array>
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

// Incremental UTF-8 validation state. `pending` counts the continuation
// bytes still expected; `min_cont`/`max_cont` bound the next continuation
// byte (tightened after an E0/ED/F0/F4 lead to reject overlongs, surrogates,
// and code points above U+10FFFF).
struct utf8_scan_state {
    int8_t pending = 0;
    uint8_t min_cont = 0x80;
    uint8_t max_cont = 0xBF;
};

// Table that maps bytes to scanner states; pending == -1 marks bytes that
// can never start a sequence. Avoids branch mispredictions on random input.
constexpr auto utf8_lead_table = [] {
    std::array<utf8_scan_state, 256> t{};
    for (int b = 0; b < 256; ++b) {
        utf8_scan_state s{.pending = -1, .min_cont = 0x80, .max_cont = 0xBF};
        if (b < 0x80) {
            s.pending = 0; // ASCII
        } else if (b < 0xC2) {
            // bare continuation or overlong 2-byte start: never a lead
        } else if (b < 0xE0) {
            s.pending = 1;
        } else if (b < 0xF0) {
            s.pending = 2;
            if (b == 0xE0) {
                s.min_cont = 0xA0; // exclude overlong
            }
            if (b == 0xED) {
                s.max_cont = 0x9F; // exclude surrogates
            }
        } else if (b < 0xF5) {
            s.pending = 3;
            if (b == 0xF0) {
                s.min_cont = 0x90; // exclude overlong
            }
            if (b == 0xF4) {
                s.max_cont = 0x8F; // exclude > U+10FFFF
            }
        }
        t[b] = s;
    }
    return t;
}();

// Advance the state machine by one byte. Returns false if `b` is not valid
// UTF-8 at the current position; the state is then unchanged and must be
// reset before feeding more bytes.
inline bool accept_utf8_byte(utf8_scan_state& s, uint8_t b) {
    if (s.pending > 0) [[unlikely]] {
        if (b < s.min_cont || b > s.max_cont) {
            return false;
        }
        --s.pending;
        // Range constraints apply only to the first continuation byte.
        s.min_cont = 0x80;
        s.max_cont = 0xBF;
        return true;
    }
    if (b < 0x80) {
        // ASCII: a complete code point. At pending == 0 the state already
        // holds the boundary defaults, so skip the table load.
        return true;
    }
    const auto lead = utf8_lead_table[b];
    if (lead.pending < 0) {
        return false;
    }
    s = lead;
    return true;
}

// Replace ill-formed bytes in `input` with U+FFFD (advance-1-on-error
// strategy: one replacement per ill-formed byte). Bytes of the current code
// point are buffered in `seq` and copied once the code point completes, so
// fragment boundaries need no special handling.
iobuf replace_invalid_utf8(const iobuf& input) {
    iobuf result;
    result.reserve_memory(input.size_bytes());
    utf8_scan_state state;
    std::array<char, 4> seq{};
    size_t seq_len = 0;

    // Appends one at a time on purpose: n is almost always 0 or 1, and a
    // constant-size append compiles to direct stores; a single runtime-size
    // append from a prepared buffer measured slower.
    auto emit_replacements = [&result](size_t n) {
        for (size_t i = 0; i < n; ++i) {
            result.append(replacement_char.data(), replacement_char.size());
        }
    };

    auto sanitize_byte = [&](uint8_t b) {
        if (!accept_utf8_byte(state, b)) {
            // Abort any partial sequence: its lead is ill-formed and each
            // consumed continuation is then a bare continuation — one U+FFFD
            // per byte. Then re-examine `b` with fresh state.
            emit_replacements(seq_len);
            seq_len = 0;
            state = utf8_scan_state{};
            if (!accept_utf8_byte(state, b)) {
                emit_replacements(1);
                return;
            }
        }
        seq[seq_len++] = static_cast<char>(b);
        if (state.pending == 0) {
            result.append(seq.data(), seq_len);
            seq_len = 0;
        }
    };

    for (const auto& frag : input) {
        for (size_t i = 0; i < frag.size(); ++i) {
            sanitize_byte(static_cast<uint8_t>(frag.get()[i]));
        }
    }
    // Input ended mid-sequence: one U+FFFD per leftover byte.
    emit_replacements(seq_len);
    return result;
}

} // namespace

bool is_valid_utf8(std::string_view s) {
    utf8_scan_state state;
    for (const auto& c : s) {
        if (!accept_utf8_byte(state, static_cast<uint8_t>(c))) {
            return false;
        }
    }
    return state.pending == 0;
}

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

iobuf utf8_sanitize(iobuf input) {
    if (is_valid_utf8(input)) {
        return input;
    }
    return replace_invalid_utf8(input);
}
