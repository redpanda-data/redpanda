/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 *
 * This file includes code from RapidJSON (https://rapidjson.org/)
 *
 * Copyright (C) 2015 THL A29 Limited, a Tencent company, and Milo Yip.
 *
 * Licensed under the MIT License (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License
 * at http://opensource.org/licenses/MIT
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

#include "serde/json/detail/string.h"

#include <array>

namespace {

consteval auto make_escape_table() {
    std::array<char, 256> table{};
    table['"'] = '"';
    table['/'] = '/';
    table['\\'] = '\\';
    table['b'] = '\b';
    table['f'] = '\f';
    table['n'] = '\n';
    table['r'] = '\r';
    table['t'] = '\t';

    return table;
}

constexpr auto escape = make_escape_table();

// RapidJSON codepoint -> UTF-8 encoding.
// Cf.
// https://github.com/Tencent/rapidjson/blob/24b5e7a8b27f42fa16b96fc70aade9106cf7102f/include/rapidjson/encodings.h#L102
static void append_codepoint(iobuf& buf, unsigned codepoint) {
    auto append = [&buf](char c) { buf.append(&c, 1); };

    if (codepoint <= 0x7F) {
        // 1-byte UTF-8: 0xxxxxxx (codepoints 0x00-0x7F)
        append(static_cast<char>(codepoint & 0xFF));
    } else if (codepoint <= 0x7FF) {
        // 2-byte UTF-8: 110xxxxx 10xxxxxx (codepoints 0x80-0x7FF)
        // First byte: 0xC0 (11000000) is the 2-byte prefix
        append(static_cast<char>(0xC0 | ((codepoint >> 6) & 0xFF)));
        // Subsequent bytes: 0x80 (10000000) is the continuation byte prefix
        // 0x3F (00111111) masks the least significant 6 bits
        append(static_cast<char>(0x80 | ((codepoint & 0x3F))));
    } else if (codepoint <= 0xFFFF) {
        // 3-byte UTF-8: 1110xxxx 10xxxxxx 10xxxxxx (codepoints 0x800-0xFFFF)
        // First byte: 0xE0 (11100000) is the 3-byte prefix
        append(static_cast<char>(0xE0 | ((codepoint >> 12) & 0xFF)));
        append(static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F)));
        append(static_cast<char>(0x80 | (codepoint & 0x3F)));
    } else {
        // TODO: Validate against maximum allowed codepoint 0x10FFFF.
        // RAPIDJSON_ASSERT(codepoint <= 0x10FFFF);

        // 4-byte UTF-8: 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx (codepoints
        // 0x10000-0x10FFFF) First byte: 0xF0 (11110000) is the 4-byte prefix
        append(static_cast<char>(0xF0 | ((codepoint >> 18) & 0xFF)));
        append(static_cast<char>(0x80 | ((codepoint >> 12) & 0x3F)));
        append(static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F)));
        append(static_cast<char>(0x80 | (codepoint & 0x3F)));
    }
}

// RapidJSON \uXXXX escape -> codepoint decoding.
// Cf.
// https://github.com/Tencent/rapidjson/blob/24b5e7a8b27f42fa16b96fc70aade9106cf7102f/include/rapidjson/reader.h#L906
static unsigned parse_codepoint(const char* buf) {
    unsigned codepoint = 0;
    for (size_t i = 0; i < 4; ++i) {
        codepoint <<= 4;
        if (buf[i] >= '0' && buf[i] <= '9') {
            codepoint |= (buf[i] - '0');
        } else if (buf[i] >= 'a' && buf[i] <= 'f') {
            codepoint |= (buf[i] - 'a' + 10);
        } else if (buf[i] >= 'A' && buf[i] <= 'F') {
            codepoint |= (buf[i] - 'A' + 10);
        }
    }
    return codepoint;
}

} // namespace

namespace serde::json::detail {

size_t string_parser::advance(
  ss::temporary_buffer<char>& buf, string_parser::result& err) {
    size_t start = 0;
    size_t pos = 0;

    auto do_sink_raw = [&](bool exclude_last) {
        // TODO: Consider zero-copy for large strings. `buf.share()` has an
        //   allocation overhead so it is not always the best option.
        auto sz = pos - start - exclude_last;
        if (sz > 0) {
            _sink.append(buf.get() + start, sz);
        }
    };

    while (pos < buf.size()) {
        switch (_state) {
        case state::finished_with_error:
            throw std::runtime_error(
              "string_parser is in error state and is not reusable");
        case state::finished_with_value:
            throw std::runtime_error(
              "string_parser is already done and is not reusable");

        case state::start:
            // Expect the " (start of string) character.
            if (buf[0] == '"') {
                _state = state::in_string;
                start = 1;
                pos = 1;
                continue;
            }

            err = result::invalid_json_string;
            _state = state::finished_with_error;
            return pos + 1;

        case state::in_string: {
            auto c = static_cast<unsigned char>(buf[pos]);
            pos += 1;

            if (c == '\\') {
                // Copy what we have so far.
                do_sink_raw(true);
                start = pos;
                _state = state::in_escape;
                continue;
            } else if (c == '"') {
                _state = state::finished_with_value;
                err = result::done;
                // Exclude the closing quote.
                do_sink_raw(true);
                return pos;
            } else if (c < 0x20) {
                // Invalid character in string.
                // RFC 4627: unescaped = %x20-21 / %x23-5B / %x5D-10FFFF
                err = result::invalid_json_string;
                _state = state::finished_with_error;
                return pos;
            } else if (c >= 0x80) {
                do_sink_raw(true);

                // Non-ASCII character. UTF-8 processing required.
                // Determine UTF-8 sequence length, validate start byte,
                // and begin accumulating decoded codepoint.
                if ((c & 0xE0u) == 0xC0u) {
                    // 2-byte UTF-8 sequence: 110xxxxx 10xxxxxx
                    _utf8_bytes_remaining = 1;
                    _utf8_codepoint = (c & 0x1Fu);
                } else if ((c & 0xF0u) == 0xE0u) {
                    // 3-byte UTF-8 sequence: 1110xxxx 10xxxxxx 10xxxxxx
                    _utf8_bytes_remaining = 2;
                    _utf8_codepoint = (c & 0x0Fu);
                } else if ((c & 0xF8u) == 0xF0u) {
                    // 4-byte UTF-8 sequence: 11110xxx 10xxxxxx 10xxxxxx
                    // 10xxxxxx
                    _utf8_bytes_remaining = 3;
                    _utf8_codepoint = (c & 0x07u);
                } else {
                    // Invalid UTF-8 start byte.
                    err = result::invalid_json_string;
                    _state = state::finished_with_error;
                    return pos;
                }

                // Reset start position to beginning of UTF-8 sequence.
                start = pos - 1;
                _state = state::in_utf8_sequence;
                continue;
            }
            break;
        }

        case state::in_escape: {
            auto c = buf[pos];
            pos += 1;

            if (unsigned(c) < 256 && escape[static_cast<unsigned char>(c)]) {
                // Copy the escaped character.
                _sink.append(&escape[static_cast<unsigned char>(c)], 1);
                start = pos;
                _state = state::in_string;
                continue;
            } else if (c == 'u') {
                _state = state::in_unicode;
                start = pos;
                continue;
            }

            err = result::invalid_json_string;
            _state = state::finished_with_error;
            return pos;
        }

        case state::in_unicode: {
            auto c = buf[pos];
            pos += 1;

            if (c >= '0' && c <= '9') {
                _unicode_buffer[_unicode_index++] = c;
                ++start;
            } else if (c >= 'a' && c <= 'f') {
                _unicode_buffer[_unicode_index++] = c;
                ++start;
            } else if (c >= 'A' && c <= 'F') {
                _unicode_buffer[_unicode_index++] = c;
                ++start;
            } else {
                err = result::invalid_json_string;
                _state = state::finished_with_error;
                return pos;
            }

            if (_unicode_index == 4) {
                unsigned codepoint = parse_codepoint(_unicode_buffer.data());
                // UTF-16 surrogate pair handling
                // Surrogates are used to encode codepoints beyond the BMP
                // (Basic Multilingual Plane) High surrogates: 0xD800-0xDBFF
                // (leading surrogate) Low surrogates: 0xDC00-0xDFFF (trailing
                // surrogate)
                if (codepoint >= 0xD800 && codepoint <= 0xDFFF) {
                    // This is a surrogate codepoint
                    if (codepoint <= 0xDBFF) {
                        // High surrogate (0xD800-0xDBFF), expect low surrogate
                        // to follow
                        _state = state::surrogate_start;
                        continue;
                    } else {
                        // Low surrogate (0xDC00-0xDFFF) without preceding high
                        // surrogate
                        err = result::invalid_json_string;
                        _state = state::finished_with_error;
                        return pos;
                    }
                } else {
                    append_codepoint(_sink, codepoint);
                    _unicode_index = 0;
                    _state = state::in_string;
                    start = pos;
                    continue;
                }
            } else if (_unicode_index == 8) {
                auto codepoint = parse_codepoint(_unicode_buffer.data());
                auto codepoint2 = parse_codepoint(_unicode_buffer.data() + 4);

                // Validate second codepoint is a low surrogate (0xDC00-0xDFFF)
                if (codepoint2 < 0xDC00 || codepoint2 > 0xDFFF) {
                    err = result::invalid_json_string;
                    _state = state::finished_with_error;
                    return pos;
                }

                // Decode surrogate pair to actual codepoint
                // Formula: ((high - 0xD800) << 10) | (low - 0xDC00) + 0x10000
                codepoint = (((codepoint - 0xD800) << 10)
                             | (codepoint2 - 0xDC00))
                            + 0x10000;

                append_codepoint(_sink, codepoint);
                _unicode_index = 0;
                _state = state::in_string;
                start = pos;
                continue;
            }

            break;
        }

        case state::surrogate_start: {
            auto c = buf[pos];
            pos += 1;

            if (c != '\\') {
                err = result::invalid_json_string;
                _state = state::finished_with_error;
                return pos;
            }

            _state = state::surrogate_escape;
            start = pos;
            continue;
        }

        case state::surrogate_escape: {
            auto c = buf[pos];
            pos += 1;

            if (c != 'u') {
                err = result::invalid_json_string;
                _state = state::finished_with_error;
                return pos;
            }

            _state = state::in_unicode;
            start = pos;
            continue;
        }

        case state::in_utf8_sequence: {
            auto c = static_cast<unsigned char>(buf[pos]);
            pos += 1;

            if ((c & 0xC0u) != 0x80u) {
                // Not a valid UTF-8 continuation byte (10xxxxxx pattern)
                err = result::invalid_json_string;
                _state = state::finished_with_error;
                return pos;
            }

            _utf8_codepoint = (_utf8_codepoint << 6u) | (c & 0x3Fu);
            _utf8_bytes_remaining--;

            if (_utf8_bytes_remaining == 0) {
                bool valid = false;

                // Check for overlong encodings and valid ranges.
                if (_utf8_codepoint <= 0x7F) {
                    // Should have been encoded as 1-byte sequence.
                    valid = false;
                } else if (_utf8_codepoint <= 0x7FF) {
                    // 2-byte sequence - check for overlong.
                    valid = (_utf8_codepoint >= 0x80);
                } else if (_utf8_codepoint <= 0xFFFF) {
                    // 3-byte sequence - check for overlong and surrogates.
                    valid
                      = (_utf8_codepoint >= 0x800)
                        && (_utf8_codepoint < 0xD800 || _utf8_codepoint > 0xDFFF);
                } else if (_utf8_codepoint <= 0x10FFFF) {
                    // 4-byte sequence - check for overlong.
                    valid = (_utf8_codepoint >= 0x10000);
                } else {
                    // Out of valid Unicode range.
                    valid = false;
                }

                if (!valid) {
                    err = result::invalid_json_string;
                    _state = state::finished_with_error;
                    return pos;
                }

                do_sink_raw(false);
                _state = state::in_string;
                start = pos;
                _utf8_codepoint = 0;
            }
            break;
        }
        }
    }

    do_sink_raw(false);

    err = result::need_more_data;
    return pos;
}

} // namespace serde::json::detail
