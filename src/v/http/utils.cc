/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "http/utils.h"

#include "bytes/bytes.h"

namespace {

inline void append_hex_utf8(ss::sstring& result, char ch) {
    bytes b = {static_cast<uint8_t>(ch)};
    result.append("%", 1);
    auto h = to_hex(b);
    result.append(h.data(), h.size());
}

} // namespace

namespace http {

ss::sstring uri_encode(std::string_view input, uri_encode_slash encode_slash) {
    // The function defined here:
    //     https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
    ss::sstring result;
    for (auto ch : input) {
        if (
          (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z')
          || (ch >= '0' && ch <= '9') || (ch == '_') || (ch == '-')
          || (ch == '~') || (ch == '.')) {
            result.append(&ch, 1);
        } else if (ch == '/') {
            if (encode_slash) {
                result.append("%2F", 3);
            } else {
                result.append(&ch, 1);
            }
        } else {
            append_hex_utf8(result, ch);
        }
    }
    return result;
}

} // namespace http
