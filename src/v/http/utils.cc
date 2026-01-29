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

#include <boost/algorithm/string/join.hpp>
namespace {
/**
 * Each URI encoded byte is formed by a '%' and the two-digit hexadecimal
 * value of the byte.
 * from:
 * https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
 */
inline void append_hex_utf8(ss::sstring& result, char ch) {
    auto h = fmt::format("%{:02X}", static_cast<uint8_t>(ch));
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

ss::sstring uri_decode(std::string_view input) {
    size_t pos = 0;
    ss::sstring buf(input.length(), 0);

    for (auto safe = input.begin(); safe != input.end(); ++safe) {
        switch (*safe) {
        case '%': {
            int hex = 0;
            auto ch = *++safe;
            if (ch >= '0' && ch <= '9') {
                hex = (ch - '0') * 16;
            } else if (ch >= 'A' && ch <= 'F') {
                hex = (ch - 'A' + 10) * 16;
            } else if (ch >= 'a' && ch <= 'f') {
                hex = (ch - 'a' + 10) * 16;
            } else {
                buf[pos++] = '%';
                if (ch == 0) {
                    buf.resize(pos);
                    return buf;
                }
                buf[pos++] = ch;
                break;
            }

            ch = *++safe;
            if (ch >= '0' && ch <= '9') {
                hex += (ch - '0');
            } else if (ch >= 'A' && ch <= 'F') {
                hex += (ch - 'A' + 10);
            } else if (ch >= 'a' && ch <= 'f') {
                hex += (ch - 'a' + 10);
            } else {
                buf[pos++] = '%';
                buf[pos++] = *(safe - 1);
                if (ch == 0) {
                    buf.resize(pos);
                    return buf;
                }
                buf[pos++] = ch;
                break;
            }

            buf[pos++] = char(hex);
            break;
        }
        case '+':
            buf[pos++] = ' ';
            break;
        default:
            buf[pos++] = *safe;
            break;
        }
    }

    buf.resize(pos);
    return buf;
}

iobuf form_encode_data(
  const absl::flat_hash_map<ss::sstring, ss::sstring>& data) {
    std::vector<ss::sstring> pairs;
    for (const auto& [k, v] : data) {
        pairs.emplace_back(
          fmt::format(
            "{}={}",
            uri_encode(k, uri_encode_slash::yes),
            uri_encode(v, uri_encode_slash::yes)));
    }
    return iobuf::from(boost::algorithm::join(pairs, "&"));
}

} // namespace http
