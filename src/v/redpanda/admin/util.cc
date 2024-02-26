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

#include "redpanda/admin/util.h"

namespace {

short hex_to_byte(char c) {
    if (c >= 'a' && c <= 'z') {
        return c - 'a' + 10;
    } else if (c >= 'A' && c <= 'Z') {
        return c - 'A' + 10;
    }
    return c - '0';
}

/**
 * Convert a hex encoded 2 bytes substring to char
 */
char hexstr_to_char(const std::string_view in, size_t from) {
    return static_cast<char>(
      hex_to_byte(in[from]) * 16 + hex_to_byte(in[from + 1]));
}

} // namespace

namespace admin {

bool path_decode(const std::string_view in, ss::sstring& out) {
    size_t pos = 0;
    ss::sstring buff(in.length(), 0);
    for (size_t i = 0; i < in.length(); ++i) {
        if (in[i] == '%') {
            if (i + 3 <= in.size()) {
                buff[pos++] = hexstr_to_char(in, i + 1);
                i += 2;
            } else {
                return false;
            }
        } else {
            buff[pos++] = in[i];
        }
    }
    buff.resize(pos);
    out = buff;
    return true;
}

} // namespace admin
