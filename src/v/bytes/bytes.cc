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
#include "bytes/bytes.h"

ss::sstring to_hex(bytes_view b) {
    ss::sstring out = ss::uninitialized_string(b.size() * 2);
    to_hex(out, 0, b);
    return out;
}

ss::sstring to_hex(const bytes& b) { return to_hex(bytes_view(b)); }

size_t to_hex(ss::sstring& out, size_t pos, bytes_view data) {
    static constexpr std::string_view digits{"0123456789abcdef"};
    for (size_t i = 0; i < data.size(); ++i) {
        uint8_t x = data[i];
        out[pos + i * 2] = digits[x >> uint8_t(4)];
        out[pos + i * 2 + 1] = digits[x & uint8_t(0xf)];
    }
    return data.size() * 2;
}

std::ostream& operator<<(std::ostream& os, const bytes& b) {
    return os << bytes_view(b);
}

std::ostream& operator<<(std::ostream& os, const bytes_view& b) {
    fmt::print(os, "{{bytes:{}}}", b.size());
    return os;
}
