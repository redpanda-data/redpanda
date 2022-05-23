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
#include "bytes/iobuf.h"

inline bool is_zero(const char* data, size_t size) {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    return data[0] == 0 && memcmp(data, data + 1, size - 1) == 0;
}

inline bool is_zero(const iobuf& buffer) {
    if (buffer.empty()) {
        return false;
    }
    bool ret = true;
    iobuf::iterator_consumer in(buffer.cbegin(), buffer.cend());
    in.consume(buffer.size_bytes(), [&ret](const char* src, size_t len) {
        if (!is_zero(src, len)) {
            ret = false;
            return ss::stop_iteration::yes;
        }
        return ss::stop_iteration::no;
    });
    return ret;
}
