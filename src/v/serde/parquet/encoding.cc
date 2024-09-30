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

#include "serde/parquet/encoding.h"

#include <algorithm>
#include <climits>

namespace serde::parquet {

iobuf encode_plain(const chunked_vector<boolean_value>& vals) {
    iobuf buf;
    size_t i = 0;
    while (i < vals.size()) {
        size_t pass_size = std::min<size_t>(vals.size() - i, CHAR_BIT);
        uint8_t tmp = 0;
        size_t shift = 0;
        for (; shift < pass_size; ++i, ++shift) {
            if (vals[i].val) {
                tmp |= 1U << shift;
            }
        }
        buf.append(&tmp, 1);
    }
    return buf;
}

iobuf encode_plain(const chunked_vector<int32_value>& vals) {
    iobuf buf;
    for (const auto& v : vals) {
        int32_t i = ss::cpu_to_le(v.val);
        // NOLINTNEXTLINE(*reinterpret-cast*)
        buf.append(reinterpret_cast<const uint8_t*>(&i), sizeof(int32_t));
    }
    return buf;
}

iobuf encode_plain(const chunked_vector<int64_value>& vals) {
    iobuf buf;
    for (const auto& v : vals) {
        int64_t i = ss::cpu_to_le(v.val);
        // NOLINTNEXTLINE(*reinterpret-cast*)
        buf.append(reinterpret_cast<const uint8_t*>(&i), sizeof(int64_t));
    }
    return buf;
}

iobuf encode_plain(const chunked_vector<float32_value>& vals) {
    iobuf buf;
    for (const auto& v : vals) {
        // NOLINTNEXTLINE(*reinterpret-cast*)
        buf.append(reinterpret_cast<const uint8_t*>(&v.val), sizeof(float));
    }
    return buf;
}

iobuf encode_plain(const chunked_vector<float64_value>& vals) {
    iobuf buf;
    for (const auto& v : vals) {
        // NOLINTNEXTLINE(*reinterpret-cast*)
        buf.append(reinterpret_cast<const uint8_t*>(&v.val), sizeof(double));
    }
    return buf;
}

iobuf encode_plain(chunked_vector<byte_array_value> vals) {
    iobuf buf;
    for (auto& v : vals) {
        int32_t i = ss::cpu_to_le(static_cast<int32_t>(v.val.size_bytes()));
        // NOLINTNEXTLINE(*reinterpret-cast*)
        buf.append(reinterpret_cast<const uint8_t*>(&i), sizeof(int32_t));
        buf.append(std::move(v.val));
    }
    return buf;
}

iobuf encode_plain(chunked_vector<fixed_byte_array_value> vals) {
    iobuf buf;
    for (auto& v : vals) {
        buf.append(std::move(v.val));
    }
    return buf;
}

} // namespace serde::parquet
