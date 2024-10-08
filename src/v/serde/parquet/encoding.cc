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

#include "utils/vint.h"

#include <algorithm>
#include <bit>
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

iobuf encode_levels(uint8_t max_value, const chunked_vector<uint8_t>& levels) {
    size_t bit_width = std::bit_width(max_value);
    iobuf buf;
    if (bit_width == 0) {
        bytes len = unsigned_vint::to_bytes(levels.size() << 1U);
        buf.append(len.data(), len.size());
        buf.append(&max_value, sizeof(uint8_t));
        return buf;
    }
    auto it = levels.begin();
    vassert(it != levels.end(), "bit width must be 0 if levels is empty");
    // NOTE: This implementation is simple, not optimal. A clean optimal
    // encoding can be found in parquet-go, which alternates between bitpacking
    // and run length encoding depending on which encodes better. This tradeoff
    // is also what duckdb does FWIW.
    uint8_t current_value = *it;
    uint32_t current_run = 1;
    auto flush = [&]() {
        bytes rl = unsigned_vint::to_bytes(current_run << 1U);
        buf.append(rl.data(), rl.size());
        buf.append(&current_value, sizeof(uint8_t));
    };
    for (++it; it != levels.end(); ++it) {
        uint8_t v = *it;
        if (v == current_value) {
            ++current_run;
        } else {
            flush();
            current_value = v;
            current_run = 1;
        }
    }
    flush();
    return buf;
}

} // namespace serde::parquet
