/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/bytes.h"
#include "bytes/iobuf.h"

inline bytes iobuf_to_bytes(const iobuf& in) {
    auto out = ss::uninitialized_string<bytes>(in.size_bytes());
    {
        iobuf::iterator_consumer it(in.cbegin(), in.cend());
        it.consume_to(in.size_bytes(), out.data());
    }
    return out;
}

inline iobuf bytes_to_iobuf(const bytes& in) {
    iobuf out;
    // NOLINTNEXTLINE
    out.append(reinterpret_cast<const char*>(in.data()), in.size());
    return out;
}

inline bool
bytes_type_eq::operator()(const bytes& lhs, const iobuf& rhs) const {
    if (lhs.size() != rhs.size_bytes()) {
        return false;
    }
    auto iobuf_end = iobuf::byte_iterator(rhs.cend(), rhs.cend());
    auto iobuf_it = iobuf::byte_iterator(rhs.cbegin(), rhs.cend());
    size_t bytes_idx = 0;
    const size_t max = lhs.size();
    while (iobuf_it != iobuf_end && bytes_idx < max) {
        const char r_c = *iobuf_it;
        const char l_c = lhs[bytes_idx];
        if (l_c != r_c) {
            return false;
        }
        // the equals case
        ++bytes_idx;
        ++iobuf_it;
    }
    return true;
}
