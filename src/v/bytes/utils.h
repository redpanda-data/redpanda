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
#include "bytes/iobuf.h"
#include "hashing/crc32c.h"

inline void crc_extend_iobuf(crc::crc32c& crc, const iobuf& buf) {
    auto in = iobuf::iterator_consumer(buf.cbegin(), buf.cend());
    (void)in.consume(buf.size_bytes(), [&crc](const char* src, size_t sz) {
        // NOLINTNEXTLINE
        crc.extend(reinterpret_cast<const uint8_t*>(src), sz);
        return ss::stop_iteration::no;
    });
}
