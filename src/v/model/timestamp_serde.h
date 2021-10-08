/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "model/timestamp.h"
#include "serde/serde.h"

namespace model {

// ADL helpers for interfacing with the serde library.

inline void
read_nested(iobuf_parser& in, timestamp& ts, size_t const bytes_left_limit) {
    model::timestamp::type val;
    serde::read_nested(in, val, bytes_left_limit);
    ts = model::timestamp{val};
}

inline void write(iobuf& out, timestamp ts) { serde::write(out, ts.value()); }

} // namespace model
