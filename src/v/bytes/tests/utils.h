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
#include "random/generators.h"

static const constexpr size_t characters_per_append = 10;

inline void append_sequence(iobuf& buf, size_t count) {
    for (size_t i = 0; i < count; i++) {
        auto str = random_generators::gen_alphanum_string(
          characters_per_append);
        buf.append(str.data(), str.size());
    }
}
