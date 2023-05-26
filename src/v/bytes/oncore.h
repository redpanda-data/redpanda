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
#include "utils/source_location.h"

#ifndef NDEBUG
#define expression_in_debug_mode(x) x
#else
#define expression_in_debug_mode(x)
#endif

class oncore final {
public:
    // allow defining this class without importing seastar/core/smp.hh. a
    // static_assert in oncore.cc that this type is the same as ss::shard_id.
    using shard_id_type = unsigned;

    // owner shard set on construction
    oncore();

    void assert_shard_source_location(
      const vlog::file_line = vlog::file_line::current()) const;

private:
    shard_id_type _owner_shard;
};

// Debug builds assert, no checks in release builds
// NOLINTNEXTLINE
#define oncore_debug_verify(member)                                            \
    do {                                                                       \
        expression_in_debug_mode((member).assert_shard_source_location());     \
    } while (0)
