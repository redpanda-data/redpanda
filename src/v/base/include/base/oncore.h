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
#include "base/vassert.h"
#include "source_location.h"

#include <seastar/core/smp.hh>

#ifndef NDEBUG
#define expression_in_debug_mode(x) x
#else
#define expression_in_debug_mode(x)
#endif

class oncore final {
public:
    // owner shard set on construction
    oncore()
      : _owner_shard(ss::this_shard_id()) {}

    void assert_shard_source_location(
      const vlog::file_line fl = vlog::file_line::current()) const {
        vassert(
          _owner_shard == seastar::this_shard_id(),
          "{} - Shard missmatch -  Operation on shard: {}. Owner shard:{}",
          fl,
          seastar::this_shard_id(),
          _owner_shard);
    }

private:
    seastar::shard_id _owner_shard;
};

// Debug builds assert, no checks in release builds
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define oncore_debug_verify(member)                                            \
    /* NOLINTNEXTLINE(cppcoreguidelines-avoid-do-while) */                     \
    do {                                                                       \
        expression_in_debug_mode((member).assert_shard_source_location());     \
    } while (0)
