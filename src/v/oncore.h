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

#ifndef NDEBUG

#include "seastarx.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/smp.hh>

#define expression_in_debug_mode(x) x
#else
#define expression_in_debug_mode(x)
#endif

class oncore final {
public:
    void verify_shard_source_location(
      [[maybe_unused]] const char* file, [[maybe_unused]] int linenum) const {
        expression_in_debug_mode(vassert(
          _owner_shard == ss::this_shard_id(),
          "{}:{} - Shard missmatch -  Operation on shard: {}. Owner shard:{}",
          file,
          linenum,
          ss::this_shard_id(),
          _owner_shard));
    }

private:
    expression_in_debug_mode(ss::shard_id _owner_shard = ss::this_shard_id();)
};

// Next function should be replace with source_location in c++20 very soon
// NOLINTNEXTLINE
#define oncore_debug_verify(member)                                            \
    do {                                                                       \
        expression_in_debug_mode(member.verify_shard_source_location(          \
          (const char*)&__FILE__[vlog_internal::log_basename_start<            \
            vlog_internal::basename_index(__FILE__)>::value],                  \
          __LINE__));                                                          \
    } while (0)
