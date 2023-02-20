/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "bytes/oncore.h"

#include "seastarx.h"
#include "vassert.h"

#include <seastar/core/smp.hh>

static_assert(std::is_same_v<oncore::shard_id_type, ss::shard_id>);

oncore::oncore()
  : _owner_shard(ss::this_shard_id()) {}

void oncore::verify_shard_source_location(const char* file, int linenum) const {
    vassert(
      _owner_shard == ss::this_shard_id(),
      "{}:{} - Shard missmatch -  Operation on shard: {}. Owner shard:{}",
      file,
      linenum,
      ss::this_shard_id(),
      _owner_shard);
}
