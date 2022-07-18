// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/fundamental.h"
#include "random/generators.h"
#include "storage/compacted_index.h"
#include "storage/compaction_reducers.h"

#include <seastar/core/loop.hh>
#include <seastar/core/reactor.hh>
#include <seastar/testing/perf_tests.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_map.h>

#include <unordered_map>

struct reducer_bench {
    storage::internal::compaction_key_reducer reducer;
};

PERF_TEST_F(reducer_bench, compaction_key_reducer_test) {
    model::offset o{0};
    auto key = random_generators::get_bytes(20);

    storage::compacted_index::entry entry(
      storage::compacted_index::entry_type::key,
      storage::compaction_key(std::move(key)),
      o,
      0);

    perf_tests::start_measuring_time();
    return reducer(std::move(entry)).discard_result().finally([] {
        perf_tests::stop_measuring_time();
    });
}
