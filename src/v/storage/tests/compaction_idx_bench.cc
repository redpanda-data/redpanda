// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "compaction/key.h"
#include "model/fundamental.h"
#include "random/generators.h"
#include "storage/compacted_index.h"
#include "storage/compaction_reducers.h"
#include "test_utils/random_bytes.h"

#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/reactor.hh>
#include <seastar/testing/perf_tests.hh>

#include <cstddef>

struct reducer_bench {
    // around 50K is enough to hit the max mem limit, so
    // are benching 50/50 with/without evictions
    static constexpr size_t keys_per_iteration = 100'000;

    using reducer_t = storage::internal::compaction_key_reducer;
    using rand_t = random_generators::rng;

    static ss::future<size_t>
    reduce_n_keys(rand_t& rng, reducer_t& reducer, size_t n) {
        for (size_t i = 0; i < n; ++i) {
            model::offset o{0};
            auto key = tests::random_bytes(20, rng);

            storage::compacted_index::entry entry(
              storage::compacted_index::entry_type::key,
              compaction::compaction_key(std::move(key)),
              o,
              0);

            co_await reducer(std::move(entry)).discard_result();
        }
        co_return n;
    }
};

PERF_TEST_CN(reducer_bench, compaction_key_reducer_test) {
    rand_t rng; // start the rng sequence from the default seed every time
    reducer_t reducer; // fresh reducer every time
    co_return co_await reduce_n_keys(rng, reducer, keys_per_iteration);
}
