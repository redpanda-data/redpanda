/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "bytes/random.h"
#include "random/generators.h"
#include "storage/compacted_index.h"
#include "storage/compaction_reducers.h"

#include <seastar/testing/thread_test_case.hh>

SEASTAR_THREAD_TEST_CASE(compaction_reducer_key_clash_test) {
    // Insert three elements with the same key in the reducer
    // and validate that the one with the largest offset wins.

    storage::internal::compaction_key_reducer reducer{16_KiB};

    auto key = random_generators::get_bytes(20);

    // natural offset 0, rp offset 0
    storage::compacted_index::entry entry_at_0(
      storage::compacted_index::entry_type::key,
      storage::compaction_key(key),
      model::offset(0),
      0);

    // natural index 1, rp offset 5 (should win)
    storage::compacted_index::entry entry_at_5(
      storage::compacted_index::entry_type::key,
      storage::compaction_key(key),
      model::offset(5),
      0);

    // natural index 2, rp offset 1
    storage::compacted_index::entry entry_at_1(
      storage::compacted_index::entry_type::key,
      storage::compaction_key(key),
      model::offset(1),
      0);

    reducer(std::move(entry_at_0)).get();
    reducer(std::move(entry_at_5)).get();
    reducer(std::move(entry_at_1)).get();

    auto bitmap = reducer.end_of_stream();
    BOOST_REQUIRE_EQUAL(bitmap.minimum(), 1);
    BOOST_REQUIRE_EQUAL(bitmap.maximum(), 1);
}

SEASTAR_THREAD_TEST_CASE(compaction_reducer_max_mem_usage_test) {
    storage::internal::compaction_key_reducer reducer{16_KiB};

    // Empirically, 200 of the entries below use 16KiB of memory.
    // Test that the index stays within the memory usage bounds.
    for (size_t i = 0; i < 1000; ++i) {
        auto key = random_generators::get_bytes(20);
        storage::compacted_index::entry entry(
          storage::compacted_index::entry_type::key,
          storage::compaction_key(std::move(key)),
          model::offset(i),
          0);

        reducer(std::move(entry)).get();
        BOOST_REQUIRE_LE(reducer.idx_mem_usage(), 16_KiB);
    }
}
