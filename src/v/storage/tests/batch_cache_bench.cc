// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "storage/batch_cache.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/temporary_buffer.hh>
#include <seastar/testing/perf_tests.hh>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <memory>
#include <numeric>
#include <optional>
#include <random>

namespace {

storage::batch_cache::reclaim_options make_opts() {
    return {
      .growth_window = std::chrono::milliseconds(3000),
      .stable_window = std::chrono::milliseconds(10000),
      .min_size = 128 << 10,
      .max_size = 4 << 20,
    };
}

model::record_batch make_batch(model::offset base, size_t value_size) {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, base);
    iobuf value;
    value.append(ss::temporary_buffer<char>(value_size));
    builder.add_raw_kv(iobuf{}, std::move(value));
    return std::move(builder).build();
}

} // namespace

/*
 * measures the cost of evicting ranges under memory pressure. small batches
 * pack ~190 to a 32KiB range, so a 4MiB reclaim (the default reclaim_max_size,
 * i.e. the quantum of a single reclaim upcall under sustained pressure)
 * covers ~128 ranges and ~24k index entries.
 */
struct batch_cache_reclaim_bench {
    static constexpr size_t reclaim_bytes = 4 << 20;
    static constexpr size_t batch_value_size = 100;

    storage::batch_cache cache;
    storage::batch_cache_index index;

    batch_cache_reclaim_bench()
      : cache(make_opts())
      , index(cache) {}

    ~batch_cache_reclaim_bench() { cache.stop().get(); }

    void fill() {
        for (int64_t o = 0; cache.size_bytes() < reclaim_bytes; ++o) {
            index.put(
              make_batch(model::offset(o), batch_value_size),
              storage::batch_cache::is_dirty_entry::no);
        }
    }
};

/*
 * the synchronous reclaim call: this is what runs inside the seastar memory
 * allocator's reclaim upcall, on the allocating fiber's critical path.
 */
PERF_TEST_F(batch_cache_reclaim_bench, reclaim_sync) {
    fill();
    perf_tests::start_measuring_time();
    auto reclaimed = cache.reclaim(reclaim_bytes);
    perf_tests::stop_measuring_time();
    perf_tests::do_not_optimize(reclaimed);
    // release any deferred state outside the measured region so iterations
    // are independent
    cache.clear();
}

/*
 * total reclamation work including any index cleanup deferred by reclaim():
 * quantifies whether deferring changed the overall cost or just moved it.
 */
PERF_TEST_F(batch_cache_reclaim_bench, reclaim_total) {
    fill();
    perf_tests::start_measuring_time();
    auto reclaimed = cache.reclaim(reclaim_bytes);
    cache.clear();
    perf_tests::stop_measuring_time();
    perf_tests::do_not_optimize(reclaimed);
}

/*
 * lookup path costs. single-record batches are placed at even offsets so that
 * even lookups hit and odd lookups miss after the containment check.
 */
struct batch_cache_lookup_bench {
    static constexpr int64_t num_batches = 100'000;
    static constexpr size_t batch_value_size = 100;
    static constexpr size_t lookups_per_iteration = 1000;

    storage::batch_cache cache;
    storage::batch_cache_index index;

    batch_cache_lookup_bench()
      : cache(make_opts())
      , index(cache) {
        for (int64_t i = 0; i < num_batches; ++i) {
            index.put(
              make_batch(model::offset(2 * i), batch_value_size),
              storage::batch_cache::is_dirty_entry::no);
        }
    }

    ~batch_cache_lookup_bench() { cache.stop().get(); }
};

PERF_TEST_F(batch_cache_lookup_bench, get_hit) {
    model::offset o{0};
    for (size_t i = 0; i < lookups_per_iteration; ++i) {
        auto batch = index.get(o);
        perf_tests::do_not_optimize(batch);
        o = model::offset((o() + 2) % (2 * num_batches));
    }
    return lookups_per_iteration;
}

PERF_TEST_F(batch_cache_lookup_bench, get_miss) {
    model::offset o{1};
    for (size_t i = 0; i < lookups_per_iteration; ++i) {
        auto batch = index.get(o);
        perf_tests::do_not_optimize(batch);
        o = model::offset((o() + 2) % (2 * num_batches));
    }
    return lookups_per_iteration;
}

/*
 * span reads need gapless offsets: read() stops at the first hole in offset
 * coverage.
 */
struct batch_cache_span_bench {
    static constexpr int64_t num_batches = 100'000;
    static constexpr size_t batch_value_size = 100;
    static constexpr int64_t batches_per_read = 1000;

    storage::batch_cache cache;
    storage::batch_cache_index index;

    batch_cache_span_bench()
      : cache(make_opts())
      , index(cache) {
        for (int64_t i = 0; i < num_batches; ++i) {
            index.put(
              make_batch(model::offset(i), batch_value_size),
              storage::batch_cache::is_dirty_entry::no);
        }
    }

    ~batch_cache_span_bench() { cache.stop().get(); }
};

/*
 * a contiguous scan taking every batch: the fetch path shape.
 */
PERF_TEST_F(batch_cache_span_bench, read_span) {
    auto result = index.read(
      model::offset(0),
      model::offset(batches_per_read - 1),
      std::nullopt,
      std::nullopt,
      std::numeric_limits<size_t>::max(),
      true);
    perf_tests::do_not_optimize(result);
    return result.batches.size();
}

/*
 * a scan whose type filter rejects every batch: isolates the per-entry
 * metadata inspection cost from batch materialization.
 */
PERF_TEST_F(batch_cache_span_bench, read_span_filtered) {
    auto result = index.read(
      model::offset(0),
      model::offset(batches_per_read - 1),
      model::record_batch_type::raft_configuration,
      std::nullopt,
      std::numeric_limits<size_t>::max(),
      true);
    perf_tests::do_not_optimize(result);
    return batches_per_read;
}

/*
 * production-scale reclaim: a large, cold set of indexes whose ranges hold
 * scattered (non-adjacent) offsets, as happens when reads populate the cache
 * out of order. every reclaimed range touches a different deep btree with
 * cold caches, which is the regime behind reactor stalls observed on
 * memory-pressured fetch-heavy clusters.
 */
struct batch_cache_large_bench {
    static constexpr size_t reclaim_bytes = 4 << 20;
    static constexpr size_t batch_value_size = 100;
    static constexpr int64_t num_indexes = 8;
    static constexpr int64_t entries_per_index = 96'000;

    storage::batch_cache cache;
    std::vector<std::unique_ptr<storage::batch_cache_index>> indexes;
    std::vector<std::vector<int64_t>> shuffled_offsets;

    batch_cache_large_bench()
      : cache(make_opts()) {
        std::mt19937 rng(42);
        for (int64_t i = 0; i < num_indexes; ++i) {
            indexes.push_back(
              std::make_unique<storage::batch_cache_index>(cache));
            auto& offsets = shuffled_offsets.emplace_back(entries_per_index);
            std::iota(offsets.begin(), offsets.end(), 0);
            std::shuffle(offsets.begin(), offsets.end(), rng);
        }
        fill();
    }

    ~batch_cache_large_bench() {
        cache.clear();
        cache.stop().get();
    }

    void fill() {
        for (int64_t i = 0; i < entries_per_index; ++i) {
            for (int64_t j = 0; j < num_indexes; ++j) {
                indexes[j]->put(
                  make_batch(
                    model::offset(shuffled_offsets[j][i]), batch_value_size),
                  storage::batch_cache::is_dirty_entry::no);
            }
        }
    }
};

PERF_TEST_F(batch_cache_large_bench, reclaim_sync_cold) {
    if (cache.size_bytes() < reclaim_bytes) {
        // release deferred state so offsets can be re-inserted, then refill
        cache.clear();
        fill();
    }
    perf_tests::start_measuring_time();
    auto reclaimed = cache.reclaim(reclaim_bytes);
    perf_tests::stop_measuring_time();
    perf_tests::do_not_optimize(reclaimed);
}
