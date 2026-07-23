// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/bytes.h"
#include "compaction/key.h"
#include "compaction/key_offset_map.h"
#include "compaction/tests/simple_reducer.h"
#include "container/chunked_circular_buffer.h"
#include "model/batch_compression.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/coroutine.hh>
#include <seastar/testing/perf_tests.hh>

#include <cstddef>

namespace {

const auto test_ntp = model::ntp(
  model::ns("kafka"), model::topic("tapioca"), model::partition_id(0));

// A semi-compressible payload: a short random block repeated to `size` so that
// (de)compression performs realistic work rather than operating on either
// trivially-compressible zeros or wholly-incompressible noise.
iobuf gen_value(size_t size) {
    const auto block = random_generators::gen_alphanum_string(64);
    iobuf buf;
    size_t remaining = size;
    while (remaining > 0) {
        const auto step = std::min(remaining, block.size());
        buf.append(block.data(), step);
        remaining -= step;
    }
    return buf;
}

// Exercises `compaction::filter` over a single batch. The `removal_frac`
// parameter controls the optimization under test: when it is 0.0 nothing is
// filtered out, so a compressed source batch is reused verbatim (the path added
// in 57e62ea1a7). Any value > 0.0 forces the batch to be rebuilt and
// re-compressed, serving as the control.
class compaction_filter_bench {
public:
    ss::future<size_t> bench(
      model::compression codec,
      int n_records,
      size_t value_size,
      double removal_frac) {
        // Build and (optionally) compress the input batch outside the timed
        // region; we only want to measure the filter pass.
        storage::record_batch_builder builder(
          model::record_batch_type::raft_data, model::offset(0));
        for (int i = 0; i < n_records; ++i) {
            builder.add_raw_kv(reflection::to_iobuf(i), gen_value(value_size));
        }
        auto batch = std::move(builder).build();
        const auto uncompressed_bytes = batch.size_bytes();
        if (codec != model::compression::none) {
            batch = model::compress_batch_sync(codec, std::move(batch));
        }

        // Index the keys of the records we want removed at an offset strictly
        // higher than any record in the batch, so the filter drops them. The
        // remaining keys are left unindexed and are therefore kept. With
        // `removal_frac == 0.0` the map is empty and the batch passes through
        // unchanged.
        compaction::simple_key_offset_map map{};
        const int n_removed = static_cast<int>(n_records * removal_frac);
        for (int i = 0; i < n_removed; ++i) {
            auto key = compaction::compaction_key{
              iobuf_to_bytes(reflection::to_iobuf(i))};
            co_await map.put(key, model::offset(n_records));
        }

        chunked_circular_buffer<model::record_batch> output;
        compaction::simple_sink sink(output);
        compaction::simple_map_filter filter(sink, map, test_ntp);

        perf_tests::start_measuring_time();
        co_await filter(std::move(batch));
        perf_tests::stop_measuring_time();

        perf_tests::do_not_optimize(output);
        co_return uncompressed_bytes;
    }
};

constexpr int default_records = 100;
constexpr size_t small_value = 512;
constexpr size_t large_value = 4096;
// Small values so the per-record rebuild work (not payload size) dominates,
// exposing the rebuild loop's record-vs-kept-set matching cost.
constexpr size_t tiny_value = 32;

} // namespace

// --- zstd ---------------------------------------------------------------

PERF_TEST_CN(compaction_filter_bench, zstd_unchanged) {
    return bench(model::compression::zstd, default_records, small_value, 0.0);
}

PERF_TEST_CN(compaction_filter_bench, zstd_half_removed) {
    return bench(model::compression::zstd, default_records, small_value, 0.5);
}

PERF_TEST_CN(compaction_filter_bench, zstd_unchanged_large) {
    return bench(model::compression::zstd, default_records, large_value, 0.0);
}

PERF_TEST_CN(compaction_filter_bench, zstd_half_removed_large) {
    return bench(model::compression::zstd, default_records, large_value, 0.5);
}

// --- lz4 ----------------------------------------------------------------

PERF_TEST_CN(compaction_filter_bench, lz4_unchanged) {
    return bench(model::compression::lz4, default_records, small_value, 0.0);
}

PERF_TEST_CN(compaction_filter_bench, lz4_half_removed) {
    return bench(model::compression::lz4, default_records, small_value, 0.5);
}

// --- uncompressed (floor; optimization is a no-op here) -----------------

PERF_TEST_CN(compaction_filter_bench, none_unchanged) {
    return bench(model::compression::none, default_records, small_value, 0.0);
}

PERF_TEST_CN(compaction_filter_bench, none_half_removed) {
    return bench(model::compression::none, default_records, small_value, 0.5);
}

// --- many records, rebuild path (exposes O(N) vs O(N^2) record matching) ----

PERF_TEST_CN(compaction_filter_bench, none_half_removed_2k) {
    return bench(model::compression::none, 2'000, tiny_value, 0.5);
}

PERF_TEST_CN(compaction_filter_bench, none_half_removed_8k) {
    return bench(model::compression::none, 8'000, tiny_value, 0.5);
}
