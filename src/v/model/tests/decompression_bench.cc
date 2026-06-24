/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

// This microbenchmark is a bit odd. It doesn't benchmark our code, but rather
// the compression codecs Kafka uses. Its main use is for fitting a model we
// have for CPU util when decompressing batches.
//
// The benchmark sweeps two axes per codec:
//   * target ratio        -> seed length (chunk_size / ratio); the achieved
//                            ratio is measured, not assumed.
//   * decompressed size   -> via records_per_batch (chunk size is fixed).
// so for each ratio we get a size sweep, which separates the fixed per-batch
// overhead from the marginal per-byte cost.
//
// Each benchmark emits one stable metadata line to stderr (the achieved ratio
// and byte counts seastar's timing output does not know about):
//   DECOMP_META,<case>,<codec>,<target_ratio>,<chunk_size>,<records_per_batch>,
//       <n_batches>,<decompressed_bytes_per_iter>,<compressed_bytes_per_iter>,
//       <measured_ratio>

#include "bytes/iobuf.h"
#include "container/chunked_vector.h"
#include "model/batch_compression.h"
#include "model/compression.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "random/generators.h"
#include "storage/record_batch_builder.h"

#include <seastar/testing/perf_tests.hh>

#include <algorithm>
#include <cstddef>
#include <optional>
#include <string>

namespace {

// Bytes per record value (one "record-sized chunk").
constexpr size_t chunk_size = 256;
// Batches consumed per timed iteration. More batches -> more decompress calls
// per measurement -> steadier timing, while batch size stays the swept knob.
constexpr size_t n_batches = 32;

struct decompressing_consumer {
    // Tally input/output bytes so we can derive the compression ratio.
    size_t total_compressed_bytes = 0;
    size_t total_decompressed_bytes = 0;

    ss::future<ss::stop_iteration> operator()(model::record_batch batch) {
        total_compressed_bytes += batch.size_bytes();
        if (batch.compressed()) {
            batch = co_await model::decompress_batch(batch);
        }
        total_decompressed_bytes += batch.size_bytes();
        co_return ss::stop_iteration::no;
    }
    ss::future<decompressing_consumer> end_of_stream() {
        co_return std::move(*this);
    }
};

struct case_data {
    chunked_vector<model::record_batch> compressed;
    size_t decompressed_bytes = 0;
    size_t compressed_bytes = 0;
    double measured_ratio = 0.0;
    bool meta_logged = false;
};

ss::logger dlog("decompression_bench");

} // namespace

class decompression_bench_fixture {
public:
    ss::future<> configure(
      model::compression codec, size_t target_ratio, size_t records_per_batch) {
        if (_case) {
            co_return;
        }
        auto& cd = _case.emplace();

        // Each record value is a short random seed tiled up to chunk_size, so
        // it compresses by ~target_ratio on its own.
        size_t seed_len = std::max<size_t>(1, chunk_size / target_ratio);

        for (size_t b = 0; b < n_batches; ++b) {
            storage::record_batch_builder builder(
              model::record_batch_type::raft_data, model::offset{0});
            for (size_t r = 0; r < records_per_batch; ++r) {
                auto seed = random_generators::gen_alphanum_string(seed_len);
                iobuf value;
                while (value.size_bytes() < chunk_size) {
                    size_t take = std::min(
                      seed_len, chunk_size - value.size_bytes());
                    value.append(seed.data(), take);
                }
                builder.add_raw_kv(std::nullopt, std::move(value));
            }
            auto uncompressed = std::move(builder).build();
            cd.decompressed_bytes += uncompressed.size_bytes();
            auto compressed = co_await model::compress_batch(
              codec, std::move(uncompressed));
            cd.compressed_bytes += compressed.size_bytes();
            cd.compressed.push_back(std::move(compressed));
        }
        cd.measured_ratio = static_cast<double>(cd.decompressed_bytes)
                            / static_cast<double>(cd.compressed_bytes);
    }

    ss::future<size_t> run_bench(
      std::string case_name,
      std::string codec,
      size_t target_ratio,
      size_t records_per_batch) {
        auto& cd = *_case;

        // Emit the stable metadata row once per case.
        if (!cd.meta_logged) {
            cd.meta_logged = true;
            vlog(
              dlog.info,
              "DECOMP_META,{},{},{},{},{},{},{},{},{:.6f}",
              case_name,
              codec,
              target_ratio,
              chunk_size,
              records_per_batch,
              n_batches,
              cd.decompressed_bytes,
              cd.compressed_bytes,
              cd.measured_ratio);
        }

        chunked_vector<model::record_batch> batches;
        batches.reserve(cd.compressed.size());
        for (auto& b : cd.compressed) {
            batches.push_back(b.share());
        }
        auto reader = model::make_chunked_memory_record_batch_reader(
          std::move(batches));

        perf_tests::start_measuring_time();
        auto res = co_await reader.consume(
          decompressing_consumer{}, model::no_timeout);
        perf_tests::stop_measuring_time();

        perf_tests::do_not_optimize(res.total_decompressed_bytes);
        co_return res.total_decompressed_bytes;
    }

private:
    std::optional<case_data> _case;
};

// One PERF_TEST per (codec, target_ratio, records_per_batch) grid point. The
// case name encodes the params and is the join key for the DECOMP_META line.
#define DECOMP_CASE(codec, ratio, rpb)                                         \
    PERF_TEST_CN(decompression_bench_fixture, codec##_r##ratio##_n##rpb) {     \
        co_await configure(model::compression::codec, ratio, rpb);             \
        co_return co_await run_bench(                                          \
          #codec "_r" #ratio "_n" #rpb, #codec, ratio, rpb);                   \
    }

// Two grids, both spanning ratio 1 (incompressible) .. 16 (highly redundant)
// and batch size n * 256 B = 4 KiB .. 256 KiB.
//   MINIMAL: just the min/max corner of each dimension -- a fast smoke check
//            that the bench builds and runs (the default).
//   FULL:    dense, equal coverage per codec for actually fitting the 4
//            coefficients. Switch to it (DECOMP_UPDATE_COEFFICIENTS=1) only
//            when refreshing the model.
// clang-format off
#define GRID_MINIMAL(codec, X)                                                 \
    X(codec,  1,   16) X(codec,  1, 1024)                                      \
    X(codec, 16,   16) X(codec, 16, 1024)

#define GRID_FULL(codec, X)                                                     \
    X(codec,  1,   16) X(codec,  1,   64) X(codec,  1,  256) X(codec,  1, 1024) \
    X(codec,  4,   16) X(codec,  4,   64) X(codec,  4,  256) X(codec,  4, 1024) \
    X(codec,  8,   16) X(codec,  8,   64) X(codec,  8,  256) X(codec,  8, 1024) \
    X(codec, 16,   16) X(codec, 16,   64) X(codec, 16,  256) X(codec, 16, 1024)
// clang-format on

#define DECOMP_UPDATE_COEFFICIENTS 0
#if DECOMP_UPDATE_COEFFICIENTS
#define GRID_FOR(codec, X) GRID_FULL(codec, X)
#else
#define GRID_FOR(codec, X) GRID_MINIMAL(codec, X)
#endif

#define DECOMP_GRID(X)                                                         \
    GRID_FOR(zstd, X) GRID_FOR(lz4, X) GRID_FOR(snappy, X) GRID_FOR(gzip, X)

DECOMP_GRID(DECOMP_CASE)
