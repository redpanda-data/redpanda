// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "compression/async_stream_zstd.h"
#include "compression/internal/gzip_compressor.h"
#include "compression/internal/lz4_frame_compressor.h"
#include "compression/stream_zstd.h"
#include "random/generators.h"
#include "units.h"
#include "vassert.h"

#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include <seastar/testing/perf_tests.hh>

static inline iobuf gen(const size_t data_size) {
    const auto data = random_generators::gen_alphanum_string(512);
    iobuf ret;
    size_t i = data_size;
    while (i > 0) {
        const auto step = std::min<size_t>(i, data.size());
        ret.append(data.data(), step);
        i -= step;
    }
    vassert(
      ret.size_bytes() == data_size,
      "hmm... what happened: {}, we wanted {}",
      ret,
      data_size);
    return ret;
}

inline void compress_test(size_t data_size) {
    auto o = gen(data_size);
    compression::stream_zstd fn;
    compression::stream_zstd::init_workspace(2_MiB);

    perf_tests::start_measuring_time();
    perf_tests::do_not_optimize(fn.compress(std::move(o)));
    perf_tests::stop_measuring_time();
}

inline void uncompress_test(size_t data_size) {
    compression::stream_zstd fn;
    compression::stream_zstd::init_workspace(2_MiB);
    auto o = fn.compress(gen(data_size));

    perf_tests::start_measuring_time();
    perf_tests::do_not_optimize(fn.uncompress(std::move(o)));
    perf_tests::stop_measuring_time();
}

inline ss::future<> async_compress_test(size_t data_size) {
    auto o = gen(data_size);
    compression::async_stream_zstd fn(2_MiB, 1);

    perf_tests::start_measuring_time();
    perf_tests::do_not_optimize(co_await fn.compress(std::move(o)));
    perf_tests::stop_measuring_time();
}

inline ss::future<> async_uncompress_test(size_t data_size) {
    compression::async_stream_zstd fn(2_MiB, 1);
    auto o = co_await fn.compress(gen(data_size));

    perf_tests::start_measuring_time();
    perf_tests::do_not_optimize(co_await fn.uncompress(std::move(o)));
    perf_tests::stop_measuring_time();
}

PERF_TEST(streaming_zstd_1mb, compress) { compress_test(1 << 20); }
PERF_TEST(streaming_zstd_1mb, uncompress) { return uncompress_test(1 << 20); }
PERF_TEST(streaming_zstd_10mb, compress) { compress_test(10 << 20); }
PERF_TEST(streaming_zstd_10mb, uncompress) { return uncompress_test(10 << 20); }

template<typename CompressFunc>
inline void compress_fn_test(CompressFunc&& comp_fn, size_t data_size) {
    auto o = gen(data_size);
    perf_tests::start_measuring_time();
    perf_tests::do_not_optimize(comp_fn(o));
    perf_tests::stop_measuring_time();
}

template<typename CompressFunc, typename UncompressFunc>
inline void uncompress_fn_test(
  CompressFunc&& comp_fn, UncompressFunc&& uncomp_fn, size_t data_size) {
    auto o = comp_fn(gen(data_size));
    perf_tests::start_measuring_time();
    perf_tests::do_not_optimize(uncomp_fn(o));
    perf_tests::stop_measuring_time();
}

PERF_TEST(lz4_1mb, compress) {
    return compress_fn_test(
      compression::internal::lz4_frame_compressor::compress, 1 << 20);
}

PERF_TEST(lz4_10mb, compress) {
    return compress_fn_test(
      compression::internal::lz4_frame_compressor::compress, 10 << 20);
}

PERF_TEST(lz4_1mb, uncompress) {
    return uncompress_fn_test(
      compression::internal::lz4_frame_compressor::compress,
      compression::internal::lz4_frame_compressor::uncompress,
      1 << 20);
}

PERF_TEST(lz4_10mb, uncompress) {
    return uncompress_fn_test(
      compression::internal::lz4_frame_compressor::compress,
      compression::internal::lz4_frame_compressor::uncompress,
      10 << 20);
}

PERF_TEST(gzip_1mb, compress) {
    return compress_fn_test(
      compression::internal::gzip_compressor::compress, 1 << 20);
}

PERF_TEST(gzip_10mb, compress) {
    return compress_fn_test(
      compression::internal::gzip_compressor::compress, 10 << 20);
}

PERF_TEST(gzip_1mb, uncompress) {
    return uncompress_fn_test(
      compression::internal::gzip_compressor::compress,
      compression::internal::gzip_compressor::uncompress,
      1 << 20);
}

PERF_TEST(gzip_10mb, uncompress) {
    return uncompress_fn_test(
      compression::internal::gzip_compressor::compress,
      compression::internal::gzip_compressor::uncompress,
      10 << 20);
}

struct async_stream_zstd {};
PERF_TEST_C(async_stream_zstd, 1mb_compress) {
    co_await async_compress_test(1 << 20);
}
PERF_TEST_C(async_stream_zstd, 1mb_uncompress) {
    co_return co_await async_uncompress_test(1 << 20);
}
PERF_TEST_C(async_stream_zstd, 10mb_compress) {
    co_await async_compress_test(10 << 20);
}
PERF_TEST_C(async_stream_zstd, 10mb_uncompress) {
    co_return co_await async_uncompress_test(10 << 20);
}
