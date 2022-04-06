// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "compression/stream_zstd.h"
#include "random/generators.h"
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
    perf_tests::start_measuring_time();
    perf_tests::do_not_optimize(fn.compress(std::move(o)));
    perf_tests::stop_measuring_time();
}

inline void uncompress_test(size_t data_size) {
    compression::stream_zstd fn;
    auto o = fn.compress(gen(data_size));
    perf_tests::start_measuring_time();
    perf_tests::do_not_optimize(fn.uncompress(std::move(o)));
    perf_tests::stop_measuring_time();
}

PERF_TEST(streaming_zstd_1mb, compress) { compress_test(1 << 20); }
PERF_TEST(streaming_zstd_1mb, uncompress) { return uncompress_test(1 << 20); }
PERF_TEST(streaming_zstd_10mb, compress) { compress_test(10 << 20); }
PERF_TEST(streaming_zstd_10mb, uncompress) { return uncompress_test(10 << 20); }
