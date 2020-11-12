// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "compression/internal/gzip_compressor.h"
#include "compression/internal/lz4_frame_compressor.h"
#include "compression/internal/snappy_java_compressor.h"
#include "compression/internal/zstd_compressor.h"
#include "compression/snappy_standard_compressor.h"
#include "compression/stream_zstd.h"
#include "random/generators.h"
#include "units.h"
#include "vassert.h"

#include <seastar/testing/thread_test_case.hh>

static inline constexpr std::array<size_t, 12> sizes{{
  0,
  8,
  16,
  32,
  64,
  512,
  1_KiB,
  2_KiB,
  4_KiB,
  6_KiB,
  8_KiB,
  10_KiB,
}};

static inline iobuf gen(const size_t data_size) {
    const auto data = random_generators::gen_alphanum_string(512);
    iobuf ret;
    for (auto i = 0; i < data_size; i += 512) {
        ret.append(data.data(), data.size());
    }
    ret.trim_back(ret.size_bytes() - data_size);
    return ret;
}

template<typename CompressFunc, typename DecompressFunc>
inline void
roundtrip_compression(CompressFunc&& comp_fn, DecompressFunc&& decomp_fn) {
    for (size_t i : sizes) {
        iobuf buf = gen(i);
        auto cbuf = comp_fn(buf.share(0, i));
        auto dbuf = decomp_fn(cbuf);
        BOOST_CHECK_EQUAL(dbuf, buf);
    }
}

SEASTAR_THREAD_TEST_CASE(stream_zstd_test) {
    compression::stream_zstd fn;
    for (size_t i : sizes) {
        iobuf buf = gen(i);
        auto cbuf = fn.compress(buf.share(0, i));
        auto dbuf = fn.uncompress(std::move(cbuf));
        BOOST_CHECK_EQUAL(dbuf, buf);
    }
}

SEASTAR_THREAD_TEST_CASE(lz4_block_tests) {
    using fn = compression::internal::lz4_frame_compressor;
    roundtrip_compression(fn::compress, fn::uncompress);
}
SEASTAR_THREAD_TEST_CASE(snapy_java_test) {
    using fn = compression::internal::snappy_java_compressor;
    roundtrip_compression(fn::compress, fn::uncompress);
}
SEASTAR_THREAD_TEST_CASE(snapy_std_test) {
    using fn = compression::snappy_standard_compressor;
    roundtrip_compression(fn::compress, fn::uncompress);
}
SEASTAR_THREAD_TEST_CASE(zstd_forward_test) {
    using fn = compression::internal::zstd_compressor;
    roundtrip_compression(fn::compress, fn::uncompress);
}
SEASTAR_THREAD_TEST_CASE(gzip_test) {
    using fn = compression::internal::gzip_compressor;
    roundtrip_compression(fn::compress, fn::uncompress);
}
