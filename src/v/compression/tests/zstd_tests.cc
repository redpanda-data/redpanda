// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/units.h"
#include "base/vassert.h"
#include "compression/async_stream_zstd.h"
#include "compression/internal/gzip_compressor.h"
#include "compression/internal/lz4_frame_compressor.h"
#include "compression/internal/snappy_java_compressor.h"
#include "compression/internal/zstd_compressor.h"
#include "compression/snappy_standard_compressor.h"
#include "compression/stream_zstd.h"
#include "random/generators.h"

#include <seastar/testing/thread_test_case.hh>

static inline constexpr std::array<size_t, 16> sizes{{
  0,
  1,
  2,
  3,
  8,
  9,
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
static constexpr size_t default_decompression_size = 2_MiB;

static std::vector<size_t> get_test_sizes() {
    std::vector<size_t> test_sizes;
    for (auto size : sizes) {
        test_sizes.push_back(size);
    }
    // add in some extras sizes from iobuf allocator
    for (auto size : details::io_allocation_size::alloc_table) {
        test_sizes.push_back(size);
    }
    test_sizes.push_back(details::io_allocation_size::alloc_table.back() * 2);
    test_sizes.push_back(
      details::io_allocation_size::alloc_table.back() * 2 - 1);
    test_sizes.push_back(
      details::io_allocation_size::alloc_table.back() * 2 + 1);
    test_sizes.push_back(details::io_allocation_size::alloc_table.back() * 3);
    test_sizes.push_back(
      details::io_allocation_size::alloc_table.back() * 3 + 1);
    test_sizes.push_back(
      details::io_allocation_size::alloc_table.back() * 3 - 1);
    return test_sizes;
}

static inline iobuf gen(const size_t data_size) {
    iobuf ret;
    for (size_t i = 0; i < data_size; i += 512) {
        const auto data = random_generators::gen_alphanum_string(512);
        ret.append(data.data(), data.size());
    }
    ret.trim_back(ret.size_bytes() - data_size);
    return ret;
}

template<typename CompressFunc, typename DecompressFunc>
inline void
roundtrip_compression(CompressFunc&& comp_fn, DecompressFunc&& decomp_fn) {
    auto test_sizes = get_test_sizes();
    for (size_t i : test_sizes) {
        iobuf buf = gen(i);
        auto cbuf = comp_fn(buf.share(0, i));
        auto dbuf = decomp_fn(cbuf);
        BOOST_CHECK_EQUAL(dbuf, buf);
    }
}

SEASTAR_THREAD_TEST_CASE(stream_zstd_test) {
    compression::stream_zstd fn;
    auto test_sizes = get_test_sizes();
    for (size_t i : sizes) {
        iobuf buf = gen(i);
        auto cbuf = fn.compress(buf.share(0, i));
        auto dbuf = fn.uncompress(std::move(cbuf));
        BOOST_CHECK_EQUAL(dbuf, buf);
    }
}

SEASTAR_THREAD_TEST_CASE(async_stream_zstd_test) {
    compression::async_stream_zstd fn(default_decompression_size, 1);
    auto test_sizes = get_test_sizes();
    for (size_t i : sizes) {
        iobuf buf = gen(i);

        auto cbuf = fn.compress(buf.share(0, i)).get();
        auto dbuf = fn.uncompress(std::move(cbuf)).get();

        BOOST_CHECK_EQUAL(dbuf, buf);
    }
}

SEASTAR_THREAD_TEST_CASE(interleaved_async_stream_zstd_test) {
    std::array<size_t, 5> test_sizes{
      50_MiB,
      50_MiB,
      50_MiB,
      50_MiB,
      50_MiB,
    };

    std::vector<iobuf> gen_data;
    for (size_t i : test_sizes) {
        gen_data.push_back(gen(i));
    }

    auto fut = ss::parallel_for_each(
      gen_data.begin(), gen_data.end(), [](iobuf& buf) {
          return ss::do_with(
            std::move(buf),
            compression::async_stream_zstd(default_decompression_size, 3),
            [](auto& buf, auto& fn) {
                return fn.compress(buf.share(0, buf.size_bytes()))
                  .then(
                    [&](iobuf cbuf) { return fn.uncompress(std::move(cbuf)); })
                  .then([&](iobuf dbuf) { BOOST_CHECK_EQUAL(dbuf, buf); });
            });
      });

    fut.get();
}

SEASTAR_THREAD_TEST_CASE(async_stream_to_stream_test) {
    compression::async_stream_zstd fn(default_decompression_size, 1);
    compression::stream_zstd fn_s;
    auto test_sizes = get_test_sizes();
    for (size_t i : sizes) {
        iobuf buf = gen(i);

        auto cbuf = fn.compress(buf.share(0, i)).get();
        auto dbuf = fn_s.uncompress(std::move(cbuf));

        BOOST_CHECK_EQUAL(dbuf, buf);
    }
}

SEASTAR_THREAD_TEST_CASE(stream_to_async_stream_test) {
    compression::async_stream_zstd fn(default_decompression_size, 1);
    compression::stream_zstd fn_s;
    auto test_sizes = get_test_sizes();
    for (size_t i : sizes) {
        iobuf buf = gen(i);

        auto cbuf = fn_s.compress(buf.share(0, i));
        auto dbuf = fn.uncompress(std::move(cbuf)).get();

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
