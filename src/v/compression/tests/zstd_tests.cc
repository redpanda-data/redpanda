#include "compression/internal/lz4_frame_compressor.h"
#include "compression/internal/snappy_compressor.h"
#include "compression/stream_zstd.h"
#include "random/generators.h"
#include "units.h"
#include "vassert.h"

#include <seastar/testing/thread_test_case.hh>

static inline iobuf gen(const size_t data_size) {
    const auto data = random_generators::gen_alphanum_string(512);
    iobuf ret;
    for (auto i = 0; i < data_size; i += 512) {
        ret.append(data.data(), data.size());
    }
    ret.trim_back(ret.size_bytes() - data_size);
    return ret;
}

SEASTAR_THREAD_TEST_CASE(stream_zstd_test) {
    compression::stream_zstd fn;
    for (size_t i = 1024; i < 1024 + 100; ++i) {
        iobuf buf = gen(i);
        auto cbuf = fn.compress(buf.share(0, i));
        auto dbuf = fn.uncompress(std::move(cbuf));
        BOOST_CHECK_EQUAL(dbuf, buf);
    }
}

SEASTAR_THREAD_TEST_CASE(lz4_block_tests) {
    std::cout.setf(std::ios::unitbuf);
    using fn = compression::internal::lz4_frame_compressor;
    for (size_t i = 1_KiB; i < 10_KiB; i += 1_KiB) {
        iobuf buf = gen(i);
        auto cbuf = fn::compress(buf.share(0, i));
        auto dbuf = fn::uncompress(cbuf);
        BOOST_CHECK_EQUAL(dbuf, buf);
    }
}
SEASTAR_THREAD_TEST_CASE(snapy_test) {
    using fn = compression::internal::snappy_compressor;
    for (size_t i = 1024; i < 1024 + 100; ++i) {
        iobuf buf = gen(i);
        auto cbuf = fn::compress(buf.share(0, i));
        auto dbuf = fn::uncompress(cbuf);
        BOOST_CHECK_EQUAL(dbuf, buf);
    }
}
