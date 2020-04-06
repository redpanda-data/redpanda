#include "compression/stream_zstd.h"
#include "random/generators.h"
#include "vassert.h"

#include <seastar/testing/thread_test_case.hh>

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

SEASTAR_THREAD_TEST_CASE(test_appended_data_is_retained) {
    compression::stream_zstd fn;
    for (size_t i = 1024; i < 1024 + 100; ++i) {
        iobuf buf = gen(i);
        auto cbuf = fn.compress(buf.share(0, i));
        auto dbuf = fn.uncompress(std::move(cbuf));
        BOOST_CHECK_EQUAL(dbuf, buf);
    }
}
