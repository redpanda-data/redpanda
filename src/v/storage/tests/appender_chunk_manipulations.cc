#include "random/generators.h"
#include "storage/log_segment_appender.h"

#include <seastar/testing/thread_test_case.hh>
#include <boost/test/tools/old/interface.hpp>

#include <cstring>

using chunk = storage::log_segment_appender::chunk;
static constexpr size_t alignment = 4096;

SEASTAR_THREAD_TEST_CASE(chunk_manipulation) {
    const auto b = random_generators::gen_alphanum_string(1024 * 1024);
    chunk c(alignment);
    {
        c.append(b.data(), c.space_left());
        BOOST_REQUIRE(c.is_full());
        BOOST_REQUIRE_EQUAL(
          c.size(), storage::log_segment_appender::chunk_size);
        c.reset();
    }
    {
        c.append(b.data(), alignment - 2);
        c.append(b.data(), 4);
        BOOST_REQUIRE_EQUAL(c.dma_size(alignment), 8192);
        c.reset();
    }
    {
        size_t i = (alignment * 3) + 10;
        c.append(b.data(), i);
        BOOST_REQUIRE_EQUAL(c.dma_size(alignment), alignment * 4);
        const char* dptr = c.dma_ptr(alignment);
        const char* eptr = b.data();
        BOOST_REQUIRE(std::memcmp(dptr, eptr, i) == 0);
        c.flush();
        // same after flush
        BOOST_REQUIRE_EQUAL(c.dma_size(alignment), alignment);
        BOOST_REQUIRE_EQUAL(c.flushed_pos() % alignment, 10);
        c.append(b.data() + i, alignment + 10);
        i += alignment + 10;
        // we appended 4096+10, but had 10 left from prev
        BOOST_REQUIRE_EQUAL(c.dma_size(alignment), alignment * 2);
        c.flush();
        c.compact(alignment);
        BOOST_REQUIRE_EQUAL(c.size(), 20);
        BOOST_REQUIRE_EQUAL(c.flushed_pos(), 20);
        dptr = c.dma_ptr(alignment);
        eptr = b.data() + i - 20;
        BOOST_REQUIRE(std::memcmp(dptr, eptr, 20) == 0);
        c.reset();
    }
}
