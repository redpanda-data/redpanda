#include "random/generators.h"
#include "storage/segment_appender.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_log.hpp>

#include <cstring>

using chunk = storage::segment_appender::chunk;
static constexpr size_t alignment = 4096;

SEASTAR_THREAD_TEST_CASE(chunk_manipulation) {
    const auto b = random_generators::gen_alphanum_string(1024 * 1024);
    chunk c(alignment);
    {
        c.append(b.data(), c.space_left());
        BOOST_REQUIRE(c.is_full());
        BOOST_REQUIRE_EQUAL(c.size(), storage::segment_appender::chunk_size);
        c.reset();
    }
    {
        c.append(b.data(), alignment - 2);
        c.append(b.data(), 4);
        BOOST_REQUIRE_EQUAL(c.dma_size(), 8192);
        c.reset();
    }
    {
        size_t i = (alignment * 3) + 10;
        c.append(b.data(), i);
        BOOST_REQUIRE_EQUAL(c.dma_size(), alignment * 4);
        const char* dptr = c.dma_ptr();
        const char* eptr = b.data();
        BOOST_REQUIRE(std::memcmp(dptr, eptr, i) == 0);
        c.flush();
        // same after flush
        BOOST_REQUIRE_EQUAL(c.dma_size(), alignment);
        // 10 bytes on the next page
        BOOST_REQUIRE_EQUAL(c.flushed_pos() % alignment, 10);
        BOOST_TEST_MESSAGE("10 bytes spill over: " << c);
        c.append(b.data() + i, alignment + 10);
        static_assert(chunk::chunk_size == 16_KiB);
        BOOST_TEST_MESSAGE("Should be full: " << c.is_full() << ", " << c);
        BOOST_REQUIRE(c.is_full());
        // we flushed after 3 pages. so the dma_size() should be 1 page left
        BOOST_REQUIRE_EQUAL(c.dma_size(), alignment);
        c.reset();
        BOOST_REQUIRE_EQUAL(c.dma_size(), 0);
    }
}
