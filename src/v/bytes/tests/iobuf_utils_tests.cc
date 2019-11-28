#include "bytes/tests/utils.h"

#include <seastar/testing/thread_test_case.hh>

SEASTAR_THREAD_TEST_CASE(test_reading_zero_bytes_empty_stream) {
    auto buf = iobuf();
    auto is = make_iobuf_input_stream(std::move(buf));

    auto read_buf = read_iobuf_exactly(is, 0).get0();
    BOOST_REQUIRE_EQUAL(read_buf.size_bytes(), 0);
};

SEASTAR_THREAD_TEST_CASE(test_reading_zero_bytes) {
    auto buf = iobuf();
    append_sequence(buf, 5);
    auto is = make_iobuf_input_stream(std::move(buf));

    auto read_buf = read_iobuf_exactly(is, 0).get0();
    BOOST_REQUIRE_EQUAL(read_buf.size_bytes(), 0);
};

SEASTAR_THREAD_TEST_CASE(test_reading_some_bytes) {
    auto buf = iobuf();
    append_sequence(buf, 5);
    auto is = make_iobuf_input_stream(std::move(buf));

    auto read_buf = read_iobuf_exactly(is, 16).get0();
    BOOST_REQUIRE_EQUAL(read_buf.size_bytes(), 16);
};