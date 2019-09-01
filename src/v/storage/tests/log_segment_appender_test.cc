#include "storage/log_segment_appender.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

using namespace storage;

SEASTAR_THREAD_TEST_CASE(test_can_append_ftb) {
    uint64_t value = 0x1234'5678'abcd'ef02ull;
    auto data = bytes(bytes::initialized_later(), sizeof(value));

    std::copy_n(
      reinterpret_cast<const int8_t*>(&value), sizeof(value), data.begin());
    std::vector<temporary_buffer<char>> fragments;
    fragments.emplace_back(reinterpret_cast<char*>(data.data()), 3);
    fragments.emplace_back(reinterpret_cast<char*>(data.data() + 3), 2);
    fragments.emplace_back(reinterpret_cast<char*>(data.data() + 5), 3);
    auto ftb = fragmented_temporary_buffer(std::move(fragments), sizeof(value));

    auto f = open_file_dma("test", open_flags::create | open_flags::rw).get0();
    auto appender = log_segment_appender(f, file_output_stream_options());
    appender.append(ftb).get();
    appender.flush().get();

    auto in = make_file_input_stream(f);
    auto buf = in.read_exactly(sizeof(value)).get0();
    auto bv = bytes_view(
      reinterpret_cast<const int8_t*>(buf.get()), buf.size());
    BOOST_CHECK_EQUAL(bv, data);

    in.close().get();
    appender.close().get();
}
