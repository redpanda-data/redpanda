#include "storage/log_segment.h"

#include <seastar/core/simple-stream.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

using namespace storage;

SEASTAR_THREAD_TEST_CASE(test_read_write) {
    uint64_t value = 0x1234'5678'abcd'ef02ull;
    auto data = bytes(bytes::initialized_later(), sizeof(value));

    auto f = open_file_dma("test", open_flags::create | open_flags::rw).get0();

    auto log_seg = log_segment("test", f, 0, model::offset(0), 1024);

    auto appender = log_seg.data_appender(default_priority_class());
    appender.append(data).get();
    appender.flush().get();

    auto in = log_seg.data_stream(0, default_priority_class());
    auto buf = in.read_exactly(sizeof(value)).get0();
    auto bv = bytes_view(
      reinterpret_cast<const int8_t*>(buf.get()), buf.size());
    BOOST_CHECK_EQUAL(bv, data);

    auto stat = log_seg.stat().get0();
    BOOST_CHECK_EQUAL(stat.st_size, data.size());

    in.close().get();
    log_seg.close().get();
}
