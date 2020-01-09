#include "storage/log_segment_reader.h"

#include <seastar/core/simple-stream.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

using namespace storage; // NOLINT

SEASTAR_THREAD_TEST_CASE(test_read_write) {
    uint64_t value = 0x1234'5678'abcd'ef02ull;
    auto data = bytes(bytes::initialized_later(), sizeof(value));

    auto appender = log_segment_appender(
      open_file_dma(
        "test", open_flags::create | open_flags::rw | open_flags::truncate)
        .get0(),
      log_segment_appender::options(seastar::default_priority_class()));

    auto log_seg = log_segment_reader(
      "test",
      open_file_dma("test", open_flags::ro).get0(),
      model::term_id(0),
      model::offset(0),
      0,
      1024);

    appender.append(data).get();
    appender.flush().get();
    log_seg.set_last_visible_byte_offset(appender.file_byte_offset());

    auto in = log_seg.data_stream(0, default_priority_class());
    auto buf = in.read_exactly(sizeof(value)).get0();
    auto bv = bytes_view(
      reinterpret_cast<const int8_t*>(buf.get()), buf.size());
    BOOST_CHECK_EQUAL(bv, data);

    BOOST_CHECK_EQUAL(appender.file_byte_offset(), data.size());

    appender.close().get();
    in.close().get();
    log_seg.close().get();

    auto bytes_file = open_file_dma("test", open_flags::ro).get0();
    auto stat_size = bytes_file.stat().get0().st_size;
    BOOST_CHECK_EQUAL(stat_size, data.size());
    bytes_file.close().get();
}

SEASTAR_THREAD_TEST_CASE(log_set_orders_segments) {
    file f(nullptr);
    auto log_seg1 = make_lw_shared<log_segment_reader>(
      "test", f, model::term_id(0), model::offset(1), 0, 1024);
    auto log_seg0 = make_lw_shared<log_segment_reader>(
      "test", f, model::term_id(0), model::offset(0), 0, 1024);
    auto log_seg3 = make_lw_shared<log_segment_reader>(
      "test", f, model::term_id(0), model::offset(2), 0, 1024);

    log_set segs({log_seg1, log_seg0, log_seg3});

    auto o = model::offset(0);
    for (auto seg : segs) {
        BOOST_CHECK_EQUAL(seg->base_offset(), o);
        o += 1;
    }
}

SEASTAR_THREAD_TEST_CASE(log_set_expects_monotonic_adds) {
    file f(nullptr);
    auto log_seg1 = make_lw_shared<log_segment_reader>(
      "test", f, model::term_id(0), model::offset(1), 0, 1024);
    auto log_seg0 = make_lw_shared<log_segment_reader>(
      "test", f, model::term_id(0), model::offset(0), 0, 1024);

    log_set segs({log_seg1});
    segs.add(log_seg0);

    BOOST_CHECK_EQUAL(segs.last()->base_offset(), model::offset(0));
}

SEASTAR_THREAD_TEST_CASE(log_set_invalidates_iterators) {
    file f(nullptr);
    auto log_seg = make_lw_shared<log_segment_reader>(
      "test", f, model::term_id(0), model::offset(1), 0, 1024);
    auto other_log_seg = make_lw_shared<log_segment_reader>(
      "test", f, model::term_id(0), model::offset(0), 0, 1024);

    log_set segs({log_seg});
    auto gen = segs.iter_gen();
    segs.add(other_log_seg);

    BOOST_CHECK_NE(gen, segs.iter_gen());
}

SEASTAR_THREAD_TEST_CASE(test_log_seg_selector) {
    file f(nullptr);

    auto log_seg1 = make_lw_shared<log_segment_reader>(
      "test", f, model::term_id(0), model::offset(0), 0, 1024);
    log_seg1->set_last_written_offset(model::offset(10));

    auto log_seg2 = make_lw_shared<log_segment_reader>(
      "test", f, model::term_id(0), model::offset(11), 0, 1024);
    log_seg2->set_last_written_offset(model::offset(20));

    auto log_seg3 = make_lw_shared<log_segment_reader>(
      "test", f, model::term_id(0), model::offset(21), 0, 1024);
    log_seg3->set_last_written_offset(model::offset(21));

    log_set segs({log_seg1, log_seg2, log_seg3});

    auto select = log_segment_selector(segs);

    auto seg = select.select(model::offset(0));
    BOOST_CHECK_EQUAL(seg, log_seg1);

    seg = select.select(model::offset(10));
    BOOST_CHECK_EQUAL(seg, log_seg1);

    seg = select.select(model::offset(11));
    BOOST_CHECK_EQUAL(seg, log_seg2);

    seg = select.select(model::offset(15));
    BOOST_CHECK_EQUAL(seg, log_seg2);

    seg = select.select(model::offset(20));
    BOOST_CHECK_EQUAL(seg, log_seg2);

    seg = select.select(model::offset(21));
    BOOST_CHECK_EQUAL(seg, log_seg3);

    seg = select.select(model::offset(22));
    BOOST_CHECK_EQUAL(seg, segment_reader_ptr());

    auto log_seg4 = make_lw_shared<log_segment_reader>(
      "test", f, model::term_id(0), model::offset(22), 0, 1024);
    log_seg4->set_last_written_offset(model::offset(25));
    segs.add(log_seg4);
    seg = select.select(model::offset(22));
    BOOST_CHECK_EQUAL(seg, log_seg4);

    segs = log_set({log_seg1, log_seg2, log_seg3, log_seg4});
    seg = select.select(model::offset(12));
    BOOST_CHECK_EQUAL(seg, log_seg2);
}
