#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "storage/log_reader.h"
#include "storage/log_segment.h"
#include "storage/log_segment_appender.h"
#include "storage/log_writer.h"
#include "storage/tests/random_batch.h"
#include "utils/file_sanitizer.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

using namespace storage; // NOLINT

struct context {
    log_segment_ptr log_seg;
    offset_tracker tracker;
    probe prb;

    void write(std::vector<model::record_batch>& batches) {
        auto fd
          = open_file_dma("test", open_flags::create | open_flags::rw).get0();
        fd = file(make_shared(file_io_sanitizer(std::move(fd))));
        auto appender = log_segment_appender(fd, file_output_stream_options());
        for (auto& b : batches) {
            storage::write(appender, b).get();
        }
        appender.flush().get();
        log_seg = log_segment(
          "test",
          std::move(fd),
          model::term_id(0),
          batches.begin()->base_offset(),
          128);
        log_seg->flush().get();
    }

    model::record_batch_reader reader(
      model::offset start,
      size_t max_bytes = std::numeric_limits<size_t>::max()) {
        auto cfg = log_reader_config{
          start, max_bytes, 0, default_priority_class()};
        return model::make_record_batch_reader<log_segment_reader>(
          log_seg, tracker, std::move(cfg), prb);
    }

    ~context() {
        if (log_seg) {
            log_seg->close().get();
        }
    }
};

class consumer {
public:
    future<stop_iteration> operator()(model::record_batch b) {
        _result.push_back(std::move(b));
        return make_ready_future<stop_iteration>(stop_iteration::no);
    }
    std::vector<model::record_batch> end_of_stream() {
        return std::move(_result);
    }

private:
    std::vector<model::record_batch> _result;
};

void check_batches(
  std::vector<model::record_batch>& actual,
  std::vector<model::record_batch>& expected) {
    BOOST_REQUIRE_EQUAL_COLLECTIONS(
      actual.begin(), actual.end(), expected.begin(), expected.end());
}

SEASTAR_THREAD_TEST_CASE(test_can_read_single_batch_smaller_offset) {
    context ctx;
    auto batches = test::make_random_batches(model::offset(1), 1);
    ctx.write(batches);
    ctx.tracker.update_committed_offset(model::offset(1000));
    {
        auto reader = ctx.reader(model::offset(0));
        auto res = reader.consume(consumer(), model::no_timeout).get0();
        check_batches(res, batches);
    }
}

SEASTAR_THREAD_TEST_CASE(test_can_read_single_batch_same_offset) {
    context ctx;
    auto batches = test::make_random_batches(model::offset(1), 1);
    ctx.write(batches);
    ctx.tracker.update_committed_offset(model::offset(1000));
    {
        auto reader = ctx.reader(model::offset(1));
        auto res = reader.consume(consumer(), model::no_timeout).get0();
        check_batches(res, batches);
    }
}

SEASTAR_THREAD_TEST_CASE(test_can_read_multiple_batches) {
    context ctx;
    auto batches = test::make_random_batches(model::offset(1));
    ctx.write(batches);
    ctx.tracker.update_committed_offset(model::offset(1000));
    {
        auto reader = ctx.reader(model::offset(0));
        auto res = reader.consume(consumer(), model::no_timeout).get0();
        check_batches(res, batches);
    }
}

SEASTAR_THREAD_TEST_CASE(test_does_not_read_past_committed_offset_one_segment) {
    context ctx;
    auto batches = test::make_random_batches(model::offset(2));
    ctx.write(batches);
    ctx.tracker.update_committed_offset(model::offset(1));
    {
        auto reader = ctx.reader(model::offset(0));
        auto res = reader.consume(consumer(), model::no_timeout).get0();
        BOOST_REQUIRE(res.empty());
    }
}

SEASTAR_THREAD_TEST_CASE(
  test_does_not_read_past_committed_offset_multiple_segments) {
    context ctx;
    auto batches = test::make_random_batches(model::offset(1), 2);
    ctx.write(batches);
    auto o = batches.begin()->last_offset();
    ctx.tracker.update_committed_offset(o);
    {
        auto reader = ctx.reader(model::offset(0));
        auto res = reader.consume(consumer(), model::no_timeout).get0();
        std::vector<model::record_batch> first;
        first.push_back(std::move(*batches.begin()));
        check_batches(res, first);
    }
}

SEASTAR_THREAD_TEST_CASE(test_does_not_read_past_max_bytes) {
    context ctx;
    auto batches = test::make_random_batches(model::offset(1), 2);
    ctx.write(batches);
    auto o = batches.begin()->last_offset();
    ctx.tracker.update_committed_offset(model::offset(1000));
    {
        auto reader = ctx.reader(
          model::offset(0), batches.begin()->size_bytes());
        auto res = reader.consume(consumer(), model::no_timeout).get0();
        std::vector<model::record_batch> first;
        first.push_back(std::move(*batches.begin()));
        check_batches(res, first);
    }
}

SEASTAR_THREAD_TEST_CASE(test_reads_at_least_one_batch) {
    context ctx;
    auto batches = test::make_random_batches(model::offset(1), 2);
    ctx.write(batches);
    auto o = batches.begin()->last_offset();
    ctx.tracker.update_committed_offset(model::offset(1000));
    {
        auto reader = ctx.reader(model::offset(0), 1);
        auto res = reader.consume(consumer(), model::no_timeout).get0();
        std::vector<model::record_batch> first;
        first.push_back(std::move(*batches.begin()));
        check_batches(res, first);
    }
}

SEASTAR_THREAD_TEST_CASE(test_seeks_to_first_relevant_batch) {
    context ctx;
    auto batches = test::make_random_batches(model::offset(0), 10);
    ctx.write(batches);
    ctx.tracker.update_committed_offset(model::offset(1000));
    for (auto& b : batches) {
        // seeks to batch with same base offset
        auto reader = ctx.reader(b.base_offset(), 1);
        auto res = reader.consume(consumer(), model::no_timeout).get0();
        BOOST_TEST(!res.empty());
        BOOST_TEST(res.front() == b);

        // seeks to batch with a middle-ish offset
        if (b.size() >= 3) {
            auto offset = model::offset(
              b.base_offset() + model::offset((b.size() / 2)));
            reader = ctx.reader(offset, 1);
            res = reader.consume(consumer(), model::no_timeout).get0();
            BOOST_TEST(!res.empty());
            BOOST_TEST(res.front() == b);
        }

        // seeks to batch with same last_offset
        reader = ctx.reader(b.last_offset(), 1);
        res = reader.consume(consumer(), model::no_timeout).get0();
        BOOST_TEST(!res.empty());
        BOOST_TEST(res.front() == b);
    }
}
