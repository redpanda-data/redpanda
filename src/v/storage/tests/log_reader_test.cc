#include "model/fundamental.h"
#include "model/limits.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "storage/log_reader.h"
#include "storage/log_segment_appender.h"
#include "storage/log_segment_reader.h"
#include "storage/log_writer.h"
#include "storage/tests/random_batch.h"
#include "utils/file_sanitizer.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

#include <limits>

using namespace storage; // NOLINT

struct context {
    log_set logs = log_set({});
    offset_tracker tracker;
    probe prb;
    void write(std::vector<model::record_batch>& batches) {
        unsigned id = 0;
        for (auto& batch : batches) {
            auto fd = ss::open_file_dma(
                        "test" + ss::to_sstring(id++),
                        ss::open_flags::create | ss::open_flags::rw)
                        .get0();
            fd = ss::file(ss::make_shared(file_io_sanitizer(std::move(fd))));
            auto appender = log_segment_appender(
              fd, log_segment_appender::options(ss::default_priority_class()));
            storage::write(appender, batch).get();
            appender.flush().get();
            auto log_seg = ss::make_lw_shared<log_segment_reader>(
              "test",
              std::move(fd),
              model::term_id(0),
              batches.begin()->base_offset(),
              appender.file_byte_offset(),
              128);
            log_seg->set_last_written_offset(batch.last_offset());
            logs.add(log_seg);
        }
    }

    model::record_batch_reader reader(
      model::offset start,
      size_t max_bytes = std::numeric_limits<size_t>::max(),
      model::offset max_offset = model::model_limits<model::offset>::max()) {
        auto cfg = log_reader_config{
          start, max_bytes, 0, ss::default_priority_class(), {}, max_offset};
        return model::make_record_batch_reader<log_reader>(
          logs, tracker, std::move(cfg), prb);
    }

    ~context() {
        for (auto log_seg : logs) {
            log_seg->close().get();
        }
    }
};

class consumer {
public:
    ss::future<ss::stop_iteration> operator()(model::record_batch b) {
        _result.push_back(std::move(b));
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::no);
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

SEASTAR_THREAD_TEST_CASE(test_can_read_batches_smaller_offset) {
    context ctx;
    auto batches = test::make_random_batches(model::offset(1), 3);
    ctx.write(batches);
    ctx.tracker.update_committed_offset(model::offset(1000));
    {
        auto reader = ctx.reader(model::offset(0));
        auto res = reader.consume(consumer(), model::no_timeout).get0();
        check_batches(res, batches);
    }
}

SEASTAR_THREAD_TEST_CASE(test_can_read_batches_same_offset) {
    context ctx;
    auto batches = test::make_random_batches(model::offset(1), 3);
    ctx.write(batches);
    ctx.tracker.update_committed_offset(model::offset(1000));
    {
        auto reader = ctx.reader(model::offset(1));
        auto res = reader.consume(consumer(), model::no_timeout).get0();
        check_batches(res, batches);
    }
}

SEASTAR_THREAD_TEST_CASE(test_does_not_read_past_committed_offset_one_segment) {
    context ctx;
    auto batches = test::make_random_batches(model::offset(2), 1);
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
    auto batches = test::make_random_batches(model::offset(1), 3);
    ctx.write(batches);
    auto o = std::next(batches.begin())->last_offset();
    ctx.tracker.update_committed_offset(o);
    {
        auto reader = ctx.reader(model::offset(0));
        auto res = reader.consume(consumer(), model::no_timeout).get0();
        std::vector<model::record_batch> first;
        first.push_back(std::move(*batches.begin()));
        first.push_back(std::move(*std::next(batches.begin())));
        check_batches(res, first);
    }
}

SEASTAR_THREAD_TEST_CASE(test_does_not_read_past_max_bytes) {
    context ctx;
    auto batches = test::make_random_batches(model::offset(1), 3);
    ctx.write(batches);
    auto o = batches.begin()->last_offset();
    ctx.tracker.update_committed_offset(model::offset(1000));
    {
        auto reader = ctx.reader(
          model::offset(0),
          batches.begin()->size_bytes()
            + std::next(batches.begin())->size_bytes());
        auto res = reader.consume(consumer(), model::no_timeout).get0();
        std::vector<model::record_batch> first;
        first.push_back(std::move(*batches.begin()));
        first.push_back(std::move(*std::next(batches.begin())));
        check_batches(res, first);
    }
}

SEASTAR_THREAD_TEST_CASE(test_does_not_read_past_max_offset_multiple_batches) {
    context ctx;

    // 3 batch, last offset = 299, in chunks of 100 batches each
    std::vector<model::record_batch> batches;
    batches.push_back(test::make_random_batch(model::offset(0), 100));
    batches.push_back(test::make_random_batch(model::offset(100), 100));
    batches.push_back(test::make_random_batch(model::offset(200), 100));
    ctx.write(batches);
    ctx.tracker.update_committed_offset(model::offset(299));

    // max reader offset is 200
    auto maxbytes = std::numeric_limits<size_t>::max();
    auto reader = ctx.reader(model::offset(0), maxbytes, model::offset(199));

    std::vector<model::record_batch> consumed
      = reader.consume(consumer(), model::no_timeout).get0();

    // max offset set to 199, so the last batch should not be returned.
    std::vector<model::record_batch> expected;
    expected.push_back(std::move(batches[0]));
    expected.push_back(std::move(batches[1]));

    check_batches(consumed, expected);
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
