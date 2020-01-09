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

using namespace storage; // NOLINT

struct context {
    segment_reader_ptr log_seg;
    offset_tracker tracker;
    probe prb;

    void write(std::vector<model::record_batch>& batches) {
        auto fd = ss::open_file_dma(
                    "test", ss::open_flags::create | ss::open_flags::rw)
                    .get0();
        fd = ss::file(ss::make_shared(file_io_sanitizer(std::move(fd))));
        auto appender = log_segment_appender(
          fd, log_segment_appender::options(ss::default_priority_class()));
        for (auto& b : batches) {
            storage::write(appender, b).get();
        }
        appender.flush().get();
        log_seg = ss::make_lw_shared<log_segment_reader>(
          "test",
          std::move(fd),
          model::term_id(0),
          batches.begin()->base_offset(),
          appender.file_byte_offset(),
          128);
    }

    model::record_batch_reader reader(
      model::offset start,
      size_t max_bytes = std::numeric_limits<size_t>::max()) {
        auto cfg = log_reader_config{
          start, max_bytes, 0, ss::default_priority_class()};
        return model::make_record_batch_reader<log_segment_batch_reader>(
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

SEASTAR_THREAD_TEST_CASE(test_read_batch_range) {
    context ctx;
    auto batches = test::make_random_batches(model::offset(0), 10);
    ctx.write(batches);
    ctx.tracker.update_committed_offset(model::offset(1000));
    {
        // read batches from 3 to 7
        auto start_offset = batches[2].base_offset();
        auto max_offset = batches[6].last_offset();

        log_reader_config cfg{
          .start_offset = start_offset,
          .max_bytes = std::numeric_limits<size_t>::max(),
          .min_bytes = 0,
          .prio = ss::default_priority_class(),
          .type_filter = {},
          .max_offset = max_offset,
        };

        auto reader = model::make_record_batch_reader<log_segment_batch_reader>(
          ctx.log_seg, ctx.tracker, std::move(cfg), ctx.prb);

        auto res = reader.consume(consumer(), model::no_timeout).get0();
        BOOST_REQUIRE_EQUAL_COLLECTIONS(
          res.begin(),
          res.end(),
          std::next(batches.begin(), 2),
          std::next(batches.begin(), 7));
    }
};

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

SEASTAR_THREAD_TEST_CASE(test_batch_type_filter) {
    // write some batches with various types
    auto batches = test::make_random_batches(model::offset(0), 5);
    for (auto i = 0u; i < batches.size(); i++) {
        batches[i].get_header_for_testing().type = model::record_batch_type(i);
    }

    std::shuffle(
      batches.begin(), batches.end(), random_generators::internal::gen);

    context ctx;
    ctx.write(batches);
    ctx.tracker.update_committed_offset(model::offset(1000));

    // read and extract types with optional type filter
    auto read_types = [&ctx](std::vector<int> types_wanted) {
        std::vector<model::record_batch_type> type_filter;
        for (auto type : types_wanted) {
            type_filter.push_back(model::record_batch_type(type));
        }

        auto config = log_reader_config{
          .start_offset = model::offset(0),
          .max_bytes = std::numeric_limits<size_t>::max(),
          .min_bytes = 0,
          .prio = ss::default_priority_class(),
          .type_filter = std::move(type_filter),
        };

        auto reader = model::make_record_batch_reader<log_segment_batch_reader>(
          ctx.log_seg, ctx.tracker, std::move(config), ctx.prb);
        auto batches = reader.consume(consumer(), model::no_timeout).get0();

        std::set<int> types;
        for (auto& batch : batches) {
            types.insert(batch.type()());
        }
        return types;
    };

    auto types = read_types({});
    BOOST_TEST(types == std::set<int>({0, 1, 2, 3, 4}));

    types = read_types({1});
    BOOST_TEST(types == std::set<int>({1}));

    types = read_types({0, 1});
    BOOST_TEST(types == std::set<int>({0, 1}));

    types = read_types({1, 0});
    BOOST_TEST(types == std::set<int>({0, 1}));

    types = read_types({0, 2, 3, 4});
    BOOST_TEST(types == std::set<int>({0, 2, 3, 4}));

    types = read_types({3, 2, 4, 0});
    BOOST_TEST(types == std::set<int>({0, 2, 3, 4}));
}
