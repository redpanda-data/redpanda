#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "random/generators.h"
#include "storage/disk_log_appender.h"
#include "storage/log_reader.h"
#include "storage/log_segment_appender.h"
#include "storage/log_segment_appender_utils.h"
#include "storage/log_segment_reader.h"
#include "storage/tests/utils/random_batch.h"
#include "utils/file_sanitizer.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

using namespace storage; // NOLINT

struct context {
    std::unique_ptr<segment> _seg;
    probe prb;
    log_set logs = log_set({});

    void initialize(model::offset base) {
        ss::sstring base_name = "test."
                                + random_generators::gen_alphanum_string(20);
        auto fd = ss::open_file_dma(
                    base_name,
                    ss::open_flags::truncate | ss::open_flags::create
                      | ss::open_flags::rw)
                    .get0();
        auto fidx = ss::open_file_dma(
                      base_name + ".offset_index",
                      ss::open_flags::truncate | ss::open_flags::create
                        | ss::open_flags::rw)
                      .get0();
        fd = ss::file(ss::make_shared(file_io_sanitizer(std::move(fd))));
        fidx = ss::file(ss::make_shared(file_io_sanitizer(std::move(fidx))));

        auto appender = std::make_unique<log_segment_appender>(
          fd, log_segment_appender::options(ss::default_priority_class()));
        auto indexer = std::make_unique<segment_index>(
          base_name + ".offset_index", std::move(fidx), base, 4096);
        auto reader = ss::make_lw_shared<log_segment_reader>(
          base_name,
          // file.dup() _must_ be for read only. the opposite order between
          // appender and reader is a bug.
          ss::file(fd.dup()),
          model::term_id(0),
          base,
          appender->file_byte_offset(),
          128);
        _seg = std::make_unique<segment>(
          reader, std::move(indexer), std::move(appender), nullptr);
    }

    void write(ss::circular_buffer<model::record_batch> batches) {
        initialize(batches.begin()->base_offset());
        for (auto& b : batches) {
            _seg->append(std::move(b)).get();
        }
        _seg->flush().get();
    }

    model::record_batch_reader reader(
      model::offset start,
      model::offset end,
      size_t max_bytes = std::numeric_limits<size_t>::max()) {
        auto cfg = log_reader_config(
          start,
          end,
          0,
          max_bytes,
          ss::default_priority_class(),
          std::nullopt,
          std::nullopt);
        if (_seg) {
            logs.add(std::move(_seg));
        }
        return model::make_record_batch_reader<log_reader>(
          logs, std::move(cfg), prb);
    }

    model::record_batch_reader reader(log_reader_config cfg) {
        if (_seg) {
            logs.add(std::move(_seg));
        }
        return model::make_record_batch_reader<log_reader>(
          logs, std::move(cfg), prb);
    }

    ~context() {
        for (auto& log_seg : logs) {
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
    ss::circular_buffer<model::record_batch> end_of_stream() {
        return std::move(_result);
    }

private:
    ss::circular_buffer<model::record_batch> _result;
};

#define check_batches(actual, expected)                                        \
    BOOST_REQUIRE_EQUAL_COLLECTIONS(                                           \
      actual.begin(), actual.end(), expected.begin(), expected.end());

static ss::circular_buffer<model::record_batch>
copy(ss::circular_buffer<model::record_batch>& input) {
    ss::circular_buffer<model::record_batch> ret;
    ret.reserve(input.size());
    for (auto& b : input) {
        ret.push_back(b.share());
    }
    return ret;
}

SEASTAR_THREAD_TEST_CASE(test_can_read_single_batch_smaller_offset) {
    context ctx;
    auto batches = test::make_random_batches(model::offset(1), 1);
    ctx.write(copy(batches));
    {
        auto reader = ctx.reader(model::offset(0), model::offset(0));
        auto res = reader.consume(consumer(), model::no_timeout).get0();
        BOOST_REQUIRE(res.empty());
    }
}

SEASTAR_THREAD_TEST_CASE(test_can_read_single_batch_same_offset) {
    context ctx;
    auto batches = test::make_random_batches(model::offset(1), 1);
    ctx.write(copy(batches));
    {
        auto reader = ctx.reader(
          batches.front().base_offset(), batches.front().last_offset());
        auto res = reader.consume(consumer(), model::no_timeout).get0();
        check_batches(res, batches);
    }
}

SEASTAR_THREAD_TEST_CASE(test_can_read_multiple_batches) {
    context ctx;
    auto batches = test::make_random_batches(model::offset(1));
    ctx.write(copy(batches));
    {
        auto reader = ctx.reader(
          batches.front().base_offset(), batches.back().last_offset());
        auto res = reader.consume(consumer(), model::no_timeout).get0();
        check_batches(res, batches);
    }
}

SEASTAR_THREAD_TEST_CASE(test_does_not_read_past_committed_offset_one_segment) {
    context ctx;
    auto batches = test::make_random_batches(model::offset(2));
    ctx.write(copy(batches));
    {
        auto reader = ctx.reader(
          batches.back().last_offset() + model::offset(1),
          batches.back().last_offset() + model::offset(1));
        auto res = reader.consume(consumer(), model::no_timeout).get0();
        BOOST_REQUIRE(res.empty());
    }
}

SEASTAR_THREAD_TEST_CASE(
  test_does_not_read_past_committed_offset_multiple_segments) {
    context ctx;
    auto batches = test::make_random_batches(model::offset(1), 2);
    ctx.write(copy(batches));
    auto o = batches.begin()->last_offset();
    {
        auto reader = ctx.reader(
          batches.back().last_offset(), batches.back().last_offset());
        auto res = reader.consume(consumer(), model::no_timeout).get0();
        std::vector<model::record_batch> first;
        first.push_back(std::move(batches.back()));
        check_batches(res, first);
    }
}

SEASTAR_THREAD_TEST_CASE(test_does_not_read_past_max_bytes) {
    context ctx;
    auto batches = test::make_random_batches(model::offset(1), 2);
    ctx.write(copy(batches));
    auto o = batches.begin()->last_offset();
    {
        auto reader = ctx.reader(
          batches.front().base_offset(),
          batches.front().last_offset(),
          batches.begin()->size_bytes());
        auto res = reader.consume(consumer(), model::no_timeout).get0();
        std::vector<model::record_batch> first;
        first.push_back(std::move(*batches.begin()));
        check_batches(res, first);
    }
}

SEASTAR_THREAD_TEST_CASE(test_reads_at_least_one_batch) {
    context ctx;
    auto batches = test::make_random_batches(model::offset(1), 2);
    ctx.write(copy(batches));
    auto o = batches.begin()->last_offset();
    {
        auto reader = ctx.reader(
          batches.front().base_offset(), batches.front().last_offset());
        auto res = reader.consume(consumer(), model::no_timeout).get0();
        ss::circular_buffer<model::record_batch> first;
        first.push_back(std::move(batches.front()));
        check_batches(res, first);
    }
}

SEASTAR_THREAD_TEST_CASE(test_read_batch_range) {
    context ctx;
    auto batches = test::make_random_batches(model::offset(0), 10);
    ctx.write(copy(batches));
    {
        // read batches from 3 to 7
        auto start_offset = batches[2].base_offset();
        auto max_offset = batches[6].last_offset();
        auto reader = ctx.reader(
          batches.front().base_offset(), batches.back().last_offset());

        auto res = reader.consume(consumer(), model::no_timeout).get0();
        BOOST_REQUIRE_EQUAL_COLLECTIONS(
          std::next(res.begin(), 2),
          std::next(res.begin(), 7),
          std::next(batches.begin(), 2),
          std::next(batches.begin(), 7));
    }
}

SEASTAR_THREAD_TEST_CASE(test_batch_type_filter) {
    // write some batches with various types
    auto batches = test::make_random_batches(model::offset(0), 5);
    for (auto i = 0u; i < batches.size(); i++) {
        batches[i].header().type = model::record_batch_type(i);
    }
    context ctx;
    ctx.write(copy(batches));

    // read and extract types with optional type filter
    auto read_types = [&ctx, &batches](int type_wanted) {
        std::optional<model::record_batch_type> type_filter
          = model::record_batch_type(type_wanted);

        auto config = log_reader_config(
          batches.front().base_offset(),
          batches.back().last_offset(),
          0,
          std::numeric_limits<size_t>::max(),
          ss::default_priority_class(),
          type_filter,
          std::nullopt);

        auto reader = ctx.reader(config);
        auto res = reader.consume(consumer(), model::no_timeout).get0();

        std::set<int> types;
        for (auto& batch : res) {
            types.insert(batch.header().type);
        }
        return types;
    };

    auto types = read_types({});
    BOOST_TEST(types == std::set<int>({0, 1, 2, 3, 4}));

    types = read_types(1);
    BOOST_TEST(types == std::set<int>({1}));

    types = read_types(0);
    BOOST_TEST(types == std::set<int>({0}));

    types = read_types(2);
    BOOST_TEST(types == std::set<int>({2}));

    types = read_types(4);
    BOOST_TEST(types == std::set<int>({4}));
}
