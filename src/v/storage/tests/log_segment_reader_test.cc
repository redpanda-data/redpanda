#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "random/generators.h"
#include "storage/disk_log_appender.h"
#include "storage/log_reader.h"
#include "storage/log_segment_appender.h"
#include "storage/log_segment_appender_utils.h"
#include "storage/segment_reader.h"
#include "storage/tests/utils/random_batch.h"
#include "utils/disk_log_builder.h"
#include "utils/file_sanitizer.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

using namespace storage; // NOLINT

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

void write(
  ss::circular_buffer<model::record_batch> batches, disk_log_builder& builder) {
    auto seg = builder.get_log_segments().front().get();
    for (auto& b : batches) {
        seg->append(std::move(b)).get();
    }
    seg->flush().get();
}

SEASTAR_THREAD_TEST_CASE(test_can_read_single_batch_smaller_offset) {
    disk_log_builder b;
    b | start() | add_segment(1);
    auto buf = test::make_random_batches(model::offset(1), 1);
    write(std::move(buf), b);
    // To-do Kostas Add support for pipe consume!
    auto res = b.consume().get0();
    b | stop();
    BOOST_REQUIRE(res.empty());
}

SEASTAR_THREAD_TEST_CASE(test_can_read_single_batch_same_offset) {
    storage::log_reader_config reader_config(
      model::offset(1),
      model::offset(1),
      0,
      model::model_limits<model::offset>::max(),
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt);
    disk_log_builder b;
    b | start() | add_segment(1);
    auto batches = test::make_random_batches(model::offset(1), 1);
    write(copy(batches), b);
    auto res = b.consume(reader_config).get0();
    b | stop();
    check_batches(res, batches);
}

SEASTAR_THREAD_TEST_CASE(test_can_read_multiple_batches) {
    auto batches = test::make_random_batches(model::offset(1));
    storage::log_reader_config reader_config(
      batches.front().base_offset(),
      batches.back().last_offset(),
      0,
      model::model_limits<model::offset>::max(),
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt);
    disk_log_builder b;
    b | start() | add_segment(batches.front().base_offset());
    write(copy(batches), b);
    auto res = b.consume(reader_config).get0();
    b | stop();
    check_batches(res, batches);
}

SEASTAR_THREAD_TEST_CASE(test_does_not_read_past_committed_offset_one_segment) {
    auto batches = test::make_random_batches(model::offset(2));
    storage::log_reader_config reader_config(
      batches.back().last_offset() + model::offset(1),
      batches.back().last_offset() + model::offset(1),
      0,
      model::model_limits<model::offset>::max(),
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt);
    disk_log_builder b;
    b | start() | add_segment(batches.front().base_offset());
    write(copy(batches), b);
    auto res = b.consume(reader_config).get0();
    b | stop();
    BOOST_REQUIRE(res.empty());
}

SEASTAR_THREAD_TEST_CASE(
  test_does_not_read_past_committed_offset_multiple_segments) {
    auto batches = test::make_random_batches(model::offset(1), 2);
    storage::log_reader_config reader_config(
      batches.back().last_offset(),
      batches.back().last_offset(),
      0,
      model::model_limits<model::offset>::max(),
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt);
    disk_log_builder b;
    b | start() | add_segment(batches.front().base_offset());
    write(copy(batches), b);
    auto res = b.consume(reader_config).get0();
    b | stop();
    ss::circular_buffer<model::record_batch> first;
    first.push_back(std::move(batches.back()));
    check_batches(res, first);
}

SEASTAR_THREAD_TEST_CASE(test_does_not_read_past_max_bytes) {
    auto batches = test::make_random_batches(model::offset(1), 2);
    storage::log_reader_config reader_config(
      batches.front().base_offset(),
      batches.front().last_offset(),
      0,
      static_cast<size_t>(batches.begin()->size_bytes()),
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt);
    disk_log_builder b;
    b | start() | add_segment(batches.front().base_offset());
    write(copy(batches), b);
    auto res = b.consume(reader_config).get0();
    b | stop();
    ss::circular_buffer<model::record_batch> first;
    first.push_back(std::move(*batches.begin()));
    check_batches(res, first);
}

SEASTAR_THREAD_TEST_CASE(test_reads_at_least_one_batch) {
    auto batches = test::make_random_batches(model::offset(1), 2);
    storage::log_reader_config reader_config(
      batches.front().base_offset(),
      batches.front().last_offset(),
      0,
      model::model_limits<model::offset>::max(),
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt);
    disk_log_builder b;
    b | start() | add_segment(batches.front().base_offset());
    write(copy(batches), b);
    auto res = b.consume(reader_config).get0();
    b | stop();
    ss::circular_buffer<model::record_batch> first;
    first.push_back(std::move(batches.front()));
    check_batches(res, first);
}

SEASTAR_THREAD_TEST_CASE(test_read_batch_range) {
    auto batches = test::make_random_batches(model::offset(0), 10);
    storage::log_reader_config reader_config(
      batches.front().base_offset(),
      batches.back().last_offset(),
      0,
      model::model_limits<model::offset>::max(),
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt);
    disk_log_builder b;
    b | start();
    b | add_segment(batches.front().base_offset());
    write(copy(batches), b);
    auto res = b.consume(reader_config).get0();
    b | stop();
    BOOST_REQUIRE_EQUAL_COLLECTIONS(
      std::next(res.begin(), 2),
      std::next(res.begin(), 7),
      std::next(batches.begin(), 2),
      std::next(batches.begin(), 7));
}

SEASTAR_THREAD_TEST_CASE(test_batch_type_filter) {
    auto batches = test::make_random_batches(model::offset(0), 5);
    for (auto i = 0u; i < batches.size(); i++) {
        batches[i].header().type = model::record_batch_type(i);
    }

    storage::log_reader_config reader_config(
      batches.front().base_offset(),
      batches.back().last_offset(),
      0,
      model::model_limits<model::offset>::max(),
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt);
    disk_log_builder b;
    b | start() | add_segment(batches.front().base_offset());
    write(copy(batches), b);

    // read and extract types with optional type filter
    auto read_types =
      [&b, &batches](std::optional<int> type_wanted) -> std::vector<int> {
        std::optional<model::record_batch_type> type_filter;
        if (type_wanted) {
            type_filter = model::record_batch_type(type_wanted.value());
        }

        auto config = log_reader_config(
          batches.front().base_offset(),
          batches.back().last_offset(),
          0,
          std::numeric_limits<size_t>::max(),
          ss::default_priority_class(),
          type_filter,
          std::nullopt);

        auto res = b.consume(config).get0();

        std::set<int> types;
        for (auto& batch : res) {
            types.insert(batch.header().type);
        }
        return {types.begin(), types.end()};
    };

    std::vector<int> types = read_types({});
    BOOST_CHECK_EQUAL(types, std::vector<int>({0, 1, 2, 3, 4}));

    types = read_types(1);
    BOOST_TEST(types == std::vector<int>({1}));

    types = read_types(0);
    BOOST_TEST(types == std::vector<int>({0}));

    types = read_types(2);
    BOOST_TEST(types == std::vector<int>({2}));

    types = read_types(4);
    BOOST_TEST(types == std::vector<int>({4}));

    b | stop();
}
