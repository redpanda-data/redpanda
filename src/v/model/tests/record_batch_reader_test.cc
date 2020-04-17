#include "model/record.h"
#include "model/record_batch_reader.h"
#include "storage/tests/utils/random_batch.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

#include <vector>

using namespace model; // NOLINT

class consumer {
public:
    explicit consumer(size_t depth)
      : _depth(depth) {}

    ss::future<ss::stop_iteration> operator()(record_batch b) {
        _result.push_back(std::move(b));
        if (--_depth == 0) {
            return ss::make_ready_future<ss::stop_iteration>(
              ss::stop_iteration::yes);
        }
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::no);
    }

    ss::circular_buffer<record_batch> end_of_stream() {
        return std::move(_result);
    }

private:
    ss::circular_buffer<record_batch> _result;
    size_t _depth;
};

class consumer_with_init {
public:
    explicit consumer_with_init(size_t depth)
      : _depth(depth)
      , _init_count(0) {}

    ss::future<> initialize() {
        _init_count++;
        return ss::make_ready_future<>();
    }

    ss::future<ss::stop_iteration> operator()(record_batch b) {
        _result.push_back(std::move(b));
        if (--_depth == 0) {
            return ss::make_ready_future<ss::stop_iteration>(
              ss::stop_iteration::yes);
        }
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::no);
    }

    struct consumer_ret {
        int init_count;
        ss::circular_buffer<record_batch> batches;
    };

    consumer_ret end_of_stream() {
        return consumer_ret{
          .init_count = _init_count,
          .batches = std::move(_result),
        };
    }

private:
    int _init_count;
    ss::circular_buffer<record_batch> _result;
    size_t _depth;
};

template<typename... Offsets>
ss::circular_buffer<model::record_batch> make_batches(Offsets... o) {
    ss::circular_buffer<model::record_batch> batches;
    (batches.emplace_back(storage::test::make_random_batch(o, 1, true)), ...);
    return batches;
}

record_batch_reader
make_generating_reader(ss::circular_buffer<record_batch> batches) {
    return make_generating_record_batch_reader(
      [batches = std::move(batches)]() mutable {
          if (batches.empty()) {
              return ss::make_ready_future<record_batch_opt>();
          }
          auto batch = std::move(batches.front());
          batches.pop_front();
          return ss::make_ready_future<record_batch_opt>(std::move(batch));
      });
}

SEASTAR_THREAD_TEST_CASE(test_pop) {
    auto reader = make_memory_record_batch_reader(
      make_batches(offset(1), offset(2), offset(3), offset(4)));
    {
        BOOST_REQUIRE(reader.should_load_slice());
        reader.load_slice(no_timeout).get();
        BOOST_REQUIRE(!reader.should_load_slice());
    };
    {
        auto& batch = reader.peek_batch();
        BOOST_CHECK_EQUAL(batch.base_offset(), offset(1));
        reader.pop_batch();
    }
    {
        auto batch = reader.pop_batch();
        BOOST_CHECK_EQUAL(batch.base_offset(), offset(2));
        reader.pop_batch();
    }
    {
        BOOST_REQUIRE(!reader.should_load_slice());
        BOOST_REQUIRE(!reader.is_end_of_stream());
    }
    {
        auto batch = reader.pop_batch();
        BOOST_CHECK_EQUAL(batch.base_offset(), offset(4));
    }
    {
        BOOST_REQUIRE(!reader.should_load_slice());
        BOOST_REQUIRE(reader.is_end_of_stream());
    }
}

SEASTAR_THREAD_TEST_CASE(test_pop_multiple_slices) {
    auto reader = make_generating_reader(
      make_batches(offset(1), offset(2), offset(3), offset(4)));
    {
        BOOST_REQUIRE(reader.should_load_slice());
        reader.load_slice(no_timeout).get();
        BOOST_REQUIRE(!reader.should_load_slice());
    }
    {
        auto& batch = reader.peek_batch();
        BOOST_CHECK_EQUAL(batch.base_offset(), offset(1));
        reader.pop_batch();
    }
    {
        BOOST_REQUIRE(reader.should_load_slice());
        reader.load_slice(no_timeout).get();
        auto batch = reader.pop_batch();
        BOOST_CHECK_EQUAL(batch.base_offset(), offset(2));
        reader.load_slice(no_timeout).get();
        reader.pop_batch();
    }
    {
        BOOST_REQUIRE(reader.should_load_slice());
        reader.load_slice(no_timeout).get();
        auto batch = reader.pop_batch();
        BOOST_CHECK_EQUAL(batch.base_offset(), offset(4));
        BOOST_REQUIRE(!reader.is_end_of_stream());
    }
    {
        BOOST_REQUIRE(reader.should_load_slice());
        reader.load_slice(no_timeout).get();
        BOOST_REQUIRE(!reader.should_load_slice());
        BOOST_REQUIRE(reader.is_end_of_stream());
    }
}

void do_test_consume(record_batch_reader reader) {
    auto batches = reader.consume(consumer(4), no_timeout).get0();
    auto o = offset(1);
    for (auto& batch : batches) {
        BOOST_CHECK_EQUAL(batch.base_offset(), o);
        o += 1;
    }

    batches = reader.consume(consumer(4), no_timeout).get0();
    BOOST_CHECK_EQUAL(batches.size(), 0);
}

SEASTAR_THREAD_TEST_CASE(test_consume) {
    do_test_consume(make_memory_record_batch_reader(
      make_batches(offset(1), offset(2), offset(3), offset(4))));
}

SEASTAR_THREAD_TEST_CASE(test_consume_multiple_slices) {
    do_test_consume(make_generating_reader(
      make_batches(offset(1), offset(2), offset(3), offset(4))));
}

void do_test_interrupt_consume(record_batch_reader reader) {
    auto batches = reader.consume(consumer(2), no_timeout).get0();
    auto o = offset(1);
    for (auto& batch : batches) {
        BOOST_CHECK_EQUAL(batch.base_offset(), o);
        o += 1;
    }
    BOOST_CHECK_EQUAL(o, offset(3));

    batches = reader.consume(consumer(2), no_timeout).get0();
    for (auto& batch : batches) {
        BOOST_CHECK_EQUAL(batch.base_offset(), o);
        o += 1;
    }
    BOOST_CHECK_EQUAL(o, offset(5));

    batches = reader.consume(consumer(2), no_timeout).get0();
    for (auto& batch : batches) {
        BOOST_CHECK_EQUAL(batch.base_offset(), o);
        o += 1;
    }
    BOOST_CHECK_EQUAL(o, offset(6));

    batches = reader.consume(consumer(4), no_timeout).get0();
    BOOST_CHECK_EQUAL(batches.size(), 0);
}

SEASTAR_THREAD_TEST_CASE(test_interrupt_consume) {
    do_test_interrupt_consume(make_memory_record_batch_reader(
      make_batches(offset(1), offset(2), offset(3), offset(4), offset(5))));
}

SEASTAR_THREAD_TEST_CASE(test_interrupt_consume_multiple_slices) {
    do_test_interrupt_consume(make_generating_reader(
      make_batches(offset(1), offset(2), offset(3), offset(4), offset(5))));
}
SEASTAR_THREAD_TEST_CASE(record_batch_sharing) {
    auto v1 = make_batches(
      offset(1), offset(2), offset(3), offset(4), offset(5));
    decltype(v1) v2;
    v2.reserve(v1.size());
    std::transform(
      v1.begin(), v1.end(), std::back_inserter(v2), [](record_batch& batch) {
          return batch.share();
      });

    BOOST_CHECK_EQUAL(v1.size(), v2.size());
    for (auto i = 0; i < v1.size(); ++i) {
        BOOST_CHECK(v1[i] == v2[i]);
    }
}
