// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

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
    size_t _depth;
    int _init_count;
    ss::circular_buffer<record_batch> _result;
};

template<typename... Offsets>
ss::circular_buffer<model::record_batch> make_batches(Offsets... o) {
    ss::circular_buffer<model::record_batch> batches;
    (batches.emplace_back(model::test::make_random_batch(o, 1, true)), ...);
    return batches;
}

record_batch_reader
make_generating_reader(ss::circular_buffer<record_batch> batches) {
    return make_generating_record_batch_reader(
      [batches = std::move(batches)]() mutable {
          return ss::make_ready_future<record_batch_reader::data_t>(
            std::move(batches));
      });
}

void do_test_consume(record_batch_reader reader) {
    auto batches = reader.consume(consumer(4), no_timeout).get();
    auto o = offset(1);
    for (auto& batch : batches) {
        BOOST_CHECK_EQUAL(batch.base_offset(), o);
        o += 1;
    }

    batches = reader.consume(consumer(4), no_timeout).get();
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
    auto batches = reader.consume(consumer(2), no_timeout).get();
    auto o = offset(1);
    for (auto& batch : batches) {
        BOOST_CHECK_EQUAL(batch.base_offset(), o);
        o += 1;
    }
    BOOST_CHECK_EQUAL(o, offset(3));

    batches = reader.consume(consumer(2), no_timeout).get();
    for (auto& batch : batches) {
        BOOST_CHECK_EQUAL(batch.base_offset(), o);
        o += 1;
    }
    BOOST_CHECK_EQUAL(o, offset(5));

    batches = reader.consume(consumer(2), no_timeout).get();
    for (auto& batch : batches) {
        BOOST_CHECK_EQUAL(batch.base_offset(), o);
        o += 1;
    }
    BOOST_CHECK_EQUAL(o, offset(6));

    batches = reader.consume(consumer(4), no_timeout).get();
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
    for (size_t i = 0; i < v1.size(); ++i) {
        BOOST_CHECK(v1[i] == v2[i]);
    }
}

SEASTAR_THREAD_TEST_CASE(empty_record_batch_reader) {
    auto reader = make_empty_record_batch_reader();
    auto data
      = consume_reader_to_memory(std::move(reader), model::no_timeout).get();
    BOOST_REQUIRE(data.empty());
}
