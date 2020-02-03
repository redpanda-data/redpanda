
#include "storage/offset_assignment.h"
#include "storage/tests/utils/random_batch.h"

#include <seastar/testing/thread_test_case.hh>

using namespace storage; // NOLINT

struct offset_validating_consumer {
    offset_validating_consumer(model::offset o)
      : starting_offset(o) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch&& batch) {
        BOOST_REQUIRE_EQUAL(batch.base_offset(), starting_offset);
        starting_offset += batch.record_count();
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::no);
    }

    void end_of_stream() {}

    model::offset starting_offset;
};

SEASTAR_THREAD_TEST_CASE(test_offset_assignment) {
    auto batches = storage::test::make_random_batches(model::offset(0), 10);
    auto reader = model::make_memory_record_batch_reader(std::move(batches));
    auto starting_offset = model::offset(123);
    reader
      .consume(
        wrap_with_offset_assignment(
          offset_validating_consumer(starting_offset), starting_offset),
        model::no_timeout)
      .get();
};
