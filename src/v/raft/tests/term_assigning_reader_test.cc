#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "raft/consensus_utils.h"
#include "storage/tests/random_batch.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

SEASTAR_THREAD_TEST_CASE(test_assigning_batch_term) {
    auto batches = storage::test::make_random_batches(model::offset(0), 10);
    auto term = model::term_id(11);
    auto src_reader = model::make_memory_record_batch_reader(
      std::move(batches));
    auto assigning_reader
      = model::make_record_batch_reader<raft::details::term_assigning_reader>(
        std::move(src_reader), term);
    auto batches_with_term = model::consume_reader_to_memory(
                               std::move(assigning_reader), model::no_timeout)
                               .get0();

    BOOST_REQUIRE_EQUAL(batches_with_term.size(), 10);
    for (auto& b : batches_with_term) {
        BOOST_REQUIRE_EQUAL(b.term(), term);
    }
};

SEASTAR_THREAD_TEST_CASE(test_assigning_batch_term_release) {
    auto batches = storage::test::make_random_batches(model::offset(0), 10);
    auto term = model::term_id(11);
    auto src_reader = model::make_memory_record_batch_reader(
      std::move(batches));

    auto assigning_reader
      = model::make_record_batch_reader<raft::details::term_assigning_reader>(
        std::move(src_reader), term);
    auto batches_with_term = model::consume_reader_to_memory(
                               std::move(assigning_reader), model::no_timeout)
                               .get0();

    BOOST_REQUIRE_EQUAL(batches_with_term.size(), 10);
    for (auto& b : batches_with_term) {
        BOOST_REQUIRE_EQUAL(b.term(), term);
    }
};
