#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "raft/consensus_utils.h"
#include "raft/types.h"
#include "storage/record_batch_builder.h"
#include "storage/tests/random_batch.h"
#include "test_utils/rpc.h"

#include <seastar/core/future.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

#include <vector>

struct checking_consumer {
    using batches_t = std::vector<model::record_batch>;
    using batches_iter = std::vector<model::record_batch>::const_iterator;

    checking_consumer(std::vector<model::record_batch> exp)
      : expected(std::move(exp))
      , current_batch(expected.cbegin()) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch batch) {
        BOOST_REQUIRE_EQUAL(current_batch->base_offset(), batch.base_offset());
        BOOST_REQUIRE_EQUAL(current_batch->last_offset(), batch.last_offset());
        BOOST_REQUIRE_EQUAL(current_batch->crc(), batch.crc());
        BOOST_REQUIRE_EQUAL(current_batch->compressed(), batch.compressed());
        BOOST_REQUIRE_EQUAL(current_batch->type(), batch.type());
        BOOST_REQUIRE_EQUAL(current_batch->size_bytes(), batch.size_bytes());
        BOOST_REQUIRE_EQUAL(current_batch->size(), batch.size());
        current_batch++;
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::no);
    }

    void end_of_stream() { BOOST_CHECK(current_batch == expected.end()); }

    batches_t expected;
    batches_iter current_batch;
};

SEASTAR_THREAD_TEST_CASE(entry) {
    auto batches = storage::test::make_random_batches(
      model::offset(1), 3, false);
    raft::entry e(
      raft::data_batch_type,
      model::make_memory_record_batch_reader(std::move(batches)));

    std::vector<raft::entry> entries
      = raft::details::share_one_entry(std::move(e), 2, false).get0();
    auto d = serialize_roundtrip_rpc(std::move(entries.back())).get0();
    entries.pop_back();

    d.reader()
      .consume(
        checking_consumer(
          std::move(entries.back().reader().release_buffered_batches())),
        model::no_timeout)
      .get0();
}
