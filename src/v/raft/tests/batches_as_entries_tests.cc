#include "raft/consensus_utils.h"

// tests
#include "raft/tests/simple_record_fixture.h"
#include "test_utils/fixture.h"

using test_fixture = raft::simple_record_fixture;
FIXTURE_TEST(batches_as_entries_sequential, test_fixture) {
    std::vector<model::record_batch> batches;
    batches.reserve(20);
    for (size_t i = 0; i < 10; ++i) {
        batches.push_back(data_batch());
    }
    for (size_t i = 0; i < 10; ++i) {
        batches.push_back(config_batch());
    }
    auto entries = raft::details::batches_as_entries(std::move(batches));
    BOOST_REQUIRE_EQUAL(entries.size(), 2);
}
FIXTURE_TEST(batches_as_entries_interspersed, test_fixture) {
    std::vector<model::record_batch> batches;
    batches.reserve(20);
    for (size_t i = 0; i < 10; ++i) {
        batches.push_back(data_batch());
        batches.push_back(config_batch());
    }
    auto entries = raft::details::batches_as_entries(std::move(batches));
    BOOST_REQUIRE_EQUAL(entries.size(), 20);
    info("20 batches of 2 types interspersed must produce 20 entries "
         "unfortunately");
}
FIXTURE_TEST(batches_as_entries_empty, test_fixture) {
    std::vector<model::record_batch> batches;
    auto entries = raft::details::batches_as_entries(std::move(batches));
    BOOST_REQUIRE_EQUAL(entries.size(), 0);
}
FIXTURE_TEST(batches_as_entries_one_type, test_fixture) {
    std::vector<model::record_batch> batches;
    batches.reserve(20);
    for (size_t i = 0; i < 20; ++i) {
        batches.push_back(data_batch());
    }
    auto entries = raft::details::batches_as_entries(std::move(batches));
    BOOST_REQUIRE_EQUAL(entries.size(), 1);
}
