#include "config/config_store.h"
#include "model/timeout_clock.h"
#include "raft/consensus.h"
#include "raft/consensus_utils.h"
#include "storage/tests/utils/random_batch.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

SEASTAR_THREAD_TEST_CASE(write_and_read_voted_for_config) {
    raft::consensus::voted_for_configuration cfg;
    cfg.voted_for = model::node_id(42);
    cfg.term = model::term_id(77);
    std::cout << "persisting?" << std::endl;
    raft::details::persist_voted_for("./test.yml", cfg).get();
    std::cout << "reading?" << std::endl;
    auto const cfg_dup = raft::details::read_voted_for("./test.yml").get0();
    std::cout << "cfg.voted_for " << cfg.voted_for << ", cfg.term " << cfg.term
              << ", dup.voted_for" << cfg_dup.voted_for << ", dup.term "
              << cfg_dup.term << std::endl;

    BOOST_REQUIRE_EQUAL(cfg.voted_for, cfg_dup.voted_for);
    BOOST_REQUIRE_EQUAL(cfg.term, cfg_dup.term);
};

SEASTAR_THREAD_TEST_CASE(clone_entries_utils) {
    auto reader = model::make_memory_record_batch_reader(
      storage::test::make_random_batches());

    auto v = raft::details::share_n(std::move(reader), 5).get0();
    for (auto& i : v) {
        i.load_slice(model::no_timeout).get();
    }
    for (auto& i : v) {
        for (auto& j : v) {
            BOOST_REQUIRE_EQUAL(i.peek_batch(), j.peek_batch());
        }
    }
}
