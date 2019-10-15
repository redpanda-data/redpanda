#include "raft/consensus.h"
#include "raft/consensus_utils.h"
#include "redpanda/config/config_store.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

using namespace raft; // NOLINT

SEASTAR_THREAD_TEST_CASE(write_and_read_config) {
    consensus::voted_for_configuration cfg;
    cfg.voted_for = model::node_id(42);
    cfg.term = model::term_id(77);
    std::cout << "persisting?" << std::endl;
    details::persist_voted_for("./test.yml", cfg).get();
    std::cout << "reading?" << std::endl;
    auto const cfg_dup = details::read_voted_for("./test.yml").get0();
    std::cout << "cfg.voted_for " << cfg.voted_for << ", cfg.term " << cfg.term
              << ", dup.voted_for" << cfg_dup.voted_for << ", dup.term "
              << cfg_dup.term << std::endl;

    BOOST_REQUIRE_EQUAL(cfg.voted_for, cfg_dup.voted_for);
    BOOST_REQUIRE_EQUAL(cfg.term, cfg_dup.term);
};
