#include "raft/consensus.h"
#include "raft/consensus_utils.h"
#include "redpanda/config/config_store.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

using namespace raft; // NOLINT

SEASTAR_THREAD_TEST_CASE(write_and_read_voted_for_config) {
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
// TODO replace these batch-making with @michal's fluent api
model::record_batch_header make_header(model::offset o) {
    return model::record_batch_header{1,
                                      o,
                                      model::record_batch_type(1),
                                      1,
                                      model::record_batch_attributes(),
                                      0,
                                      model::timestamp(),
                                      model::timestamp()};
}
model::record_batch make_batch(model::offset o) {
    return model::record_batch(
      make_header(o), model::record_batch::compressed_records(1, {}));
}

template<typename... Offsets>
std::vector<model::record_batch> make_batches(Offsets... o) {
    std::vector<model::record_batch> batches;
    (batches.emplace_back(make_batch(o)), ...);
    return batches;
}

SEASTAR_THREAD_TEST_CASE(clone_entries_utils) {
    static constexpr const size_t sz = 10;
    std::vector<entry> entries;
    entries.reserve(sz);
    for (size_t i = 0; i < sz; ++i) {
        auto reader = model::make_memory_record_batch_reader(make_batches(
          model::offset(1),
          model::offset(2),
          model::offset(3),
          model::offset(4)));
        entries.emplace_back(model::record_batch_type(1), std::move(reader));
    }
    auto v = details::copy_n(std::move(entries), 5).get0();
    for (auto& i : v) {
        BOOST_REQUIRE_EQUAL(i.size(), sz);
    }
    for (auto& i : v) {
        for (auto& j : v) {
            for (size_t k = 0; k < i.size(); ++k) {
                i[k].reader().load_slice(model::no_timeout).get();
                j[k].reader().load_slice(model::no_timeout).get();
                BOOST_REQUIRE_EQUAL(i[k].entry_type(), j[k].entry_type());
                BOOST_REQUIRE_EQUAL(
                  i[k].reader().peek_batch(), j[k].reader().peek_batch());
            }
        }
    }
}
