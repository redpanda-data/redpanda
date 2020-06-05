#include "config/config_store.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "raft/consensus.h"
#include "raft/consensus_utils.h"
#include "storage/tests/utils/random_batch.h"
#include "utils/state_crc_file.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

/*
 * The yaml encoder and voted_for writer utilities below have been migrated from
 * raft::consensus_utils to be used to create the files necessary to test the
 * decoder and readers. The decoder and readers remain used for handling
 * upgrades, but the writers are no longer used. They have been replaced by the
 * key-value store.
 */
namespace YAML {
template<>
struct convert<::raft::consensus::voted_for_configuration> {
    static Node encode(const ::raft::consensus::voted_for_configuration& c) {
        Node node;
        node["voted_for"] = c.voted_for();
        node["term"] = c.term();
        return node;
    }
};
} // namespace YAML

static ss::future<> persist_voted_for(
  ss::sstring filename, raft::consensus::voted_for_configuration r) {
    return utils::state_crc_file(std::move(filename)).persist(std::move(r));
}

static ss::future<>
writefile(ss::sstring name, ss::temporary_buffer<char> buf) {
    // open the file in synchronous mode
    vassert(
      buf.size() <= 4096,
      "This utility is inteded to save the voted_for file contents, which is "
      "usually 16 bytes at most. but asked to save: {}",
      buf.size());
    auto flags = ss::open_flags::wo | ss::open_flags::create
                 | ss::open_flags::truncate;
    return ss::open_file_dma(std::move(name), flags)
      .then([b = std::move(buf)](ss::file f) {
          std::unique_ptr<char[], ss::free_deleter> buf(
            ss::allocate_aligned_buffer<char>(4096, 4096));
          std::memset(buf.get(), 0, 4096);
          // copy the yaml file
          const size_t yaml_size = b.size();
          std::copy_n(b.get(), yaml_size, buf.get() /*destination*/);
          auto ptr = buf.get();
          return f.dma_write(0, ptr, 4096, ss::default_priority_class())
            .then(
              [buf = std::move(buf), f](size_t) mutable { return f.flush(); })
            .then([f, yaml_size]() mutable { return f.truncate(yaml_size); })
            .then([f]() mutable { return f.close(); })
            .finally([f] {});
      });
}

static ss::future<> legacy_persist_voted_for(
  ss::sstring filename, raft::consensus::voted_for_configuration r) {
    YAML::Emitter out;
    out << YAML::convert<raft::consensus::voted_for_configuration>::encode(r);
    ss::temporary_buffer<char> buf(out.size());
    std::copy_n(out.c_str(), out.size(), buf.get_write());
    return writefile(filename, std::move(buf));
}

SEASTAR_THREAD_TEST_CASE(test_reading_legacy_voted_for_config) {
    raft::consensus::voted_for_configuration cfg;
    cfg.voted_for = model::node_id(42);
    cfg.term = model::term_id(77);

    legacy_persist_voted_for("./test.yml", cfg).get();
    auto read = raft::details::read_voted_for("./test.yml").get0().value();

    BOOST_REQUIRE_EQUAL(cfg.voted_for, read.voted_for);
    BOOST_REQUIRE_EQUAL(cfg.term, read.term);
};

SEASTAR_THREAD_TEST_CASE(write_and_read_voted_for_config) {
    raft::consensus::voted_for_configuration cfg;
    cfg.voted_for = model::node_id(42);
    cfg.term = model::term_id(77);
    std::cout << "persisting?" << std::endl;
    persist_voted_for("./test.yml", cfg).get();
    std::cout << "reading?" << std::endl;
    auto const cfg_dup
      = raft::details::read_voted_for("./test.yml").get0().value();
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
    std::vector<ss::circular_buffer<model::record_batch>> data;
    data.reserve(5);
    for (auto& i : v) {
        data.emplace_back(
          model::consume_reader_to_memory(std::move(i), model::no_timeout)
            .get0());
    }
    for (const auto& i : data) {
        for (const auto& j : data) {
            BOOST_REQUIRE(std::equal(i.begin(), i.end(), j.begin()));
        }
    }
}
