#include "model/record.h"
#include "raft/configuration_bootstrap_state.h"
#include "raft/consensus_utils.h"
#include "random/generators.h"
#include "resource_mgmt/io_priority.h"
#include "rpc/models.h"
#include "storage/log.h"
#include "storage/log_manager.h"
#include "storage/record_batch_builder.h"
// testing
#include "test_utils/fixture.h"

#include <seastar/core/print.hh>
#include <seastar/util/log.hh>

using namespace raft; // NOLINT
struct bootstrap_fixture {
    static constexpr int active_nodes = 3;
    bootstrap_fixture()
      : _mngr(storage::log_config{
        .base_dir = ".",
        .max_segment_size = 1 << 30,
        .should_sanitize = storage::log_config::sanitize_files::yes}) {
        _log = _mngr.manage(_ntp).get0();
    }
    std::vector<storage::log::append_result> write_n(const std::size_t n) {
        auto cfg = storage::log_append_config{
          storage::log_append_config::fsync::no,
          default_priority_class(),
          model::no_timeout};
        std::vector<storage::log::append_result> res;
        res.push_back(_log->append(datas(n), cfg).get0());
        res.push_back(_log->append(configs(n), cfg).get0());
        _log->flush().get();
        return res;
    }
    template<typename Func>
    model::record_batch_reader reader_gen(std::size_t n, Func&& f) {
        std::vector<model::record_batch> batches;
        batches.reserve(n);
        while (n-- > 0) {
            batches.push_back(f());
        }
        return model::make_memory_record_batch_reader(std::move(batches));
    }
    model::record_batch_reader configs(std::size_t n) {
        return reader_gen(n, [this] { return config_batch(); });
    }
    model::record_batch_reader datas(std::size_t n) {
        return reader_gen(n, [this] { return data_batch(); });
    }
    model::record_batch data_batch() {
        storage::record_batch_builder bldr(raft::data_batch_type, _base_offset);
        bldr.add_raw_kv(rand_fragbuf(), rand_fragbuf());
        ++_base_offset;
        return std::move(bldr).build();
    }
    model::record_batch config_batch() {
        storage::record_batch_builder bldr(
          raft::configuration_batch_type, _base_offset);
        bldr.add_raw_kv(rand_fragbuf(), rpc::serialize(rand_config()));
        ++_base_offset;
        return std::move(bldr).build();
    }
    fragbuf rand_fragbuf() const {
        std::vector<temporary_buffer<char>> fragments;
        fragments.reserve(1);
        auto data = random_generators::gen_alphanum_string(100);
        fragments.emplace_back(data.data(), data.size());
        return fragbuf(std::move(fragments));
    }
    raft::group_configuration rand_config() const {
        std::vector<model::broker> nodes;
        std::vector<model::broker> learners;
        auto bgen = [](int l, int h) {
            return model::broker(
              model::node_id(random_generators::get_int(l, h)), // id
              random_generators::gen_alphanum_string(10),       // host
              random_generators::get_int(1025, 65535),          // port
              std::nullopt);
        };
        for (auto i = 0; i < active_nodes; ++i) {
            nodes.push_back(bgen(i, i));
            learners.push_back(
              bgen(active_nodes + 1, active_nodes * active_nodes));
        }
        return raft::group_configuration{
          .leader_id = model::node_id(
            random_generators::get_int(0, active_nodes)),
          .nodes = std::move(nodes),
          .learners = std::move(learners)};
    }
    ~bootstrap_fixture() {
        _mngr.stop().get();
    }
    model::offset _base_offset{0};
    storage::log_ptr _log;
    storage::log_manager _mngr;
    model::ntp _ntp{
      model::ns("bootstrap_test_" + random_generators::gen_alphanum_string(8)),
      model::topic_partition{
        model::topic(random_generators::gen_alphanum_string(6)),
        model::partition_id(random_generators::get_int(0, 24))}};
};

FIXTURE_TEST(serde_config, bootstrap_fixture) {
    raft::entry e(raft::configuration_batch_type, configs(1));
    auto cfg = raft::details::extract_configuration(std::move(e)).get0();
    for (auto& n : cfg.nodes) {
        BOOST_REQUIRE(n.id() >= 0 && n.id() <= bootstrap_fixture::active_nodes);
    }
    for (auto& n : cfg.learners) {
        BOOST_REQUIRE(n.id() > bootstrap_fixture::active_nodes);
    }
}
FIXTURE_TEST(write_configs, bootstrap_fixture) {
    auto replies = write_n(10);
    for (auto& i : replies) {
        info("base:{}, last:{}", i.base_offset, i.last_offset);
    }
    for (auto& s : _log->segments()) {
        info("{}", *s);
    }
    auto cfg = raft::details::read_bootstrap_state(*_log).get0();
    info(
      "data batches:{}, config batches:{}",
      cfg.data_batches_seen(),
      cfg.config_batches_seen());

    BOOST_REQUIRE(cfg.is_finished());
    for (auto& n : cfg.config().nodes) {
        BOOST_REQUIRE(n.id() >= 0 && n.id() <= bootstrap_fixture::active_nodes);
    }
    for (auto& n : cfg.config().learners) {
        BOOST_REQUIRE(n.id() > bootstrap_fixture::active_nodes);
    }
    BOOST_REQUIRE_EQUAL(cfg.data_batches_seen(), 10);
    BOOST_REQUIRE_EQUAL(cfg.config_batches_seen(), 10);
}

FIXTURE_TEST(empty_log, bootstrap_fixture) {
    auto cfg = raft::details::read_bootstrap_state(*_log).get0();

    BOOST_REQUIRE(cfg.is_finished());
    BOOST_REQUIRE_EQUAL(cfg.data_batches_seen(), 0);
    BOOST_REQUIRE_EQUAL(cfg.config_batches_seen(), 0);
}