#include "model/record.h"
#include "raft/configuration_bootstrap_state.h"
#include "raft/consensus_utils.h"
#include "random/generators.h"
#include "resource_mgmt/io_priority.h"
#include "rpc/models.h"
#include "storage/log.h"
#include "storage/log_manager.h"
#include "storage/record_batch_builder.h"
#include "test_utils/randoms.h"
// testing
#include "raft/tests/simple_record_fixture.h"
#include "test_utils/fixture.h"

#include <seastar/core/print.hh>
#include <seastar/util/log.hh>

using namespace raft; // NOLINT
struct bootstrap_fixture : raft::simple_record_fixture {
    using raft::simple_record_fixture::active_nodes;
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

    ~bootstrap_fixture() { _mngr.stop().get(); }
    storage::log_ptr _log;
    storage::log_manager _mngr;
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
