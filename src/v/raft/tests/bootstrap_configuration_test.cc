#include "model/adl_serde.h"
#include "model/record.h"
#include "raft/configuration_bootstrap_state.h"
#include "raft/consensus_utils.h"
#include "random/generators.h"
#include "resource_mgmt/io_priority.h"
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
      : _mngr(storage::log_config(
        storage::log_config::storage_type::disk,
        "test.dir",
        1_GiB,
        storage::debug_sanitize_files::yes,
        storage::log_config::with_cache::no)) {
        // ignore the get_log()
        (void)_mngr.manage(storage::ntp_config(_ntp, "test.dir")).get0();
    }
    std::vector<storage::append_result> write_n(const std::size_t n) {
        const auto cfg = storage::log_append_config{
          storage::log_append_config::fsync::no,
          ss::default_priority_class(),
          model::no_timeout};
        std::vector<storage::append_result> res;
        res.push_back(datas(n)
                        .for_each_ref(get_log().make_appender(cfg), cfg.timeout)
                        .get0());
        res.push_back(configs(n)
                        .for_each_ref(get_log().make_appender(cfg), cfg.timeout)
                        .get0());
        get_log().flush().get();
        return res;
    }
    storage::log get_log() { return _mngr.get(_ntp).value(); }

    ~bootstrap_fixture() { _mngr.stop().get(); }
    storage::log_manager _mngr;
    ss::abort_source _as;
};

FIXTURE_TEST(serde_config, bootstrap_fixture) {
    auto rdr = configs(1);
    auto cfg = raft::details::extract_configuration(std::move(rdr)).get0();
    for (auto& n : cfg->nodes) {
        BOOST_REQUIRE(n.id() >= 0 && n.id() <= bootstrap_fixture::active_nodes);
    }
    for (auto& n : cfg->learners) {
        BOOST_REQUIRE(n.id() > bootstrap_fixture::active_nodes);
    }
}
FIXTURE_TEST(write_configs, bootstrap_fixture) {
    auto replies = write_n(10);
    for (auto& i : replies) {
        info("base:{}, last:{}", i.base_offset, i.last_offset);
    }
    auto cfg = raft::details::read_bootstrap_state(get_log(), _as).get0();
    info(
      "data batches:{}, config batches: {}, data batches:{}, config batches:{}",
      replies[0],
      replies[1],
      cfg.data_batches_seen(),
      cfg.config_batches_seen());

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
    auto cfg = raft::details::read_bootstrap_state(get_log(), _as).get0();

    BOOST_REQUIRE_EQUAL(cfg.data_batches_seen(), 0);
    BOOST_REQUIRE_EQUAL(cfg.config_batches_seen(), 0);
}
