#include "model/fundamental.h"
#include "model/metadata.h"
#include "raft/configuration_manager.h"
#include "raft/logger.h"
#include "raft/types.h"
#include "random/generators.h"
#include "storage/api.h"
#include "storage/kvstore.h"
#include "storage/log_manager.h"
#include "storage/types.h"
#include "test_utils/fixture.h"
#include "test_utils/randoms.h"
#include "units.h"

#include <seastar/core/abort_source.hh>

#include <boost/test/tools/old/interface.hpp>

#include <chrono>
#include <vector>

using namespace std::chrono_literals; // NOLINT

struct config_manager_fixture {
    config_manager_fixture()
      : _storage(storage::api(
        std::move(storage::kvstore_config(
          100_MiB,
          std::chrono::milliseconds(10),
          base_dir,
          storage::debug_sanitize_files::yes)),
        std::move(storage::log_config(
          storage::log_config::storage_type::disk,
          base_dir,
          100_MiB,
          storage::debug_sanitize_files::yes))))
      , _logger(model::node_id(0), raft::group_id(1))
      , _cfg_mgr(
          raft::group_configuration{}, raft::group_id(1), _storage, _logger) {
        _storage.start().get0();
    }

    ss::sstring base_dir = "test_cfg_manager_"
                           + random_generators::gen_alphanum_string(6);
    storage::api _storage;
    raft::ctx_log _logger;
    raft::configuration_manager _cfg_mgr;

    ~config_manager_fixture() { _storage.stop().get0(); }

    raft::group_configuration random_configuration() {
        std::vector<model::broker> nodes;
        nodes.reserve(3);
        std::vector<model::broker> learners;
        learners.reserve(2);
        for (auto i = 0; i < 3; ++i) {
            nodes.push_back(tests::random_broker(i, i));
        }
        for (auto i = 0; i < 2; ++i) {
            learners.push_back(tests::random_broker(i, i));
        }
        return raft::group_configuration{std::move(nodes), std::move(learners)};
    }

    raft::group_configuration add_random_cfg(model::offset offset) {
        auto cfg = random_configuration();
        _cfg_mgr.add(offset, cfg).get0();
        return cfg;
    }

    std::vector<raft::group_configuration> test_configurations() {
        std::vector<raft::group_configuration> configurations;
        configurations.reserve(5);
        configurations.push_back(add_random_cfg(model::offset(0)));
        configurations.push_back(add_random_cfg(model::offset(20)));
        configurations.push_back(add_random_cfg(model::offset(33)));
        configurations.push_back(add_random_cfg(model::offset(34)));
        configurations.push_back(add_random_cfg(model::offset(60)));
        configurations.push_back(add_random_cfg(model::offset(1254)));
        return configurations;
    }

    void validate_recovery() {
        raft::configuration_manager recovered(
          raft::group_configuration{}, raft::group_id(1), _storage, _logger);

        recovered.start().get0();

        BOOST_REQUIRE_EQUAL(
          recovered.get_highest_known_offset(),
          _cfg_mgr.get_highest_known_offset());

        // compare recovered with original manager
        for (int i = 0; i < 2000; ++i) {
            auto expected = _cfg_mgr.get(model::offset(i));
            auto have = _cfg_mgr.get(model::offset(i));
            BOOST_REQUIRE_EQUAL(expected.has_value(), have.has_value());
            if (expected.has_value()) {
                BOOST_REQUIRE_EQUAL(expected->nodes, have->nodes);
            }
        }
    }
};

FIXTURE_TEST(test_getting_configurations, config_manager_fixture) {
    auto configurations = test_configurations();

    BOOST_REQUIRE_EQUAL(_cfg_mgr.get_latest().nodes, configurations[5].nodes);

    BOOST_REQUIRE_EQUAL(
      _cfg_mgr.get(model::offset(0))->nodes, configurations[0].nodes);
    BOOST_REQUIRE_EQUAL(
      _cfg_mgr.get(model::offset(1))->nodes, configurations[0].nodes);
    BOOST_REQUIRE_EQUAL(
      _cfg_mgr.get(model::offset(19))->nodes, configurations[0].nodes);
    BOOST_REQUIRE_EQUAL(
      _cfg_mgr.get(model::offset(20))->nodes, configurations[1].nodes);
    BOOST_REQUIRE_EQUAL(
      _cfg_mgr.get(model::offset(32))->nodes, configurations[1].nodes);
    BOOST_REQUIRE_EQUAL(
      _cfg_mgr.get(model::offset(33))->nodes, configurations[2].nodes);
    BOOST_REQUIRE_EQUAL(
      _cfg_mgr.get(model::offset(34))->nodes, configurations[3].nodes);
    BOOST_REQUIRE_EQUAL(
      _cfg_mgr.get(model::offset(1000000))->nodes, configurations[5].nodes);

    BOOST_REQUIRE_EQUAL(
      _cfg_mgr.get_highest_known_offset(), model::offset(1254));
    validate_recovery();
    _cfg_mgr
      .maybe_store_highest_known_offset(
        model::offset(10000),
        raft::configuration_manager::offset_update_treshold + 1_KiB)
      .get0();

    validate_recovery();
    BOOST_REQUIRE_EQUAL(
      _cfg_mgr.get_highest_known_offset(), model::offset(10000));
}

FIXTURE_TEST(test_prefix_truncation, config_manager_fixture) {
    auto configurations = test_configurations();

    _cfg_mgr.prefix_truncate(model::offset(20)).get0();
    _cfg_mgr.prefix_truncate(model::offset(0)).get0();

    BOOST_REQUIRE_EQUAL(
      _cfg_mgr.get(model::offset(20))->nodes, configurations[1].nodes);
    BOOST_REQUIRE(_cfg_mgr.get(model::offset(10)).has_value() == false);
    BOOST_REQUIRE(_cfg_mgr.get(model::offset(19)).has_value() == false);

    _cfg_mgr.prefix_truncate(model::offset(21)).get0();
    validate_recovery();

    BOOST_REQUIRE_EQUAL(_cfg_mgr.get(model::offset(20)).has_value(), false);
    BOOST_REQUIRE_EQUAL(
      _cfg_mgr.get(model::offset(33))->nodes, configurations[2].nodes);

    validate_recovery();
    // try to truncate whole
    BOOST_CHECK_THROW(
      _cfg_mgr.prefix_truncate(model::offset(3003)).get0(),
      std::invalid_argument);
}

FIXTURE_TEST(test_truncation, config_manager_fixture) {
    auto configurations = test_configurations();

    _cfg_mgr.truncate(model::offset(34)).get0();
    _cfg_mgr.truncate(model::offset(50)).get0();
    BOOST_REQUIRE_EQUAL(
      _cfg_mgr.get(model::offset(0))->nodes, configurations[0].nodes);
    BOOST_REQUIRE_EQUAL(
      _cfg_mgr.get(model::offset(1))->nodes, configurations[0].nodes);
    BOOST_REQUIRE_EQUAL(
      _cfg_mgr.get(model::offset(19))->nodes, configurations[0].nodes);
    BOOST_REQUIRE_EQUAL(
      _cfg_mgr.get(model::offset(20))->nodes, configurations[1].nodes);
    BOOST_REQUIRE_EQUAL(
      _cfg_mgr.get(model::offset(32))->nodes, configurations[1].nodes);
    BOOST_REQUIRE_EQUAL(
      _cfg_mgr.get(model::offset(33))->nodes, configurations[2].nodes);
    BOOST_REQUIRE_EQUAL(
      _cfg_mgr.get(model::offset(35))->nodes, configurations[2].nodes);
    BOOST_REQUIRE_EQUAL(
      _cfg_mgr.get(model::offset(60))->nodes, configurations[2].nodes);

    validate_recovery();

    // prefix truncate
    _cfg_mgr.prefix_truncate(model::offset(33)).get0();
    // try to truncate all configurations
    BOOST_CHECK_THROW(
      _cfg_mgr.truncate(model::offset(33)).get0(), std::invalid_argument);
}

FIXTURE_TEST(test_waitng_for_change, config_manager_fixture) {
    ss::abort_source as;
    auto f = _cfg_mgr.wait_for_change(model::offset(21), as);
    auto not_completed = _cfg_mgr.wait_for_change(model::offset(35000), as);
    auto configurations = test_configurations();
    auto res = f.get0();
    BOOST_REQUIRE(res.offset > model::offset(21));
    BOOST_REQUIRE_EQUAL(res.cfg.nodes, _cfg_mgr.get(res.offset)->nodes);
    BOOST_REQUIRE_EQUAL(res.cfg.learners, _cfg_mgr.get(res.offset)->learners);
    as.request_abort();
    BOOST_CHECK_THROW(
      auto res = not_completed.get0(), ss::abort_requested_exception);
}

FIXTURE_TEST(test_start_write_concurrency, config_manager_fixture) {
    // store some configurations
    auto configurations = test_configurations();

    raft::configuration_manager new_cfg_manager(
      raft::group_configuration{}, raft::group_id(1), _storage, _logger);

    auto start = new_cfg_manager.start();
    auto cfg = random_configuration();
    auto add = new_cfg_manager.add(model::offset(3000), cfg);
    configurations.push_back(cfg);

    std::vector<ss::future<>> futures;
    futures.reserve(2);
    futures.push_back(std::move(start));
    futures.push_back(std::move(add));

    ss::when_all(futures.begin(), futures.end()).get0();

    BOOST_REQUIRE_EQUAL(new_cfg_manager.get_latest().nodes, cfg.nodes);
    BOOST_REQUIRE_EQUAL(new_cfg_manager.get_latest().learners, cfg.learners);
    BOOST_REQUIRE_EQUAL(
      new_cfg_manager.get_highest_known_offset(), model::offset(3000));
}
