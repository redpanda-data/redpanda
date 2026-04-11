// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/adl_serde.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "raft/configuration_bootstrap_state.h"
#include "raft/consensus.h"
#include "raft/consensus_utils.h"
#include "raft/group_configuration.h"
#include "storage/api.h"
#include "storage/log.h"
#include "storage/log_manager.h"
#include "storage/record_batch_builder.h"
// testing
#include "raft/tests/simple_record_fixture.h"
#include "test_utils/boost_fixture.h"
#include "test_utils/test_env.h"

#include <seastar/core/print.hh>
#include <seastar/util/log.hh>

using namespace std::chrono_literals; // NOLINT

ss::sstring test_directory() { return test_env::random_dir_path(); }

struct bootstrap_fixture : raft::simple_record_fixture {
    using raft::simple_record_fixture::active_nodes;
    bootstrap_fixture()
      : _storage(
          []() {
              return storage::kvstore_config(
                1_MiB,
                config::mock_binding(10ms),
                test_directory(),
                storage::make_sanitized_file_config());
          },
          []() {
              return storage::log_config(
                test_directory(),
                1_GiB,
                storage::with_cache::no,
                storage::make_sanitized_file_config());
          },
          _feature_table) {
        _feature_table.start().get();
        _feature_table
          .invoke_on_all(
            [](features::feature_table& f) { f.testing_activate_all(); })
          .get();
        _storage.start().get();
        auto log = _storage.log_mgr()
                     .manage(storage::ntp_config(_ntp, test_directory()))
                     .get();
        log->stm_hookset()->start();
    }

    std::vector<storage::append_result> write_n(const std::size_t n) {
        const auto cfg = storage::log_append_config{
          storage::log_append_config::fsync::no, model::no_timeout};
        std::vector<storage::append_result> res;
        res.push_back(
          datas(n)
            .for_each_ref(get_log()->make_appender(cfg), cfg.timeout)
            .get());
        res.push_back(
          configs(n, raft::group_configuration::v_6)
            .for_each_ref(get_log()->make_appender(cfg), cfg.timeout)
            .get());
        get_log()->flush().get();
        return res;
    }
    ss::shared_ptr<storage::log> get_log() {
        return _storage.log_mgr().get(_ntp);
    }

    ~bootstrap_fixture() {
        get_log()->stm_hookset()->stop();
        _storage.stop().get();
        _feature_table.stop().get();
    }

    seastar::logger _test_logger{"bootstrap-test-logger"};
    ss::sharded<features::feature_table> _feature_table;
    storage::api _storage;
    ss::abort_source _as;
};

FIXTURE_TEST(write_configs, bootstrap_fixture) {
    auto replies = write_n(10);
    for (auto& i : replies) {
        info("base:{}, last:{}", i.base_offset, i.last_offset);
    }
    auto cfg = raft::details::read_bootstrap_state(
                 get_log(), model::offset(0), _as)
                 .get();
    info(
      "data batches:{}, config batches: {}, data batches:{}, config batches:{}",
      replies[0],
      replies[1],
      cfg.data_batches_seen(),
      cfg.config_batches_seen());

    BOOST_REQUIRE_EQUAL(cfg.data_batches_seen(), 10);
    BOOST_REQUIRE_EQUAL(cfg.config_batches_seen(), 10);
}
FIXTURE_TEST(mixed_config_versions, bootstrap_fixture) {
    const storage::log_append_config append_cfg{
      storage::log_append_config::fsync::no, model::no_timeout};

    datas(20)
      .for_each_ref(get_log()->make_appender(append_cfg), append_cfg.timeout)
      .get();
    configs(3, raft::group_configuration::v_4)
      .for_each_ref(get_log()->make_appender(append_cfg), append_cfg.timeout)
      .get();
    configs(2, raft::group_configuration::v_5)
      .for_each_ref(get_log()->make_appender(append_cfg), append_cfg.timeout)
      .get();
    configs(5, raft::group_configuration::v_6)
      .for_each_ref(get_log()->make_appender(append_cfg), append_cfg.timeout)
      .get();
    get_log()->flush().get();

    auto state = raft::details::read_bootstrap_state(
                   get_log(), model::offset(0), _as)
                   .get();

    // 20 data batches
    BOOST_REQUIRE_EQUAL(state.data_batches_seen(), 20);
    // 10 configuration batches
    BOOST_REQUIRE_EQUAL(state.config_batches_seen(), 10);

    for (size_t i = 0; i < state.configurations().size(); ++i) {
        auto& current_cfg = state.configurations()[i];
        info("i: {}, cfg_version: {}", i, current_cfg.cfg.version());
        if (i < 3) {
            BOOST_REQUIRE_EQUAL(
              current_cfg.cfg.version(), raft::group_configuration::v_4);
        } else if (i >= 3 && i < 5) {
            BOOST_REQUIRE_EQUAL(
              current_cfg.cfg.version(), raft::group_configuration::v_5);
        } else {
            BOOST_REQUIRE_EQUAL(
              current_cfg.cfg.version(), raft::group_configuration::v_6);
        }
    }
}

FIXTURE_TEST(empty_log, bootstrap_fixture) {
    auto cfg = raft::details::read_bootstrap_state(
                 get_log(), model::offset(0), _as)
                 .get();

    BOOST_REQUIRE_EQUAL(cfg.data_batches_seen(), 0);
    BOOST_REQUIRE_EQUAL(cfg.config_batches_seen(), 0);
}
