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
#include "random/generators.h"
#include "resource_mgmt/io_priority.h"
#include "storage/api.h"
#include "storage/log.h"
#include "storage/log_manager.h"
#include "storage/record_batch_builder.h"
#include "test_utils/randoms.h"
// testing
#include "raft/tests/simple_record_fixture.h"
#include "test_utils/fixture.h"

#include <seastar/core/print.hh>
#include <seastar/util/log.hh>

using namespace std::chrono_literals; // NOLINT

struct bootstrap_fixture : raft::simple_record_fixture {
    using raft::simple_record_fixture::active_nodes;
    bootstrap_fixture()
      : _storage(
        []() {
            return storage::kvstore_config(
              1_MiB,
              config::mock_binding(10ms),
              "test.dir",
              storage::debug_sanitize_files::yes);
        },
        []() {
            return storage::log_config(
              "test.dir",
              1_GiB,
              storage::debug_sanitize_files::yes,
              ss::default_priority_class(),
              storage::with_cache::no);
        },
        _feature_table) {
        _feature_table.start().get();
        _feature_table
          .invoke_on_all(
            [](features::feature_table& f) { f.testing_activate_all(); })
          .get();
        _storage.start().get();
        // ignore the get_log()
        (void)_storage.log_mgr()
          .manage(storage::ntp_config(_ntp, "test.dir"))
          .get0();
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
    storage::log get_log() { return _storage.log_mgr().get(_ntp).value(); }

    ~bootstrap_fixture() {
        _storage.stop().get();
        _feature_table.stop().get();
    }

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
                 .get0();
    info(
      "data batches:{}, config batches: {}, data batches:{}, config batches:{}",
      replies[0],
      replies[1],
      cfg.data_batches_seen(),
      cfg.config_batches_seen());

    cfg.configurations().back().cfg.for_each_voter([](raft::vnode rni) {
        BOOST_REQUIRE(
          rni.id() >= 0 && rni.id() <= bootstrap_fixture::active_nodes);
    });

    cfg.configurations().back().cfg.for_each_learner([](raft::vnode rni) {
        BOOST_REQUIRE(rni.id() > bootstrap_fixture::active_nodes);
    });

    BOOST_REQUIRE_EQUAL(cfg.data_batches_seen(), 10);
    BOOST_REQUIRE_EQUAL(cfg.config_batches_seen(), 10);
}

FIXTURE_TEST(empty_log, bootstrap_fixture) {
    auto cfg = raft::details::read_bootstrap_state(
                 get_log(), model::offset(0), _as)
                 .get0();

    BOOST_REQUIRE_EQUAL(cfg.data_batches_seen(), 0);
    BOOST_REQUIRE_EQUAL(cfg.config_batches_seen(), 0);
}
