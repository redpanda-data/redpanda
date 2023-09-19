// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/id_allocator_stm.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "outcome.h"
#include "raft/tests/simple_raft_fixture.h"
#include "raft/types.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "storage/record_batch_builder.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"
#include "test_utils/scoped_config.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/log.hh>

#include <boost/range/irange.hpp>
#include <boost/test/tools/old/interface.hpp>

#include <thread>

using namespace std::chrono_literals;

ss::logger idstmlog{"idstm-test"};

struct id_allocator_stm_fixture : simple_raft_fixture {
    void create_stm_and_start_raft() {
        // set configuration parameters
        test_local_cfg.get("id_allocator_batch_size").set_value(int16_t(1));
        test_local_cfg.get("id_allocator_log_capacity").set_value(int16_t(2));
        create_raft();
        raft::state_machine_manager_builder stm_m_builder;

        _stm = stm_m_builder.create_stm<cluster::id_allocator_stm>(
          idstmlog, _raft.get(), config::shard_local_cfg());

        _raft->start(std::move(stm_m_builder)).get();
        _started = true;
    }

    ss::shared_ptr<cluster::id_allocator_stm> _stm;
    scoped_config test_local_cfg;
};

FIXTURE_TEST(stm_monotonicity_test, id_allocator_stm_fixture) {
    create_stm_and_start_raft();
    wait_for_confirmed_leader();

    int64_t last_id = -1;

    for (int i = 0; i < 5; i++) {
        auto result = _stm->allocate_id(1s).get0();

        BOOST_REQUIRE_EQUAL(raft::errc::success, result.raft_status);
        BOOST_REQUIRE_LT(last_id, result.id);

        last_id = result.id;
    }
}

FIXTURE_TEST(stm_restart_test, id_allocator_stm_fixture) {
    create_stm_and_start_raft();
    wait_for_confirmed_leader();

    int64_t last_id = -1;

    for (int i = 0; i < 5; i++) {
        auto result = _stm->allocate_id(1s).get0();

        BOOST_REQUIRE_EQUAL(raft::errc::success, result.raft_status);
        BOOST_REQUIRE_LT(last_id, result.id);

        last_id = result.id;
    }
    stop_all();
    create_stm_and_start_raft();
    wait_for_confirmed_leader();

    for (int i = 0; i < 5; i++) {
        auto result = _stm->allocate_id(1s).get0();

        BOOST_REQUIRE_EQUAL(raft::errc::success, result.raft_status);
        BOOST_REQUIRE_LT(last_id, result.id);

        last_id = result.id;
    }
}
