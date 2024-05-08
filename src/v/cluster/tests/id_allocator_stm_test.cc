// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/outcome.h"
#include "cluster/id_allocator_stm.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timeout_clock.h"
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

    // Allocates n IDs, ensuring that each one new ID is greater than the
    // previous one, starting with 'cur_last_id'.
    // Returns the last allocated ID.
    int64_t allocate_n(int64_t cur_last_id, int n) {
        for (int i = 0; i < n; i++) {
            auto result = _stm->allocate_id(1s).get0();

            BOOST_REQUIRE_EQUAL(result.has_value(), true);
            BOOST_REQUIRE_LT(cur_last_id, result.assume_value());

            cur_last_id = result.assume_value();
        }
        return cur_last_id;
    }

    ss::shared_ptr<cluster::id_allocator_stm> _stm;
    scoped_config test_local_cfg;
};

FIXTURE_TEST(stm_monotonicity_test, id_allocator_stm_fixture) {
    create_stm_and_start_raft();
    wait_for_confirmed_leader();

    int64_t last_id = -1;
    allocate_n(last_id, 5);
}

FIXTURE_TEST(stm_restart_test, id_allocator_stm_fixture) {
    create_stm_and_start_raft();
    wait_for_confirmed_leader();

    int64_t last_id = -1;

    last_id = allocate_n(last_id, 5);
    stop_all();
    create_stm_and_start_raft();
    wait_for_confirmed_leader();

    allocate_n(last_id, 5);
}

FIXTURE_TEST(stm_reset_id_test, id_allocator_stm_fixture) {
    create_stm_and_start_raft();
    wait_for_confirmed_leader();
    int64_t last_id = -1;
    allocate_n(last_id, 5);

    // Reset to 100.
    last_id = 100;
    _stm->reset_next_id(last_id + 1, 1s).get();
    last_id = allocate_n(last_id, 5);
    BOOST_REQUIRE_EQUAL(last_id, 105);

    // Even after restarting, the starting point should be where we left off.
    stop_all();
    create_stm_and_start_raft();
    wait_for_confirmed_leader();

    last_id = allocate_n(last_id, 5);
    BOOST_REQUIRE_EQUAL(last_id, 110);
}

FIXTURE_TEST(stm_reset_batch_test, id_allocator_stm_fixture) {
    create_stm_and_start_raft();
    wait_for_confirmed_leader();
    int64_t last_id = -1;
    allocate_n(last_id, 5);

    last_id = 100;
    _stm->reset_next_id(last_id + 1, 1s).get();

    // After a leadership change, the reset should still take effect. However,
    // it should be offset by one batch.
    _raft->step_down("test").get();
    wait_for_confirmed_leader();
    last_id = allocate_n(last_id, 1);
    BOOST_REQUIRE_EQUAL(last_id, 102);
}
