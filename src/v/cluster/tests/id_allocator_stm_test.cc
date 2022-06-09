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
#include "raft/tests/mux_state_machine_fixture.h"
#include "raft/types.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "storage/record_batch_builder.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"

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

FIXTURE_TEST(stm_monotonicity_test, mux_state_machine_fixture) {
    start_raft();

    config::configuration cfg;
    cfg.id_allocator_batch_size.set_value(int16_t(1));
    cfg.id_allocator_log_capacity.set_value(int16_t(2));

    cluster::id_allocator_stm stm(idstmlog, _raft.get(), cfg);

    stm.start().get0();
    auto stop = ss::defer([&stm] { stm.stop().get0(); });

    wait_for_confirmed_leader();

    int64_t last_id = -1;

    for (int i = 0; i < 5; i++) {
        auto result = stm.allocate_id(1s).get0();

        BOOST_REQUIRE_EQUAL(raft::errc::success, result.raft_status);
        BOOST_REQUIRE_LT(last_id, result.id);

        last_id = result.id;
    }
}

FIXTURE_TEST(stm_restart_test, mux_state_machine_fixture) {
    start_raft();

    config::configuration cfg;
    cfg.id_allocator_batch_size.set_value(int16_t(1));
    cfg.id_allocator_log_capacity.set_value(int16_t(2));

    cluster::id_allocator_stm stm1(idstmlog, _raft.get(), cfg);
    stm1.start().get0();
    wait_for_confirmed_leader();

    int64_t last_id = -1;

    for (int i = 0; i < 5; i++) {
        auto result = stm1.allocate_id(1s).get0();

        BOOST_REQUIRE_EQUAL(raft::errc::success, result.raft_status);
        BOOST_REQUIRE_LT(last_id, result.id);

        last_id = result.id;
    }
    stm1.stop().get0();

    cluster::id_allocator_stm stm2(idstmlog, _raft.get(), cfg);
    stm2.start().get0();
    for (int i = 0; i < 5; i++) {
        auto result = stm2.allocate_id(1s).get0();

        BOOST_REQUIRE_EQUAL(raft::errc::success, result.raft_status);
        BOOST_REQUIRE_LT(last_id, result.id);

        last_id = result.id;
    }
    stm2.stop().get0();
}
