// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/rm_stm.h"
#include "cluster/tests/rm_stm_test_fixture.h"
#include "cluster/tests/tx_compaction_utils.h"
#include "test_utils/boost_fixture.h"
#include "test_utils/scoped_config.h"
#include "test_utils/test_env.h"

#include <seastar/util/defer.hh>

static ss::logger test_logger{"tx_compaction_tests"};

using cluster::tx_executor;

#define STM_BOOTSTRAP()                                                        \
    storage::ntp_config::default_overrides o{                                  \
      .retention_time = tristate<std::chrono::milliseconds>(10s),              \
      .segment_ms = tristate<std::chrono::milliseconds>(1s)};                  \
    o.cleanup_policy_bitflags = model::cleanup_policy_bitflags::compaction;    \
                                                                               \
    create_stm_and_start_raft(o);                                              \
    auto stm = _stm;                                                           \
    stm->testing_only_disable_auto_abort();                                    \
    auto stop = ss::defer([&] {                                                \
        _data_dir = test_env::random_dir_path();                               \
        stop_all();                                                            \
        producer_state_manager.stop().get();                                   \
        producer_expiration_ms.stop().get();                                   \
        max_concurent_producers.stop().get();                                  \
        _stm = nullptr;                                                        \
    });                                                                        \
    wait_for_confirmed_leader();                                               \
    wait_for_meta_initialized();                                               \
    auto log = _storage.local().log_mgr().get(_raft->ntp());                   \
    log->stm_hookset()->add_stm(stm);                                          \
    BOOST_REQUIRE(log);

FIXTURE_TEST(test_tx_compaction_combinations, rm_stm_test_fixture) {
    // This generates very interesting interleaved and non interleaved
    // transaction scopes with single and multi segment transactions. We
    // Validate that the resulting output segment file has all the aborted
    // batches and tx control batches removed.
    // Each workload execution can fully be backtracked from the test log
    // (in case of failures) and re-executed manually.
    scoped_config cfg;
    cfg.get("log_disable_housekeeping_for_tests").set_value(true);
    cfg.get("log_segment_ms_min").set_value(1ms);
    cfg.get("log_compaction_tx_batch_removal_enabled").set_value(true);
    for (auto num_tx : {10, 20, 30}) {
        for (auto num_rolls : {0, 1, 2, 3, 5}) {
            for (auto type :
                 {tx_executor::tx_types::commit_only,
                  tx_executor::tx_types::abort_only,
                  tx_executor::mixed}) {
                for (auto interleave : {true, false}) {
                    {
                        tx_executor::spec spec{
                          ._num_txes = num_tx,
                          ._num_rolls = num_rolls,
                          ._types = type,
                          ._interleave = interleave};
                        STM_BOOTSTRAP();
                        vlog(test_logger.info, "Running spec: {}", spec);
                        tx_executor{}.run_random_workload(
                          spec, _raft->term(), stm, log);
                        vlog(test_logger.info, "Finished spec: {}", spec);
                    }
                }
            }
        }
    }
}
