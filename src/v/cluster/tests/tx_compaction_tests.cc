#include "cluster/rm_stm.h"
#include "config/config_store.h"
#include "raft/tests/mux_state_machine_fixture.h"
#include "raft/tests/raft_group_fixture.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "tx_compaction_utils.h"

#include <seastar/util/defer.hh>

static ss::logger test_logger{"tx_compaction_tests"};

using cluster::random_tx_generator;

#define STM_BOOTSTRAP()                                                        \
    storage::ntp_config::default_overrides o;                                  \
    o.cleanup_policy_bitflags = model::cleanup_policy_bitflags::compaction;    \
    start_raft(o);                                                             \
    ss::sharded<cluster::tx_gateway_frontend> tx_gateway_frontend;             \
    ss::sharded<features::feature_table> feature_table;                        \
    config::config_store store;                                                \
    config::bounded_property<uint64_t> max_saved_pids_count(                   \
      store,                                                                   \
      "max_saved_pids_count",                                                  \
      "Max pids count inside rm_stm states",                                   \
      {.needs_restart = config::needs_restart::no,                             \
       .visibility = config::visibility::user},                                \
      std::numeric_limits<uint64_t>::max(),                                    \
      {.min = 1});                                                             \
    feature_table.start().get0();                                              \
    auto stm = ss::make_shared<cluster::rm_stm>(                               \
      test_logger,                                                             \
      _raft.get(),                                                             \
      tx_gateway_frontend,                                                     \
      feature_table,                                                           \
      max_saved_pids_count.bind());                                            \
    stm->testing_only_disable_auto_abort();                                    \
    stm->start().get0();                                                       \
    auto stop = ss::defer([&] {                                                \
        _data_dir = "test_dir_" + random_generators::gen_alphanum_string(6);   \
        stm->stop().get0();                                                    \
        feature_table.stop().get0();                                           \
        stop_all();                                                            \
    });                                                                        \
    wait_for_confirmed_leader();                                               \
    wait_for_meta_initialized();                                               \
    auto log = _storage.local().log_mgr().get(_raft->ntp());                   \
    log->stm_manager()->add_stm(stm);                                          \
    BOOST_REQUIRE(log);

FIXTURE_TEST(test_tx_compaction_combinations, mux_state_machine_fixture) {
    // This generates very interesting interleaved and non interleaved
    // transaction scopes with single and multi segment transactions. We
    // Validate that the resulting output segment file has all the aborted
    // batches and tx control batches removed.
    // Each workload execution can fully be backtracked from the test log
    // (in case of failures) and re-executed manually.
    for (auto num_tx : {10, 20, 30}) {
        for (auto num_rolls : {0, 1, 2, 3, 5}) {
            for (auto type :
                 {random_tx_generator::tx_types::commit_only,
                  random_tx_generator::tx_types::abort_only,
                  random_tx_generator::mixed}) {
                for (auto interleave : {true, false}) {
                    {
                        random_tx_generator::spec spec{
                          ._num_txes = num_tx,
                          ._num_rolls = num_rolls,
                          ._types = type,
                          ._interleave = interleave};
                        STM_BOOTSTRAP();
                        vlog(test_logger.info, "Running spec: {}", spec);
                        random_tx_generator{}.run_workload(
                          spec, _raft->term(), stm, log);
                        vlog(test_logger.info, "Finished spec: {}", spec);
                    }
                }
            }
        }
    }
}
