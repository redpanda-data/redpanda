/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/translation/scheduling_policies.h"
#include "datalake/translation/tests/scheduler_fixture.h"
#include "test_utils/async.h"
#include "test_utils/randoms.h"

#include <seastar/core/sleep.hh>

using namespace datalake::translation::scheduling;

class fair_scheduling_policy_fixture_parameterized
  : public scheduler_fixture
  , public testing::WithParamInterface<std::tuple<
      size_t,                       // total translators
      size_t,                       // concurrent translators
      size_t,                       // total memory
      size_t,                       // memory block size
      size_t,                       // translator throughput
      std::chrono::milliseconds>> { // scheduler time quota
public:
    [[nodiscard]] static size_t total_translators() {
        return std::get<0>(GetParam());
    }

    [[nodiscard]] static config::binding<size_t> max_concurrent_translators() {
        return config::mock_binding<size_t>(std::get<1>(GetParam()));
    }

    [[nodiscard]] static size_t total_memory() {
        return std::get<2>(GetParam());
    }

    [[nodiscard]] static size_t memory_block_size() {
        return std::get<3>(GetParam());
    }

    [[nodiscard]] static size_t translator_throughput() {
        return std::get<4>(GetParam());
    }

    [[nodiscard]] static clock::duration time_quota() {
        return std::chrono::duration_cast<clock::duration>(
          std::get<5>(GetParam()));
    }

    ss::lw_shared_ptr<scheduler> make_scheduler() override {
        return ss::make_lw_shared<scheduler>(
          total_memory(),
          memory_block_size(),
          make_scheduling_policy(),
          _disk_manager);
    }

    std::unique_ptr<scheduling_policy> make_scheduling_policy() override {
        return std::make_unique<fair_scheduling_policy>(
          max_concurrent_translators(), time_quota());
    }
};

TEST_P_CORO(fair_scheduling_policy_fixture_parameterized, test_fairness) {
    // The test ensures that a translator with a very small lag does not starve
    // a translator with a large lag.
    static constexpr auto small_target_lag = 10ms;
    static constexpr auto large_target_lag = 1min;
    for (size_t i = 0; i < total_translators(); i++) {
        co_await _scheduler->add_translator(make_normal_translator(
          {.max_target_lag = tests::random_bool() ? small_target_lag
                                                  : large_target_lag,
           .translation_throughput_bytes_per_sec = translator_throughput(),
           .num_concurrent_writers = 1}));
    }
    RPTEST_REQUIRE_EVENTUALLY_CORO(120s, [this]() {
        const auto& translators = _scheduler->all_translators();
        return std::all_of(
          translators.begin(), translators.end(), [](const auto& it) {
              return it.second.translations_scheduled() > 0;
          });
    });
}

INSTANTIATE_TEST_SUITE_P(
  scheduler_fairness,
  fair_scheduling_policy_fixture_parameterized,
  testing::Combine(
    testing::Values(100),            // total translators
    testing::Values(4),              // concurrent translators
    testing::Values(8_MiB, 100_MiB), // total memory
    testing::Values(4_MiB),          // block size
    testing::Values(1_MiB),          // translator throughput/s
    testing::Values(10ms)            // scheduler time slice
    ),
  [](
    const testing::TestParamInfo<
      fair_scheduling_policy_fixture_parameterized::ParamType>& info) {
      auto& params = info.param;
      return fmt::format(
        "translators_{}_parallel_{}_mem_{}_bs_{}_tput_{}_slice_{}",
        std::get<0>(params),
        std::get<1>(params),
        std::get<2>(params),
        std::get<3>(params),
        std::get<4>(params),
        std::get<5>(params));
  });

class simple_fair_scheduling_policy_fixture : public scheduler_fixture {
    std::unique_ptr<scheduling_policy> make_scheduling_policy() override {
        return std::make_unique<fair_scheduling_policy>(
          config::mock_binding(max_translators), 10ms);
    }

protected:
    static constexpr size_t max_translators = 10;
};

TEST_F_CORO(simple_fair_scheduling_policy_fixture, test_happy_path) {
    // The test ensures that in a happy path, when the total number of
    // translators are <= limit, each of them gets scheduled immediately.
    // without any contention
    static constexpr auto small_target_lag = 10ms;
    static constexpr auto large_target_lag = 1min;
    for (size_t i = 0; i < max_translators; i++) {
        co_await _scheduler->add_translator(make_normal_translator(
          {.max_target_lag = tests::random_bool() ? small_target_lag
                                                  : large_target_lag,
           .translation_throughput_bytes_per_sec = 1_MiB,
           .num_concurrent_writers = 1}));
    }
    // With no contention every translator should be scheduled repeatedly and
    // accrue running time. Running time per translation is dominated by the
    // mock's sleeps, not the 10ms quota: the running window spans
    // translate + maybe_checkpoint + notify_done, and notify_done alone sleeps
    // 100ms every cycle.
    //   - large-lag (1min): never checkpoints during the test
    //     -> ~110ms/translation
    //   - small-lag (10ms): checkpoints every cycle (+100ms)
    //     -> ~210ms/translation
    // A large-lag translator reaches >12s running time at ~110 translations
    // (~14s wall); a small-lag translator reaches >100 translations at ~23s
    // wall. Both are well under the 45s deadline.
    static constexpr size_t expected_translations = 100;
    static constexpr auto expected_runtime
      = std::chrono::duration_cast<clock::duration>(12s);
    RPTEST_REQUIRE_EVENTUALLY_CORO(45s, [this]() {
        const auto& translators = _scheduler->all_translators();
        return std::all_of(
          translators.begin(), translators.end(), [](const auto& it) {
              return it.second.translations_scheduled() > expected_translations
                     && it.second.total_running_time() > expected_runtime;
          });
    });
}
