// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/fundamental.h"
#include "random/generators.h"
#include "storage/file_sanitizer.h"
#include "storage/opfuzz/opfuzz.h"
#include "storage/tests/storage_test_fixture.h"
#include "storage/types.h"
#include "test_utils/test_macros.h"

#include <seastar/util/defer.hh>
#include <seastar/util/log.hh>

#include <gtest/gtest.h>

#include <algorithm>
#include <iterator>
#include <optional>

inline model::cleanup_policy_bitflags randcompaction() {
    auto c = model::cleanup_policy_bitflags::compaction;
    if (random_generators::get_int(0, 100) > 50) {
        c |= model::cleanup_policy_bitflags::deletion;
    }
    return c;
}

TEST_F(storage_test_fixture, test_random_workload) {
    // BLOCK on logging so that we can make sense of the logs
    std::cout.setf(std::ios::unitbuf);
    storage::log_manager mngr = make_log_manager(
      storage::log_config(
        std::move(test_dir), 200_MiB, storage::with_cache::yes));
    auto deferred = ss::defer([&mngr]() mutable { mngr.stop().get(); });

    // Test parameters
    const size_t ntp_count = 4;
    const size_t ops_per_ntp = 500;
    std::vector<std::unique_ptr<storage::opfuzz>> logs_to_fuzz;
    logs_to_fuzz.reserve(ntp_count);
    SUCCEED() << "generating ntp's " << ntp_count << ", with " << ops_per_ntp
              << " ops";
    for (size_t i = 0; i < ntp_count; ++i) {
        auto ntp = model::ntp(
          "test.default",
          "topic." + random_generators::gen_alphanum_string(3),
          i);
        auto overrides
          = std::make_unique<storage::ntp_config::default_overrides>(
            storage::ntp_config::default_overrides{
              .cleanup_policy_bitflags = randcompaction(),
              .compaction_strategy = model::compaction_strategy::offset,
            });

        auto cfg = storage::ntp_config(ntp, mngr.config().base_dir);
        if (random_generators::get_int(1, 100) < 50) {
            cfg = storage::ntp_config(
              ntp, mngr.config().base_dir, std::move(overrides));
        }
        auto log = mngr.manage(std::move(cfg)).get();
        log->stm_hookset()->start();
        logs_to_fuzz.emplace_back(
          std::make_unique<storage::opfuzz>(std::move(log), ops_per_ntp));
    }
    auto stop_stms = ss::defer([&logs_to_fuzz] {
        for (auto& w : logs_to_fuzz) {
            w->log()->stm_hookset()->stop();
        }
    });
    // Execute NTP workloads in parallel
    ss::parallel_for_each(
      logs_to_fuzz,
      [](std::unique_ptr<storage::opfuzz>& w) {
          return w->execute().handle_exception([&w](std::exception_ptr e) {
              vunreachable("Error:{} fuzzing log: {}", e, w->log());
          });
      })
      .get();
}
TEST_F(storage_test_fixture, test_random_remove) {
    // BLOCK on logging so that we can make sense of the logs
    std::cout.setf(std::ios::unitbuf);
    storage::log_manager mngr = make_log_manager(
      storage::log_config(
        std::move(test_dir), 200_MiB, storage::with_cache::yes));
    auto deferred = ss::defer([&mngr]() mutable { mngr.stop().get(); });

    // Test parameters
    const size_t ntp_count = 10;
    const size_t ops_per_ntp = 10;
    std::vector<std::unique_ptr<storage::opfuzz>> logs_to_fuzz;
    std::vector<model::ntp> ntps_to_fuzz;
    logs_to_fuzz.reserve(ntp_count);
    ntps_to_fuzz.reserve(ntp_count);

    for (size_t i = 0; i < ntp_count; ++i) {
        auto ntp = model::ntp(
          "test.default",
          "topic." + random_generators::gen_alphanum_string(3),
          i);
        ntps_to_fuzz.emplace_back(ntp);
    }
    for (const auto& ntp : ntps_to_fuzz) {
        auto directory = ssx::sformat(
          "{}/{}", mngr.config().base_dir, ntp.path());
        auto log = mngr.manage(storage::ntp_config(ntp, directory)).get();
        log->stm_hookset()->start();
        logs_to_fuzz.emplace_back(
          std::make_unique<storage::opfuzz>(std::move(log), ops_per_ntp));
    }

    // Execute NTP workloads in parallel
    ss::parallel_for_each(
      logs_to_fuzz,
      [](std::unique_ptr<storage::opfuzz>& w) {
          return w->execute().handle_exception([&w](std::exception_ptr e) {
              vunreachable("Error:{} fuzzing log: {}", e, w->log());
          });
      })
      .get();

    // Stop stm_managers before removing logs
    for (auto& w : logs_to_fuzz) {
        w->log()->stm_hookset()->stop();
    }

    std::vector<size_t> random_ntp_removal_sequence;
    std::generate_n(
      std::back_inserter(random_ntp_removal_sequence), ntp_count, [] {
          // generate *inclusive* indices
          return random_generators::get_int<size_t>(0, ntp_count - 1);
      });
    SUCCEED() << "Removal sequence: " << random_ntp_removal_sequence;
    for (auto i : random_ntp_removal_sequence) {
        const model::ntp& ntp = ntps_to_fuzz[i];
        SUCCEED() << "test... removing: " << ntp;
        mngr.remove(ntp).get();
        ASSERT_FALSE(mngr.get(ntp));
    }
    std::sort(
      random_ntp_removal_sequence.begin(), random_ntp_removal_sequence.end());
    ASSERT_EQ(
      ntp_count
        - std::distance(
          random_ntp_removal_sequence.begin(),
          std::unique(
            random_ntp_removal_sequence.begin(),
            random_ntp_removal_sequence.end())),
      mngr.size());
}
