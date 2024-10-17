/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "test_utils/gtest_utils.h"

#include "base/vassert.h"

#include <seastar/core/lowres_clock.hh>

#include <fmt/format.h>
#include <gtest/gtest.h>

namespace {
int gtest_iteration = 0;
} // anonymous namespace

void rp_test_listener::OnTestIterationStart(
  const ::testing::UnitTest& /*unit_test*/, int iteration) {
    gtest_iteration = iteration;
}

void rp_test_listener::OnTestPartResult(
  const ::testing::TestPartResult& result) {
    if (result.type() == testing::TestPartResult::kFatalFailure) {
        throw testing::AssertionException(result);
    }
}

ss::sstring get_test_directory() {
    const auto* test_info
      = ::testing::UnitTest::GetInstance()->current_test_info();
    vassert(test_info != nullptr, "Must be a gtest!");

    // The current timestamp uniquely identifies the process' test incantation,
    // and the test iteration uniquely identifies the individual runs of test
    // cases, e.g. in case of --gtest_repeat.
    //
    // This allows repeated test runs to operate independently without worrying
    // about leftover files from previous iterations.
    static auto now = ss::lowres_clock::now();
    ss::sstring dir = fmt::format(
      "{}.{}.{}.{}",
      test_info->test_suite_name(),
      test_info->name(),
      now.time_since_epoch().count(),
      gtest_iteration);

    // Swap out any '/'s (may come from parameterized tests).
    std::replace(dir.begin(), dir.end(), '/', '_');
    return dir;
}
