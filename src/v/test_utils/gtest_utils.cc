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
#include "test_utils/global_test_hooks.h"

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
    if (result.fatally_failed()) {
        const bool failed_with_exception = result.file_name() == nullptr
                                           && result.line_number() == -1;
        if (failed_with_exception) {
            // If the test failed because an exception was thrown, we don't want
            // to throw another exception here. The test is already failed and
            // any additional exceptions are propagated to the runner process
            // which would exit. This is a workaround for the issue
            // https://github.com/google/googletest/issues/4791.
            return;
        }

        // Throw an exception to fail the test on first assert (even if it is
        // from a helper function).
        // See
        // https://google.github.io/googletest/advanced.html#asserting-on-subroutines-with-an-exception.
        throw testing::AssertionException(result);
    }
}

void rp_test_listener::OnTestStart(const ::testing::TestInfo& test_info) {
    test_hooks::before_test_case(test_info.name());
}

void rp_test_listener::OnTestEnd(const ::testing::TestInfo& test_info) {
    test_hooks::after_test_case(test_info.name());
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
