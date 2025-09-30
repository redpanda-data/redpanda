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

#include "gtest/gtest.h"
#include "test_utils/gtest_utils.h"

#include <gtest/gtest.h>

#include <stdexcept>

/// Helper function to print and error message and exit with code 1.
/// We can't use gtest assertions because the way test is set up is meant to
/// fail and we hide that failure.
void fail_test(const std::string_view msg) {
    std::cout << testing::internal::GetCapturedStdout() << std::endl;
    fmt::print("FAILURE: {}\n", msg);
    exit(1);
}

void helper_asserts_false() {
    ASSERT_FALSE(true) << "Expected failure. Aye aye!";
}

TEST(AssertInHelper, AssertInHelperThrows) {
    try {
        helper_asserts_false();
        fail_test(
          "Expected exception to be thrown above and this line to not "
          "be executed.");
    } catch (const testing::AssertionException& e) {
        if (strstr(e.what(), "Expected failure. Aye aye!") == nullptr) {
            fail_test(
              "Expected message to contain 'Expected failure. Aye aye!'");
        }
        throw;
    }
}

TEST(ThrowFromBody, ThrowFromBodyDoesNotAbort) {
    throw std::runtime_error("throwing from test body");
}

bool global_did_run_after_throw = false;

TEST(ThrowFromBody, ThisTestRunsAfterThrow) {
    global_did_run_after_throw = true;
}

/// In this test we want to check that the listener intercepts asserts and
/// throws exception resulting in not just test failure but test termination.
int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);

    auto& listeners = ::testing::UnitTest::GetInstance()->listeners();
    listeners.Append(new rp_test_listener());

    if (GTEST_FLAG_GET(fail_fast)) {
        // this test makes assumptions about fail fast behavior not being
        // on so just short-circuit out if the user has set that (e.g.,
        // via bazel test --test_runner_fail_fast)
        fmt::print("Skipping test because --gtest_fail_fast is set\n");
        return 0;
    }

    // Hide stdout to avoid confusing users.
    testing::internal::CaptureStdout();
    int result = RUN_ALL_TESTS();
    if (result == 0) {
        fail_test("Expected the test to fail but it passed.");
    }

    if (!global_did_run_after_throw) {
        fail_test("Expected the test after throw to run but it did not.");
    }

    return 0;
}
