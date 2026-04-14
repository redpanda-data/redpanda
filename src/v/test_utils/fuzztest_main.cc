/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "fuzztest/init_fuzztest.h"
#include "test_utils/gtest_utils.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/test_runner.hh>

#include <string_view>
#include <vector>

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);

    // Disable FuzzTest's stack limit monitor. Seastar's test runner executes
    // RUN_ALL_TESTS() inside seastar::async which uses makecontext/swapcontext.
    // This causes the stack pointer to jump to a different memory region,
    // producing a bogus stack usage reading (terabytes) that immediately
    // triggers the default 128 KiB limit.
    std::vector<char*> fuzz_argv;
    for (int i = 0; i < argc; ++i) {
        fuzz_argv.push_back(argv[i]);
    }
    // NOLINTNEXTLINE(*-avoid-c-arrays)
    static char stack_flag[] = "--stack_limit_kb=0";
    fuzz_argv.push_back(stack_flag);
    fuzz_argv.push_back(nullptr);
    int fuzz_argc = static_cast<int>(fuzz_argv.size() - 1);
    char** fuzz_argv_ptr = fuzz_argv.data();

    // ParseAbslFlags must be called before InitFuzzTest to parse FuzzTest's
    // abseil flags (--fuzz, --stack_limit_kb, etc.).
    fuzztest::ParseAbslFlags(fuzz_argc, fuzz_argv_ptr);
    fuzztest::InitFuzzTest(&fuzz_argc, &fuzz_argv_ptr);

    GTEST_FLAG_SET(death_test_style, "threadsafe");

    auto& listeners = ::testing::UnitTest::GetInstance()->listeners();
    listeners.Append(new rp_test_listener());

    if (GTEST_FLAG_GET(list_tests)) {
        return RUN_ALL_TESTS();
    }

    // Strip flags that Seastar doesn't understand. InitFuzzTest and
    // InitGoogleTest consume their own flags but may leave behind flags
    // that FuzzTest registers with abseil (e.g. --fuzz). Seastar uses
    // boost::program_options which rejects unknown options.
    std::vector<char*> ss_argv;
    for (int i = 0; i < fuzz_argc; ++i) {
        std::string_view arg(fuzz_argv_ptr[i]);
        if (arg.starts_with("--fuzz") || arg.starts_with("--stack_limit_kb")) {
            continue;
        }
        ss_argv.push_back(fuzz_argv_ptr[i]);
    }
    int ss_argc = static_cast<int>(ss_argv.size());

    seastar::testing::global_test_runner().start(ss_argc, ss_argv.data());

    int ret = 0;
    seastar::testing::global_test_runner().run_sync(
      [&ret] { return seastar::async([&ret] { ret = RUN_ALL_TESTS(); }); });

    int ss_ret = seastar::testing::global_test_runner().finalize();
    if (ret) {
        return ret;
    }
    return ss_ret;
}
