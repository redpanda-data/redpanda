/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "test_utils/gtest_utils.h"

#include <seastar/core/smp.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_runner.hh>

#include <csignal>

namespace {

seastar::future<> unblock_sigill() {
    // A workaround until https://github.com/scylladb/seastar/pull/3102 lands
    return seastar::smp::invoke_on_all([] {
        ::sigset_t set;
        ::sigemptyset(&set);
        ::sigaddset(&set, SIGILL);
        auto r = ::pthread_sigmask(SIG_UNBLOCK, &set, nullptr);
        ss::throw_pthread_error(r);
    });
}

} // namespace

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    GTEST_FLAG_SET(install_failure_signal_handler, true);

    auto& listeners = ::testing::UnitTest::GetInstance()->listeners();
    listeners.Append(new rp_test_listener());

    /*
     * if the test is being passed --gtest_list_tests then do not start Seastar.
     * this is an important optimization for using `gtest_discover_tests`
     * because every test exectuable is queried in parallel, and the result is
     * that listing would timeout or seastar would exhaust some kind of resource
     * like aio-max-nr.
     */
    if (GTEST_FLAG_GET(list_tests)) {
        return RUN_ALL_TESTS();
    }

    seastar::testing::global_test_runner().start(argc, argv);

    int ret = 0;
    seastar::testing::global_test_runner().run_sync([&ret] {
        return unblock_sigill().then(
          [&ret] { return seastar::async([&ret] { ret = RUN_ALL_TESTS(); }); });
    });

    int ss_ret = seastar::testing::global_test_runner().finalize();
    if (ret) {
        return ret;
    }
    return ss_ret;
}
