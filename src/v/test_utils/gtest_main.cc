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
#include "test_utils/test.h"

namespace {
// set by main() and consumed by seastar test runner initialization
int g_argc;
char** g_argv;

// seastar runner is a singleton that needs to be initialized once
std::once_flag seastar_runner_init_flag;
} // namespace

void seastar_test_mixin::init_seastar_test_runner() {
    std::call_once(seastar_runner_init_flag, [] {
        seastar::testing::global_test_runner().start(g_argc, g_argv);
    });
}

void seastar_test_mixin::run(std::function<seastar::future<>()> task) {
    // first test to need seastar will initialize it
    init_seastar_test_runner();
    seastar::testing::global_test_runner().run_sync(std::move(task));
}

void seastar_test_mixin::run_async(std::function<void()> task) {
    run([task = std::move(task)]() mutable {
        return seastar::async([task = std::move(task)] { task(); });
    });
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    g_argc = argc;
    g_argv = argv;

    int ret = RUN_ALL_TESTS();

    int ss_ret = seastar::testing::global_test_runner().finalize();
    if (ret) {
        return ret;
    }
    return ss_ret;
}
