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

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    seastar::testing::global_test_runner().start(argc, argv);

    int ret = 0;
    seastar::testing::global_test_runner().run_sync(
      [&ret] { return seastar::async([&ret] { ret = RUN_ALL_TESTS(); }); });

    int ss_ret = seastar::testing::global_test_runner().finalize();
    if (ret) {
        return ret;
    }
    return ss_ret;
}
