// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/startup_config.h"

#include <gtest/gtest.h>

namespace config {

using test_bool_config = startup_config<bool, struct test_bool_tag>;
using another_test_bool_config
  = startup_config<bool, struct another_test_bool_config_tag>;

class StartupConfigTest : public ::testing::Test {
protected:
    void TearDown() override {
        // Reset configs after each test
        if (test_bool_config::is_initialized()) {
            test_bool_config::reset_local();
        }
        if (another_test_bool_config::is_initialized()) {
            another_test_bool_config::reset_local();
        }
    }
};

TEST_F(StartupConfigTest, InitializeAndGet) {
    test_bool_config::set_local(true);
    EXPECT_TRUE(test_bool_config::is_initialized());
    EXPECT_TRUE(test_bool_config::get());

    test_bool_config::reset_local();
    test_bool_config::set_local(false);
    EXPECT_FALSE(test_bool_config::get());
}

TEST_F(StartupConfigTest, ResetAllowsReinitialization) {
    test_bool_config::set_local(true);
    EXPECT_TRUE(test_bool_config::get());

    test_bool_config::reset_local();
    EXPECT_FALSE(test_bool_config::is_initialized());

    test_bool_config::set_local(false);
    EXPECT_FALSE(test_bool_config::get());
}

TEST_F(StartupConfigTest, DifferentTagsHaveIndependentStorage) {
    test_bool_config::set_local(true);
    another_test_bool_config::set_local(false);

    EXPECT_TRUE(test_bool_config::get());
    EXPECT_EQ(another_test_bool_config::get(), false);

    test_bool_config::reset_local();
    EXPECT_FALSE(test_bool_config::is_initialized());
    EXPECT_TRUE(another_test_bool_config::is_initialized());
    EXPECT_EQ(another_test_bool_config::get(), false);
}

} // namespace config
