/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "test_utils/test_env.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <string>
#include <unordered_set>

class test_env_test : public ::testing::Test {
protected:
    void SetUp() override {
        // save and restore TEST_TMPDIR as other tests rely on this
        auto test_tmpdir = std::getenv("TEST_TMPDIR");
        if (test_tmpdir) {
            saved_test_tmpdir_ = test_tmpdir;
        }
    }

    void TearDown() override {
        // Restore original environment state
        if (saved_test_tmpdir_) {
            setenv("TEST_TMPDIR", saved_test_tmpdir_.value().c_str(), 1);
        } else {
            unsetenv("TEST_TMPDIR");
        }
    }

private:
    std::optional<std::string> saved_test_tmpdir_;
};

TEST_F(test_env_test, random_dir_path_with_defaults) {
    // Set up TEST_TMPDIR
    setenv("TEST_TMPDIR", "/tmp/test", 1);

    auto path = test_env::random_dir_path();

    // Should be under TEST_TMPDIR
    EXPECT_TRUE(path.starts_with("/tmp/test/"));
    // Should have default prefix
    EXPECT_TRUE(path.find("test.dir_") != std::string::npos);
    // Should have 6 character suffix (default)
    auto prefix_pos = path.find("test.dir_");
    ASSERT_NE(prefix_pos, std::string::npos);
    constexpr size_t default_prefix_len = 9; // length of "test.dir_"
    auto suffix_start = prefix_pos + default_prefix_len;
    auto suffix = path.substr(suffix_start);
    constexpr size_t default_suffix_len = 6;
    EXPECT_EQ(suffix.length(), default_suffix_len);

    // Suffix should be alphanumeric
    for (char c : suffix) {
        EXPECT_TRUE(std::isalnum(c));
    }
}

TEST_F(test_env_test, random_dir_path_with_custom_parameters) {
    setenv("TEST_TMPDIR", "/tmp/custom", 1);

    constexpr size_t custom_suffix_len = 10;
    auto path = test_env::random_dir_path("my_prefix_", custom_suffix_len);

    EXPECT_TRUE(path.starts_with("/tmp/custom/"));
    EXPECT_TRUE(path.find("my_prefix_") != std::string::npos);

    // Check suffix length
    auto prefix_pos = path.find("my_prefix_");
    ASSERT_NE(prefix_pos, std::string::npos);
    constexpr size_t custom_prefix_len = 10; // length of "my_prefix_"
    auto suffix_start = prefix_pos + custom_prefix_len;
    auto suffix = path.substr(suffix_start);
    EXPECT_EQ(suffix.length(), custom_suffix_len);
}

TEST_F(test_env_test, random_dir_path_no_test_tmpdir) {
    unsetenv("TEST_TMPDIR");

    auto path = test_env::random_dir_path();

    // Should still work but path might be relative
    EXPECT_TRUE(path.find("test.dir_") != std::string::npos);
}

TEST_F(test_env_test, random_dir_path_uniqueness) {
    setenv("TEST_TMPDIR", "/tmp/unique", 1);

    // Generate multiple paths and ensure they're different
    std::unordered_set<std::string> paths;
    constexpr int num_iterations = 100;
    for (int i = 0; i < num_iterations; ++i) {
        auto path = test_env::random_dir_path();
        EXPECT_TRUE(paths.insert(path).second)
          << "Duplicate path generated: " << path;
    }
}

TEST_F(test_env_test, getenv_with_default) {
    // Test with existing environment variable
    setenv("TEST_ENV_TEST_VAR", "test_value", 1);
    EXPECT_EQ(test_env::getenv("TEST_ENV_TEST_VAR", "default"), "test_value");

    // Test with non-existing environment variable
    unsetenv("TEST_ENV_TEST_VAR");
    EXPECT_EQ(test_env::getenv("TEST_ENV_TEST_VAR", "default"), "default");

    // Test with empty default
    EXPECT_EQ(test_env::getenv("TEST_ENV_TEST_VAR"), "");
}

TEST_F(test_env_test, getenv_empty_variable) {
    // Test with empty environment variable (set but empty)
    setenv("TEST_ENV_TEST_VAR", "", 1);
    EXPECT_EQ(test_env::getenv("TEST_ENV_TEST_VAR", "default"), "");
}

TEST_F(test_env_test, getenv_default_fallback) {
    // Test primary variable exists
    setenv("TEST_ENV_TEST_VAR", "primary_value", 1);
    unsetenv("TEST_ENV_TEST_VAR_DEFAULT");
    EXPECT_EQ(
      test_env::getenv_default("TEST_ENV_TEST_VAR", "fallback"),
      "primary_value");

    // Test primary doesn't exist, default exists
    unsetenv("TEST_ENV_TEST_VAR");
    setenv("TEST_ENV_TEST_VAR_DEFAULT", "default_value", 1);
    EXPECT_EQ(
      test_env::getenv_default("TEST_ENV_TEST_VAR", "fallback"),
      "default_value");

    // Test neither exists, use fallback
    unsetenv("TEST_ENV_TEST_VAR");
    unsetenv("TEST_ENV_TEST_VAR_DEFAULT");
    EXPECT_EQ(
      test_env::getenv_default("TEST_ENV_TEST_VAR", "fallback"), "fallback");

    // Test with empty fallback
    EXPECT_EQ(test_env::getenv_default("TEST_ENV_TEST_VAR"), "");
}

TEST_F(test_env_test, getenv_default_priority) {
    // Test that primary variable takes precedence
    setenv("TEST_ENV_TEST_VAR", "primary_value", 1);
    setenv("TEST_ENV_TEST_VAR_DEFAULT", "default_value", 1);
    EXPECT_EQ(
      test_env::getenv_default("TEST_ENV_TEST_VAR", "fallback"),
      "primary_value");
}

TEST_F(test_env_test, GetenvDefaultEmptyValues) {
    // Test with empty primary variable
    setenv("TEST_ENV_TEST_VAR", "", 1);
    setenv("TEST_ENV_TEST_VAR_DEFAULT", "default_value", 1);
    EXPECT_EQ(test_env::getenv_default("TEST_ENV_TEST_VAR", "fallback"), "");

    // Test with empty default variable
    unsetenv("TEST_ENV_TEST_VAR");
    setenv("TEST_ENV_TEST_VAR_DEFAULT", "", 1);
    EXPECT_EQ(test_env::getenv_default("TEST_ENV_TEST_VAR", "fallback"), "");
}
