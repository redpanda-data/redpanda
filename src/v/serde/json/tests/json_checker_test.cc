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

#include "serde/json/parser.h"
#include "test_utils/runfiles.h"
#include "test_utils/test.h"
#include "utils/file_io.h"

#include <gtest/gtest.h>

#include <optional>

using namespace serde::json;

std::vector<std::string> collect_test_cases(const std::string& directory);

ss::future<> run_json_test(
  const std::string& directory,
  const std::string& test_case,
  std::optional<parser_config> config = std::nullopt) {
    auto test_case_path = test_utils::get_runfile_path(fmt::format(
      "src/v/serde/json/tests/testdata/{}/{}", directory, test_case));

    auto contents = co_await read_fully(test_case_path);
    auto parser = serde::json::parser(
      std::move(contents), config.value_or(parser_config{}));

    while (co_await parser.next()) {
        // Do nothing, just drain the parser.
        // We just check if the parser can parse the JSON document
        // successfully or not according to the test case name.
        // The contents are not verified in this test.
    }

    // The file name indicates whether parsing should succeed.
    bool expected_pass = test_case.starts_with("pass");
    auto current_token = parser.token();
    if (expected_pass) {
        EXPECT_NE(current_token, token::error) << "Expected to pass but failed";
    } else {
        EXPECT_EQ(current_token, token::error) << "Expected to fail but passed";
    }
}

class json_checker_test
  : public seastar_test
  , public testing::WithParamInterface<std::string> {};

TEST_P_CORO(json_checker_test, all) {
    auto test_case = GetParam();
    // Use depth limit of 19 to properly fail fail18.json (20 levels) while
    // allowing pass2.json (19 levels, "Not too deep") to succeed
    auto config = parser_config{.max_depth = 19};
    co_await run_json_test("jsonchecker", test_case, config);
}

INSTANTIATE_TEST_SUITE_P(
  json_checker_tests,
  json_checker_test,
  testing::ValuesIn(collect_test_cases("jsonchecker")));

std::vector<std::string> collect_test_cases(const std::string& directory) {
    std::vector<std::string> test_cases;

    auto test_case_path = test_utils::get_runfile_path(
      fmt::format("src/v/serde/json/tests/testdata/{}", directory));

    std::filesystem::path dir_path(test_case_path);
    vassert(
      std::filesystem::exists(dir_path),
      "Directory does not exist: {}",
      dir_path.string());
    vassert(
      std::filesystem::is_directory(dir_path),
      "Path is not a directory: {}",
      dir_path.string());

    for (const auto& entry : std::filesystem::directory_iterator(dir_path)) {
        if (entry.is_regular_file() || entry.is_symlink()) {
            std::string filename = entry.path().filename().string();
            if (filename.ends_with(".json")) {
                test_cases.push_back(filename);
            }
        } else {
            vassert(
              false,
              "Expecting only files but {} is not a file",
              entry.path().filename().string());
        }
    }

    vassert(
      !test_cases.empty(),
      "No test cases found in directory {}",
      dir_path.string());

    std::sort(
      test_cases.begin(),
      test_cases.end(),
      [](const std::string& a, const std::string& b) { return a < b; });

    return test_cases;
}
