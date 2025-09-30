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

#include "test_utils/test_env.h"

#include "random/generators.h"

#include <cstdlib>
#include <filesystem>
#include <string>

namespace test_env {

namespace {
// return the temporary test directory if set, else empty string
std::string test_tmpdir() {
    auto tmpdir = std::getenv("TEST_TMPDIR");
    return tmpdir ? tmpdir : "";
}
} // namespace

// Return a randomly named path under the test temporary directory.
// No directory is created.
std::string random_dir_path(std::string prefix, size_t suffix_len) {
    // we need with_random_seed here because multiple calls within a single
    // test process (in different test cases) should get different paths.
    std::string suffix
      = random_generators::with_random_seed().gen_alphanum_string(suffix_len);

    return std::filesystem::path(test_tmpdir()) / (prefix + suffix);
}

} // namespace test_env
