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
#pragma once

#include <string>

namespace test_env {

// Return a randomly named path under the test temporary directory.
// No directory is created. There is no guarantee the path is unique
// or unused though a sufficiently long random suffix makes that highly
// likely.
//
// Unlike the default seeding policy for random_generators, which uses
// the same seed at the start of each test case, this uses a random seed
// so that multiple calls within a single test process (in different
// test cases) will get different paths.
std::string
random_dir_path(std::string prefix = "test.dir_", size_t suffix_len = 6);

} // namespace test_env
