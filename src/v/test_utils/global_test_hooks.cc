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

#include "random/test_seeding.h"

#include <string>

namespace test_hooks {

void before_test_case([[maybe_unused]] const std::string& test_name) {
    // reset seeds for each test case to ensure reproducibility
    random_generators::reset_seed_for_tests();
}

void after_test_case([[maybe_unused]] const std::string& test_name) {}
} // namespace test_hooks
