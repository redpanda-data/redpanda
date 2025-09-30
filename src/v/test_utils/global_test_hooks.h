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

#pragma once

#include <string>

namespace test_hooks {
// Will be called before each test case.
void before_test_case(const std::string& test_name);

// Will be called after each test case.
void after_test_case(const std::string& test_name);
} // namespace test_hooks
