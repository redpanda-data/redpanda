/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "utils/arch.h"

#include <gtest/gtest.h>

using namespace util;

// Mirrors cpu_arch::current() branch for branch, including the #error. The
// previous form tested only for x86_64 and assumed arm64 otherwise, so on any
// third architecture it compiled cleanly and then asserted the wrong answer.
#if defined(__x86_64__)
constexpr auto expected_arch = arch::AMD64;
#elif defined(__aarch64__)
constexpr auto expected_arch = arch::ARM64;
#elif defined(__riscv) && __riscv_xlen == 64
constexpr auto expected_arch = arch::RISCV64;
#else
#error unknown arch
#endif

GTEST_TEST(arch, equality) { EXPECT_EQ(cpu_arch::current(), expected_arch); }
