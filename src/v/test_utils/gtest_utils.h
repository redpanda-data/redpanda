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
#pragma once

#include "base/seastarx.h"

#include <seastar/core/seastar.hh>

#include <gtest/gtest.h>

// Listens to the start of a gtest test iteration. May be used to track the
// test iteration number.
class rp_test_listener : public ::testing::EmptyTestEventListener {
    void
    OnTestIterationStart(const ::testing::UnitTest&, int iteration) override;

    void OnTestPartResult(const ::testing::TestPartResult& result) override;
};

// Returns a pathname that may be used for the currently running test.
// Expects that the caller is in the context of a GTest.
//
// Relies on callers to create the directory.
//
// Examples:
//
// clang-format off
// MySeastarFixture.TestGetTestDirectory.6125307633855650.4
// ^-- test suite   ^-- test case        ^-- timestamp    ^-- iteration
//
// Divisible_MySeastarParamFixture.TestGetTestDirectory_0.6126774487959615.6
// ^-- parameter name                                   ^-- parameter      ^-- iteration
//           ^-- test suite        ^-- test case          ^-- timestamp
// clang-format on
ss::sstring get_test_directory();
