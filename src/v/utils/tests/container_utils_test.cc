/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "utils/container_utils.h"

#include <seastar/testing/thread_test_case.hh>

#include <absl/container/flat_hash_map.h>

SEASTAR_THREAD_TEST_CASE(async_clear_flat_hash_map_test) {
    absl::flat_hash_map<int, int> testCase{
      {0, 0}, {1, 1}, {2, 2}, {3, 3}, {4, 4}};

    BOOST_CHECK_EQUAL(testCase.empty(), false);
    async_clear(testCase).get();
    BOOST_CHECK_EQUAL(testCase.empty(), true);
}
