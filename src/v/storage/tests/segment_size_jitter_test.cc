// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/log_manager.h"
#include "storage/segment_utils.h"

#include <seastar/testing/thread_test_case.hh>

SEASTAR_THREAD_TEST_CASE(test_segment_size_jitter_calculation) {
    constexpr auto jitter = storage::jitter_percents(5);
    std::array<size_t, 5> sizes = {1_GiB, 2_GiB, 100_MiB, 300_MiB, 10_GiB};
    for (auto original_size : sizes) {
        for (int i = 0; i < 100; ++i) {
            auto new_sz = original_size
                          * (1 + storage::internal::random_jitter(jitter));
            BOOST_REQUIRE_GE(new_sz, 0.95f * original_size);
            BOOST_REQUIRE_LE(new_sz, 1.05f * original_size);
        }
    }
};
