// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "base/seastarx.h"
#include "ssx/async_merge_top_k.h"
#include "test_utils/test.h"

namespace {

struct move_only {
    explicit move_only(int val)
      : val(val) {}
    move_only(move_only&&) noexcept = default;
    move_only& operator=(move_only&&) noexcept = default;
    move_only(const move_only&) = delete;
    move_only& operator=(const move_only&) = delete;
    ~move_only() noexcept = default;

    friend bool
    operator==(const move_only&, const move_only&) noexcept = default;

    friend auto
    operator<=>(const move_only&, const move_only&) noexcept = default;

    int val;
};

} // namespace

template<typename T>
ss::future<void> run_test() {
    using value_type = T;

    chunked_vector<chunked_vector<value_type>> input;
    input.emplace_back();
    input.back().emplace_back(12);
    input.back().emplace_back(5);
    input.back().emplace_back(5);
    input.emplace_back();
    input.back().emplace_back(2);
    input.emplace_back();
    input.back().emplace_back(14);
    input.back().emplace_back(9);
    input.back().emplace_back(1);
    input.emplace_back();

    chunked_vector<value_type> result;
    result.reserve(5);
    co_await ssx::async_merge_top_k(input, std::back_inserter(result), 5);

    EXPECT_EQ(result.size(), 5);
    EXPECT_EQ(result[0], value_type{14});
    EXPECT_EQ(result[1], value_type{12});
    EXPECT_EQ(result[2], value_type{9});
    EXPECT_EQ(result[3], value_type{5});
    EXPECT_EQ(result[4], value_type{5});
}

TEST_CORO(AsyncMergeTopK, copy_value) { co_await run_test<int>(); }
TEST_CORO(AsyncMergeTopK, move_value) { co_await run_test<move_only>(); }
