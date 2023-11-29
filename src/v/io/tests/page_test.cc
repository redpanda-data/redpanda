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
#include "io/page.h"
#include "test_utils/test.h"

namespace io = experimental::io;

TEST(Page, Empty) {
    io::page p(1, {});
    EXPECT_EQ(p.offset(), 1);
    EXPECT_EQ(p.size(), 0);
    EXPECT_EQ(p.data().size(), 0);
}

TEST(Page, NonEmpty) {
    constexpr auto size = 10;
    seastar::temporary_buffer<char> data(size);
    io::page p(1, std::move(data));
    EXPECT_EQ(p.offset(), 1);
    EXPECT_EQ(p.size(), size);
    EXPECT_EQ(p.data().size(), size);
}
