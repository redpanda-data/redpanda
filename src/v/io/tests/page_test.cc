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
#include "io/tests/common.h"
#include "test_utils/test.h"

namespace io = experimental::io;

TEST(Page, Empty) {
    const io::page p(1, {});
    EXPECT_EQ(p.offset(), 1);
    EXPECT_EQ(p.size(), 0);
    EXPECT_EQ(p.data().size(), 0);
}

TEST(Page, NonEmpty) {
    constexpr auto size = 10;
    seastar::temporary_buffer<char> data(size);
    const io::page p(1, std::move(data));
    EXPECT_EQ(p.offset(), 1);
    EXPECT_EQ(p.size(), size);
    EXPECT_EQ(p.data().size(), size);
}

TEST(Page, SetData) {
    constexpr auto size = 10;

    auto d0 = make_random_data(size).get();
    io::page p(1, d0.share());

    EXPECT_EQ(p.size(), size);
    EXPECT_EQ(p.data().size(), size);
    EXPECT_EQ(p.data(), d0);

    auto d1 = make_random_data(size).get();
    ASSERT_NE(d1, d0);
    p.data() = d1.share();

    EXPECT_EQ(p.size(), size);
    EXPECT_EQ(p.data().size(), size);
    EXPECT_EQ(p.data(), d1);
}

TEST(Page, Flags) {
    io::page p(1, {});

    EXPECT_FALSE(p.test_flag(io::page::flags::read));
    p.set_flag(io::page::flags::read);
    EXPECT_TRUE(p.test_flag(io::page::flags::read));

    EXPECT_FALSE(p.test_flag(io::page::flags::write));
    p.set_flag(io::page::flags::write);
    EXPECT_TRUE(p.test_flag(io::page::flags::write));
    EXPECT_TRUE(p.test_flag(io::page::flags::read));

    p.clear_flag(io::page::flags::read);
    EXPECT_FALSE(p.test_flag(io::page::flags::read));
    p.clear_flag(io::page::flags::write);
    EXPECT_FALSE(p.test_flag(io::page::flags::write));
    EXPECT_FALSE(p.test_flag(io::page::flags::read));
}
