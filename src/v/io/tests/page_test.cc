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

TEST(Page, Clear) {
    constexpr auto size = 10;

    auto d0 = make_random_data(size).get();
    io::page p(1, d0.share());

    EXPECT_EQ(p.size(), size);
    EXPECT_EQ(p.data().size(), size);
    EXPECT_EQ(p.data(), d0);

    p.clear();

    EXPECT_EQ(p.size(), 10); // page remembers its size
    EXPECT_EQ(p.data().size(), 0);
    EXPECT_EQ(p.data(), make_random_data(0).get());
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

TEST(Page, EvictionCheck) {
    auto p = seastar::make_lw_shared<io::page>(
      1, seastar::temporary_buffer<char>());
    EXPECT_TRUE(p->may_evict());

    p->set_flag(io::page::flags::faulting);
    EXPECT_FALSE(p->may_evict());

    p->clear_flag(io::page::flags::faulting);
    p->set_flag(io::page::flags::dirty);
    EXPECT_FALSE(p->may_evict());

    p->clear_flag(io::page::flags::dirty);
    EXPECT_TRUE(p->may_evict());

    // NOLINTNEXTLINE(performance-unnecessary-copy-initialization)
    [[maybe_unused]] auto p2 = p;
    EXPECT_FALSE(p->may_evict());
}

TEST(Page, Waiters) {
    io::page::waiter w0;
    io::page::waiter w1;

    io::page p(1, {});
    p.add_waiter(w0);
    p.add_waiter(w1);

    auto f0 = w0.ready.get_future();
    auto f1 = w1.ready.get_future();

    EXPECT_FALSE(f0.available());
    EXPECT_FALSE(f1.available());

    p.signal_waiters();

    EXPECT_TRUE(f0.available());
    EXPECT_TRUE(f1.available());
    EXPECT_FALSE(f0.failed());
    EXPECT_FALSE(f1.failed());
}
