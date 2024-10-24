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
#include "base/units.h"
#include "common.h"
#include "utils/memory_data_source.h"

#include <gtest/gtest.h>

TEST(Common, MakeRandomData) {
    auto d0 = make_random_data(100).get();
    auto d1 = make_random_data(100).get();
    EXPECT_EQ(d0.size(), 100);
    EXPECT_EQ(d1.size(), 100);
    EXPECT_NE(d0, d1);
}

TEST(Common, MakeRandomDataSeed) {
    auto d0 = make_random_data(100, std::nullopt, 10).get();
    auto d1 = make_random_data(100, std::nullopt, 11).get();
    auto d2 = make_random_data(100, std::nullopt, 10).get();
    EXPECT_EQ(d0.size(), 100);
    EXPECT_EQ(d1.size(), 100);
    EXPECT_EQ(d2.size(), 100);
    EXPECT_NE(d0, d1);
    EXPECT_EQ(d0, d2);
}

TEST(Common, MakeRandomDataAlignment) {
    auto d0 = make_random_data(4096, 4096).get();
    auto d1 = make_random_data(16384, 4096).get();
    auto d2 = make_random_data(32768, 8192).get();
    EXPECT_EQ(d0.size(), 4096);
    EXPECT_EQ(d1.size(), 16384);
    EXPECT_EQ(d2.size(), 32768);
    // NOLINTBEGIN(cppcoreguidelines-pro-type-reinterpret-cast)
    EXPECT_EQ(reinterpret_cast<uintptr_t>(d0.get()) % 4096, 0);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(d1.get()) % 4096, 0);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(d2.get()) % 8192, 0);
    // NOLINTEND(cppcoreguidelines-pro-type-reinterpret-cast)
}

TEST(Common, MakePage) {
    auto p0 = make_page(4096);
    auto p1 = make_page(8192, 1);
    auto p2 = make_page(16384, 2);
    auto p3 = make_page(8192, 1);

    EXPECT_EQ(p0->offset(), 4096);
    EXPECT_EQ(p1->offset(), 8192);
    EXPECT_EQ(p2->offset(), 16384);
    EXPECT_EQ(p3->offset(), 8192);

    EXPECT_EQ(p0->size(), 4096);
    EXPECT_EQ(p1->size(), 4096);
    EXPECT_EQ(p2->size(), 4096);
    EXPECT_EQ(p3->size(), 4096);

    EXPECT_NE(p0->data(), p1->data());
    EXPECT_NE(p0->data(), p2->data());
    EXPECT_NE(p1->data(), p2->data());
    EXPECT_EQ(p1->data(), p3->data());

    // NOLINTBEGIN(cppcoreguidelines-pro-type-reinterpret-cast)
    EXPECT_EQ(reinterpret_cast<uintptr_t>(p0->data().get()) % 4096, 0);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(p1->data().get()) % 4096, 0);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(p2->data().get()) % 4096, 0);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(p3->data().get()) % 4096, 0);
    // NOLINTEND(cppcoreguidelines-pro-type-reinterpret-cast)
}

TEST(Common, EqualInputStreams) {
    auto make = [](seastar::sstring s) {
        return seastar::input_stream<char>(seastar::data_source(
          std::make_unique<memory_data_source>(std::move(s).release())));
    };

    auto make_buf = [](seastar::temporary_buffer<char> s) {
        return seastar::input_stream<char>(seastar::data_source(
          std::make_unique<memory_data_source>(std::move(s))));
    };

    {
        auto a = make("");
        auto b = make("");
        EXPECT_TRUE(EqualInputStreams(a, b));
    }

    {
        auto a = make("x");
        auto b = make("");
        EXPECT_FALSE(EqualInputStreams(a, b));
    }

    {
        auto a = make("");
        auto b = make("x");
        EXPECT_FALSE(EqualInputStreams(a, b));
    }

    {
        auto a = make("x");
        auto b = make("x");
        EXPECT_TRUE(EqualInputStreams(a, b));
    }

    {
        auto a = make_buf(make_random_data(256_KiB, std::nullopt, 1).get());
        auto b = make_buf(make_random_data(256_KiB, std::nullopt, 1).get());
        EXPECT_TRUE(EqualInputStreams(a, b));
    }

    {
        auto a = make_buf(make_random_data(256_KiB, std::nullopt, 1).get());
        auto b = make_buf(make_random_data(256_KiB, std::nullopt, 2).get());
        EXPECT_FALSE(EqualInputStreams(a, b));
    }

    {
        auto a = make_buf(make_random_data(256_KiB - 1, std::nullopt, 1).get());
        auto b = make_buf(make_random_data(256_KiB, std::nullopt, 1).get());
        EXPECT_FALSE(EqualInputStreams(a, b));
    }
}
