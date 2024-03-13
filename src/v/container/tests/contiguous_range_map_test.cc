#include "container/contiguous_range_map.h"
#include "container/zip.h"
#include "gtest/gtest.h"
#include "random/generators.h"
#include "test_utils/randoms.h"

#include <absl/container/btree_map.h>
#include <fmt/core.h>
#include <gtest/gtest.h>

using int_range_map = contiguous_range_map<uint, int>;

static_assert(std::forward_iterator<int_range_map::iterator>);
static_assert(std::forward_iterator<int_range_map::const_iterator>);
static_assert(std::copy_constructible<int_range_map::iterator>);
static_assert(std::copy_constructible<int_range_map::const_iterator>);

struct verifier {
    template<typename Func>
    auto mutate(Func f) {
        f(expected);
        return f(under_test);
    }
    testing::AssertionResult is_valid() {
        if (under_test.size() != expected.size()) {
            testing::AssertionFailure() << fmt::format(
              "Expected size {} is different than current size {}",
              expected.size(),
              under_test.size());
        }

        for (auto tuple : container::zip(expected, under_test)) {
            auto& expected_value = tuple.get<0>();
            auto& current_value = tuple.get<1>();
            if (expected_value != current_value) {
                return testing::AssertionFailure() << fmt::format(
                         "Expected value {}:{} does not match the current "
                         "value: "
                         "{}:{}",
                         expected_value.first,
                         expected_value.second,
                         current_value.first,
                         current_value.second);
            }
        }

        return testing::AssertionSuccess();
    }

    int_range_map under_test;
    std::map<uint, int> expected;
};

TEST(IntegerRangeMap, Emplace) {
    verifier v;

    EXPECT_EQ(v.under_test.begin(), v.under_test.end());

    auto [it, success] = v.mutate([](auto& m) { return m.emplace(1, 0); });
    EXPECT_TRUE(success);

    EXPECT_EQ(it, v.under_test.begin());
    EXPECT_EQ(it->first, 1);
    EXPECT_EQ(it->second, 0);

    auto [new_it, not_success] = v.mutate(
      [](auto& m) { return m.emplace(1, 10); });
    EXPECT_FALSE(not_success);
    EXPECT_EQ(new_it->first, 1);
    EXPECT_EQ(new_it->second, 0);

    EXPECT_TRUE(v.is_valid());
}

TEST(IntegerRangeMap, EmplaceErase) {
    verifier v;
    v.mutate([](auto& m) { return m.erase(0); });

    v.mutate([](auto& m) { return m.emplace(1, 0); });
    v.mutate([](auto& m) { return m.emplace(3, 0); });
    v.mutate([](auto& m) { return m.emplace(8, 0); });
    v.mutate([](auto& m) { return m.emplace(10, 0); });
    v.mutate([](auto& m) { return m.emplace(15, 0); });

    v.mutate([](auto& m) { return m.erase(8); });
    v.mutate([](auto& m) { return m.erase(6); });
    v.mutate([](auto& m) { return m.erase(3); });

    v.mutate([](auto& m) {
        auto it = m.find(10);
        m.erase(it);
    });

    EXPECT_TRUE(v.is_valid());
}

TEST(IntegerRangeMap, ReserveShrink) {
    verifier v;
    v.under_test.reserve(16);
    EXPECT_EQ(v.under_test.capacity(), 16);
    EXPECT_TRUE(v.is_valid());

    v.mutate([](auto& m) { return m.emplace(1, 0); });
    v.mutate([](auto& m) { return m.emplace(3, 0); });
    v.mutate([](auto& m) { return m.emplace(8, 0); });
    v.mutate([](auto& m) { return m.emplace(10, 0); });
    v.mutate([](auto& m) { return m.emplace(15, 0); });

    v.mutate([](auto& m) { return m.erase(15); });
    v.mutate([](auto& m) { return m.erase(10); });
    v.mutate([](auto& m) { return m.erase(3); });
    // erase should not change capacity
    EXPECT_EQ(v.under_test.capacity(), 16);
    v.under_test.shrink_to_fit();
    EXPECT_EQ(v.under_test.capacity(), 9);
    EXPECT_TRUE(v.is_valid());
}

TEST(IntegerRangeMap, Iterator) {
    verifier v;

    EXPECT_EQ(v.under_test.begin(), v.under_test.end());

    auto [it_1, success_1] = v.mutate([](auto& m) { return m.emplace(1, 0); });
    auto [it_2, success_2] = v.mutate([](auto& m) { return m.emplace(3, 0); });

    EXPECT_GT(it_2, it_1);

    auto first = v.under_test.begin();
    auto second = std::next(v.under_test.begin());
    EXPECT_GT(second, first);
    EXPECT_EQ(std::next(v.under_test.begin()), second);

    EXPECT_TRUE(v.is_valid());
}

TEST(IntegerRangeMap, RandomOperations) {
    verifier v;
    static constexpr int key_set_cardinality = 3000000;
    for (int i = 0; i < 50000; ++i) {
        int key = random_generators::get_int(key_set_cardinality);
        if (tests::random_bool()) {
            auto value = random_generators::get_int(5000);
            v.mutate([key, value](auto& m) { return m.emplace(key, value); });
        } else {
            v.mutate([key](auto& m) { return m.erase(key); });
        }
    }

    EXPECT_TRUE(v.is_valid());
}

TEST(IntegerRangeMap, LookUp) {
    verifier v;

    for (auto i = 0; i < 10; i += 2) {
        v.mutate([i](auto& m) { return m.emplace(i, 2 * i); });
    }
    EXPECT_EQ(v.under_test.find(0)->first, 0);
    EXPECT_EQ(v.under_test.find(0)->second, 0);
    EXPECT_EQ(v.under_test.find(4)->first, 4);
    EXPECT_EQ(v.under_test.find(4)->second, 8);

    EXPECT_EQ(v.under_test.find(80), v.under_test.end());

    EXPECT_EQ(v.expected[0], 0);
    EXPECT_EQ(v.expected[2], 4);

    EXPECT_EQ(v.under_test[0], 0);

    v.under_test[5] = 1;
    v.expected[5] = 1;
    EXPECT_EQ(v.under_test[5], 1);

    EXPECT_TRUE(v.is_valid());
}

TEST(IntegerRangeMap, UpdateViaIterator) {
    verifier v;

    for (auto i = 0; i < 10; i += 2) {
        v.mutate([i](auto& m) { return m.emplace(i, 2 * i); });
    }

    v.mutate([](auto& m) {
        auto it = m.find(4);
        it->second = 200;
    });

    EXPECT_TRUE(v.is_valid());
}
