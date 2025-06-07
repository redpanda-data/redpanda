/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "base/opt.h"
#include "base/seastarx.h"
#include "base/type_traits.h"
#include "base/vassert.h"
#include "random/generators.h"

#include <seastar/core/sstring.hh>

#include <gtest/gtest.h>

#include <iostream>
#include <optional>

TEST(opt, WorksWithPrimitiveTypes) {
    opt<int> a;
    ASSERT_FALSE(a.has_value());
    a = std::nullopt;
    ASSERT_FALSE(a.has_value());
    a.emplace(1);
    ASSERT_TRUE(a.has_value());
    ASSERT_EQ(a.value(), 1);
    a.reset();
    ASSERT_FALSE(a.has_value());
    ASSERT_THROW(a.value(), std::bad_optional_access);
    ASSERT_EQ(a.value_or(42), 42);
    a = make_opt<int>(64);
    ASSERT_EQ(a.value(), 64);

    opt<int> b{23};
    a = b;
    ASSERT_EQ(a, b);
    ASSERT_LE(a, b);
    ASSERT_GE(a, b);
    b.emplace(42);
    ASSERT_NE(a, b);
    ASSERT_LT(a, b);
    ASSERT_GT(b, a);

    opt<int> c{std::make_optional<int>(99)};
    ASSERT_EQ(c.value(), 99);

    // below does not compile, as intended
    // ASSERT_TRUE(b);
    // ASSERT_EQ(*b, 42);
    // b = std::nullopt;
    // ASSERT_FALSE(b);
    // struct foo {
    //     int a;
    // };
    // auto some_foo = make_opt<foo>(23);
    // ASSERT_EQ(some_foo->a, 23);

    auto d = std::exchange(b, std::nullopt);
    ASSERT_EQ(d.value(), 42);
}

struct constructor_stats {
    friend auto operator<=>(const constructor_stats&, const constructor_stats&)
      = default;
    int copy_count{};
    int move_count{};
    int default_count{};
    int value_count{};
    int destructor_count{};
    int copy_assign_count{};
    int move_assign_count{};
};

std::ostream& operator<<(std::ostream& os, const constructor_stats& s) {
    fmt::print(
      os,
      "cp: {}, mv: {}, dflt: {}, val: {}, dest: {}, cp_a: {}, mv_a: {}",
      s.copy_count,
      s.move_count,
      s.default_count,
      s.value_count,
      s.destructor_count,
      s.copy_assign_count,
      s.move_assign_count);
    return os;
}

template<typename tag>
struct nontrivial {
    nontrivial() { ++stats.default_count; }
    explicit nontrivial(size_t n) {
        ++stats.value_count;
        v.reserve(n);
        for (size_t i = 0; i < n; ++i) {
            v.push_back(random_generators::gen_alphanum_string(128));
        }
    }
    ~nontrivial() { ++stats.destructor_count; }
    nontrivial(const nontrivial& other)
      : v(other.v.begin(), other.v.end()) {
        ++stats.copy_count;
    }
    nontrivial(nontrivial&& other) noexcept
      : v(std::move(other.v)) {
        ++stats.move_count;
    }
    nontrivial& operator=(const nontrivial& other) {
        ++stats.copy_assign_count;
        if (this == &other) {
            return *this;
        }
        v = other.v;
        return *this;
    }
    nontrivial& operator=(nontrivial&& other) noexcept {
        ++stats.move_assign_count;
        v = std::move(other.v);
        return *this;
    }

    std::vector<ss::sstring> v{};

    static constructor_stats stats;
};

template<typename T>
constructor_stats nontrivial<T>::stats{};

using s1 = nontrivial<struct s1_tag>;
using s2 = nontrivial<struct s2_tag>;
using s3 = nontrivial<struct s3_tag>;

template<typename T>
requires reflection::is_std_optional<T> || reflection::is_opt<T>
T run_optional_test(T oval) {
    vassert(oval.has_value(), "Pass a value please");
    // copy
    T copy{oval};
    // destroy original
    oval.reset();
    // move
    T moved{std::move(copy).value()};
    // destroy copy
    copy = std::nullopt;
    return std::move(moved).value();
}

template<typename Container>
constructor_stats run_container_of_optional_test(int n = 100) {
    Container c;
    for (int i = 0; i < n; ++i) {
        c.emplace_back(10);
    }

    Container copy_c;
    copy_c.reserve(c.size());
    std::ranges::copy(c, std::back_inserter(copy_c));
    return Container::value_type::value_type::stats;
}

TEST(opt, WorksWithNontrivialTypes) {
    {
        auto stdopt = run_optional_test(std::make_optional<s1>(10));
        stdopt = run_optional_test(
          std::make_optional<s1>(std::move(stdopt).value()));
        stdopt = run_optional_test(std::make_optional<s1>(stdopt.value()));
        ASSERT_TRUE(stdopt.has_value());
    }

    {
        auto rpdopt = run_optional_test(make_opt<s2>(10));
        rpdopt = run_optional_test(make_opt<s2>(std::move(rpdopt).value()));
        rpdopt = run_optional_test(make_opt<s2>(rpdopt.value()));
        ASSERT_TRUE(rpdopt.has_value());
    }

    {
        auto rpdopt = run_optional_test(opt<s3>{std::make_optional<s3>(10)});
        rpdopt = run_optional_test(
          std::make_optional<s3>(std::move(rpdopt).value()));
        rpdopt = run_optional_test(std::make_optional<s3>(rpdopt.value()));
        ASSERT_TRUE(rpdopt.has_value());
    }

    ASSERT_EQ(s1::stats, s2::stats) << s1::stats << " - " << s2::stats;
    ASSERT_GT(s3::stats, s1::stats) << s3::stats;
    ASSERT_EQ(s3::stats.move_count, s2::stats.move_count + 1);
    ASSERT_EQ(s3::stats.destructor_count, s2::stats.destructor_count + 1);
}

TEST(opt, WorksWithContainersOfNontrivialTypes) {
    auto stdopt
      = run_container_of_optional_test<std::vector<std::optional<s1>>>();
    auto rpdopt = run_container_of_optional_test<std::vector<opt<s2>>>();

    ASSERT_EQ(stdopt, rpdopt);
}
