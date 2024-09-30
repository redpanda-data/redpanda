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

#include "container/fragmented_vector.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "random/generators.h"
#include "serde/rw/rw.h"
#include "serde/rw/vector.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <memory>
#include <numeric>
#include <span>
#include <stdexcept>
#include <type_traits>
#include <vector>

using testing::AssertionFailure;
using testing::AssertionResult;
using testing::AssertionSuccess;
using vec = fragmented_vector<int>;

static_assert(std::forward_iterator<vec::iterator>);
static_assert(std::forward_iterator<vec::const_iterator>);

class fragmented_vector_validator {
public:
    // perform an internal consistency check of the vector structure
    template<typename T, size_t S>
    static AssertionResult validate(const fragmented_vector<T, S>& v) {
        if (v._size > v._capacity) {
            return AssertionFailure() << "size greater than capacity";
        }
        if (v._size >= std::numeric_limits<size_t>::max() / 2) {
            return AssertionFailure() << "size too big";
        }
        if (v._capacity >= std::numeric_limits<size_t>::max() / 2) {
            return AssertionFailure() << "capacity too big";
        }
        size_t calc_size = 0, calc_cap = 0;

        for (size_t i = 0; i < v._frags.size(); ++i) {
            auto& f = v._frags[i];

            calc_size += f.size();
            calc_cap += f.capacity();

            if (i + 1 < v._frags.size()) {
                if (f.size() < v.elements_per_fragment()) {
                    return AssertionFailure() << fmt::format(
                             "fragment {} is undersized ({} < {})",
                             i,
                             f.size(),
                             v.elements_per_fragment());
                }
            }
            if (f.capacity() > std::decay_t<decltype(v)>::max_frag_bytes()) {
                return AssertionFailure() << fmt::format(
                         "fragment {} capacity over max_frag_bytes ({})",
                         i,
                         calc_cap);
            }
        }

        if (calc_size != v.size()) {
            return AssertionFailure() << fmt::format(
                     "calculated size is wrong ({} != {})",
                     calc_size,
                     v.size());
        }
        if (calc_cap != v._capacity) {
            return AssertionFailure() << fmt::format(
                     "calculated capacity is wrong ({} != {})",
                     calc_cap,
                     v._capacity);
        }
        return AssertionSuccess();
    }

    /**
     * Lets tests access private members for when they
     * need to inspect internals.
     */
    template<typename T, size_t Size, typename MemberType>
    const MemberType& access(
      const fragmented_vector<T, Size>& v,
      MemberType fragmented_vector<T, Size>::*member) {
        return v.*member;
    }
};

MATCHER(IsValid, "") {
    AssertionResult result = fragmented_vector_validator::validate(arg);
    *result_listener << result.message();
    return result;
}

namespace {

/**
 * Proxy that applies a consistency check before deference
 */
template<typename T>
struct checker {
    using underlying = fragmented_vector<T>;

    static checker<T> make(std::vector<T> in) {
        checker ret;
        for (auto& e : in) {
            ret->push_back(e);
        }
        return ret;
    }

    underlying* operator->() {
        auto is_valid = fragmented_vector_validator::validate(u);
        if (!is_valid) {
            throw std::runtime_error(is_valid.message());
        }
        return &u;
    }

    underlying& get() { return *operator->(); }

    auto operator<=>(const checker&) const = default;

    friend std::ostream& operator<<(std::ostream& os, const checker& c) {
        os << c.u;
        return os;
    }

    underlying u;
};

using testing::ElementsAre;
using testing::ElementsAreArray;
using testing::Gt;
using testing::IsEmpty;
using testing::Lt;
using testing::Ne;
using testing::Not;

template<typename T, size_t S>
AssertionResult is_eq(fragmented_vector<T, S>& impl, std::vector<T>& shadow) {
    if (!std::equal(impl.begin(), impl.end(), shadow.begin(), shadow.end())) {
        return testing::AssertionFailure()
               << "iterators not equal: " << testing::PrintToString(impl)
               << " vs " << testing::PrintToString(shadow);
    }
    if (impl.empty() != shadow.empty()) {
        return testing::AssertionFailure()
               << "empty not equal: " << testing::PrintToString(impl) << " vs "
               << testing::PrintToString(shadow);
    }
    if (impl.size() != shadow.size()) {
        return testing::AssertionFailure()
               << "size not equal: " << testing::PrintToString(impl) << " vs "
               << testing::PrintToString(shadow);
    }
    return fragmented_vector_validator::validate(impl);
}

template<size_t S>
AssertionResult
push(fragmented_vector<int, S>& impl, std::vector<int>& shadow, int count) {
    for (int i = 0; i < count; ++i) {
        shadow.push_back(i);
        impl.push_back(i);
        auto r = is_eq(impl, shadow);
        if (!r) {
            return r;
        }
        impl = serde::from_iobuf<std::decay_t<decltype(impl)>>(
          serde::to_iobuf(std::move(impl)));
        r = is_eq(impl, shadow);
        if (!r) {
            return r;
        }
    }
    return testing::AssertionSuccess();
}

template<size_t S>
AssertionResult
pop(fragmented_vector<int, S>& impl, std::vector<int>& shadow, int count) {
    for (int i = 0; i < count; ++i) {
        shadow.pop_back();
        impl.pop_back();
        auto r = is_eq(impl, shadow);
        if (!r) {
            return r;
        }
        impl = serde::from_iobuf<std::decay_t<decltype(impl)>>(
          serde::to_iobuf(std::move(impl)));
        r = is_eq(impl, shadow);
        if (!r) {
            return r;
        }
    }
    return testing::AssertionSuccess();
}

TEST(Vector, PushPop) {
    std::vector<int> shadow;
    fragmented_vector<int> impl;
    EXPECT_TRUE(impl.empty());
    ASSERT_TRUE(push(impl, shadow, 2500));
    EXPECT_TRUE(pop(impl, shadow, 1234));
    ASSERT_TRUE(push(impl, shadow, 123));
    EXPECT_TRUE(pop(impl, shadow, 1389));
    EXPECT_THAT(impl, ElementsAreArray(shadow));
    EXPECT_EQ(impl.empty(), shadow.empty());
    EXPECT_EQ(impl.size(), shadow.size());
}

TEST(Vector, Iterator) {
    std::vector<int> shadow;
    fragmented_vector<int> impl;

    EXPECT_TRUE(push(impl, shadow, 2000));
    for (int i = 0; i < 6000; i++) {
        auto val = random_generators::get_int<int64_t>(0, 4000);

        auto it = std::lower_bound(shadow.begin(), shadow.end(), val);
        auto it2 = std::lower_bound(impl.begin(), impl.end(), val);
        EXPECT_EQ(it == shadow.end(), it2 == impl.end());
        EXPECT_EQ(
          std::distance(shadow.begin(), it), std::distance(impl.begin(), it2));
        EXPECT_EQ(
          std::distance(it, shadow.end()), std::distance(it2, impl.end()));

        it = std::upper_bound(shadow.begin(), shadow.end(), val);
        it2 = std::upper_bound(impl.begin(), impl.end(), val);
        EXPECT_EQ(it == shadow.end(), it2 == impl.end());
        EXPECT_EQ(
          std::distance(shadow.begin(), it), std::distance(impl.begin(), it2));
        EXPECT_EQ(
          std::distance(it, shadow.end()), std::distance(it2, impl.end()));

        it = std::find(shadow.begin(), shadow.end(), val);
        it2 = std::find(impl.begin(), impl.end(), val);
        EXPECT_EQ(it == shadow.end(), it2 == impl.end());
        EXPECT_EQ(
          std::distance(shadow.begin(), it), std::distance(impl.begin(), it2));
        EXPECT_EQ(
          std::distance(it, shadow.end()), std::distance(it2, impl.end()));
    }
}

TEST(Vector, IteratorTypes) {
    using vtype = fragmented_vector<int64_t>;
    using iter = vtype::iterator;
    using citer = vtype::const_iterator;
    auto v = vtype{};

    // const and non-const iterators should be different!
    static_assert(!std::is_same_v<iter, citer>);
    static_assert(std::is_same_v<decltype(v.begin()), iter>);
    static_assert(
      std::is_same_v<decltype(v.cbegin()), decltype(v)::const_iterator>);
    static_assert(std::is_same_v<
                  decltype(std::as_const(v).begin()),
                  decltype(v)::const_iterator>);
}

struct foo {
    int a;
    friend std::ostream& operator<<(std::ostream& os, const foo& f) {
        return os << f.a;
    }
    bool operator==(const foo&) const = default;
};

TEST(Vector, IteratorAccess) {
    using vtype = fragmented_vector<foo>;
    auto vec = vtype{};
    vec.push_back(foo{2});

    EXPECT_EQ(*vec.begin(), foo{2});
    EXPECT_EQ((*vec.begin()).a, 2);
    EXPECT_EQ(vec.begin()->a, 2);
}

TEST(Vector, IteartorArithmetic) {
    auto v = checker<int64_t>::make({0, 1, 2, 3});

    auto b = v->begin();

    EXPECT_EQ(*(b + 0), 0);
    EXPECT_EQ(*(b + 1), 1);
    EXPECT_EQ(*(b + 2), 2);
    EXPECT_EQ(*(b + 3), 3);

    auto e = v->end();

    EXPECT_EQ((e - 0), e);

    EXPECT_EQ(*(e - 1), 3);
    EXPECT_EQ(*(e - 2), 2);
    EXPECT_EQ(*(e - 3), 1);
    EXPECT_EQ(*(e - 4), 0);
}

TEST(Vector, IteratorCmp) {
    auto v = checker<int64_t>::make({0, 1, 2, 3});

    auto b = v->begin();

    EXPECT_EQ(b, b);
    EXPECT_LE(b, b);
    EXPECT_THAT(b, Not(Lt(b)));
    EXPECT_THAT(b, Not(Gt(b)));
    EXPECT_THAT(b, Not(Ne(b)));

    auto b1 = b + 1;

    EXPECT_LE(b, b1);
    EXPECT_LT(b, b1);
    EXPECT_GE(b1, b);
    EXPECT_GT(b1, b);
    EXPECT_NE(b1, b);
}

TEST(Vector, EmptyAfterMove) {
    // Checks that post move, the source vector is empty().
    // This is inline with std::vector guarantees.
    fragmented_vector<int> v1;
    v1.push_back(1);
    EXPECT_FALSE(v1.empty());

    auto v2 = std::move(v1);
    // NOLINTNEXTLINE(bugprone-use-after-move)
    EXPECT_TRUE(v1.empty());
    EXPECT_EQ(v1.begin(), v1.end());

    auto v3(std::move(v2));
    // NOLINTNEXTLINE(bugprone-use-after-move)
    EXPECT_TRUE(v2.empty());
    EXPECT_EQ(v2.begin(), v2.end());
}

TEST(Vector, Sort) {
    vec v;
    v.push_back(3);
    v.push_back(2);
    v.push_back(1);

    std::sort(v.begin(), v.end());

    EXPECT_THAT(v, ElementsAre(1, 2, 3));
}

TEST(Vector, Clear) {
    auto v = checker<int>::make({});
    EXPECT_EQ(v->size(), 0);
    v->push_back(3);
    EXPECT_EQ(v->size(), 1);
    v->push_back(2);
    EXPECT_EQ(v->size(), 2);
    v->push_back(1);
    EXPECT_EQ(v->size(), 3);
    v->clear();
    EXPECT_EQ(v.get(), vec{});
    EXPECT_EQ(v->size(), 0);
    EXPECT_THAT(v.get(), IsEmpty());
    v = checker<int>::make({5, 5, 5, 5});
    EXPECT_EQ(v->size(), 4);
}

TEST(Vector, PopBackN) {
    const int elements = 6;
    for (int i = 0; i <= elements; ++i) {
        std::vector<int> start_values(elements);
        std::iota(start_values.begin(), start_values.end(), 0);
        auto vec = checker<int>::make(start_values);

        vec->pop_back_n(i);

        std::vector<int> expected_values(elements - i);
        std::iota(expected_values.begin(), expected_values.end(), 0);
        EXPECT_EQ(vec->size(), expected_values.size());
        EXPECT_THAT(vec.get(), ElementsAreArray(expected_values));

        if (elements - i > 0) {
            EXPECT_EQ(vec->back(), expected_values.back());
        }
    }
}

TEST(Vector, EraseToEnd) {
    // small fragment size to stress multiple fragments
    fragmented_vector<char, 2> v;

    EXPECT_THAT(v, IsValid());

    v.erase_to_end(v.begin());
    EXPECT_THAT(v, IsValid());
    EXPECT_EQ(v.size(), 0);

    v.erase_to_end(v.end());
    EXPECT_THAT(v, IsValid());
    EXPECT_EQ(v.size(), 0);

    v.push_back(0);
    v.erase_to_end(v.end());
    EXPECT_THAT(v, ElementsAre(0));
    v.erase_to_end(v.begin());
    EXPECT_THAT(v, ElementsAre());

    v.push_back(0);
    v.push_back(1);
    v.push_back(2);
    v.push_back(3);
    EXPECT_THAT(v, ElementsAre(0, 1, 2, 3));
    v.erase_to_end(v.begin() + 1);
    EXPECT_THAT(v, ElementsAre(0));
}

TEST(Vector, FromIterRangeConstructor) {
    std::vector<int> vals{1, 2, 3};

    fragmented_vector<int> fv(vals.begin(), vals.end());

    EXPECT_THAT(fv, IsValid());
    EXPECT_THAT(fv, ElementsAre(1, 2, 3));
}

TEST(Vector, FromInitializerListConstructor) {
    {
        fragmented_vector<int> fv({});

        EXPECT_THAT(fv, IsValid());
        EXPECT_THAT(fv, ElementsAre());
    }

    {
        fragmented_vector<int> fv({1, 2, 3});

        EXPECT_THAT(fv, IsValid());
        EXPECT_THAT(fv, ElementsAre(1, 2, 3));
    }

    {
        chunked_vector<int> fv({1, 2, 3});

        EXPECT_THAT(fv, IsValid());
        EXPECT_THAT(fv, ElementsAre(1, 2, 3));
        // chunked_vector should have a "tight" capacity when constructed
        // from a list
        EXPECT_EQ(fv.capacity(), 3);
    }
}

TEST(ChunkedVector, PushPop) {
    for (int i = 0; i < 100; ++i) {
        chunked_vector<int32_t> vec;
        for (size_t i = 0; i < vec.elements_per_fragment(); ++i) {
            bool push_back = vec.empty() || bool(random_generators::get_int(1));
            if (push_back) {
                vec.push_back(i);
            } else {
                vec.pop_back();
            }
            EXPECT_TRUE(fragmented_vector_validator::validate(vec));
        }
    }
}

TEST(ChunkedVector, PushPopN) {
    for (int i = 0; i < 100; ++i) {
        chunked_vector<int32_t> vec;
        for (size_t i = 0; i < vec.elements_per_fragment(); ++i) {
            // Slight preference to make larger vectors because we could be
            // popping back multiple
            switch (random_generators::get_int(4)) {
            case 0:
            case 1:
            case 2:
                vec.push_back(i);
                break;
            case 3:
                vec.pop_back_n(random_generators::get_int(vec.size()));
                break;
            case 4:
                vec.erase_to_end(
                  vec.begin() + random_generators::get_int(vec.size()));
                break;
            }

            EXPECT_TRUE(fragmented_vector_validator::validate(vec));
        }
    }
}

TEST(ChunkedVector, FirstChunkCapacityDoubles) {
    chunked_vector<int32_t> vec;
    for (size_t i = 0; i < vec.elements_per_fragment(); ++i) {
        vec.push_back(i);
        EXPECT_TRUE(fragmented_vector_validator::validate(vec));
    }
}

TEST(ChunkedVector, ReserveAndPushBack) {
    chunked_vector<int32_t> vec;
    vec.reserve(vec.elements_per_fragment());
    vec.push_back(-1);
    int* initial_location = &vec.front();
    for (size_t i = 0; i < vec.elements_per_fragment(); ++i) {
        vec.push_back(i);
        EXPECT_EQ(initial_location, &vec.front());
    }
}

TEST(ChunkedVector, Reserve) {
    chunked_vector<int32_t> growing_vec;
    for (size_t i = 0; i < chunked_vector<int32_t>::elements_per_fragment();
         ++i) {
        growing_vec.reserve(i);
        EXPECT_EQ(growing_vec.capacity(), i);
        EXPECT_TRUE(fragmented_vector_validator::validate(growing_vec));
        // Also ensure "jumping" to that reserved size does the right thing.
        chunked_vector<int32_t> new_vec;
        new_vec.reserve(i);
        EXPECT_EQ(new_vec.capacity(), i);
        EXPECT_TRUE(fragmented_vector_validator::validate(new_vec));
    }
}

TEST(ChunkedVector, ShrinkToFit) {
    chunked_vector<int32_t> vec;
    vec.reserve(32);
    EXPECT_TRUE(fragmented_vector_validator::validate(vec));
    for (int i = 0; i < 10; ++i) {
        vec.push_back(1);
        EXPECT_TRUE(fragmented_vector_validator::validate(vec));
    }
    vec.shrink_to_fit();
    EXPECT_TRUE(fragmented_vector_validator::validate(vec));
    EXPECT_EQ(vec.capacity(), 10);
}

} // namespace
