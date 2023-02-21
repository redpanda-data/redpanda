/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "random/generators.h"
#include "serde/serde.h"
#include "utils/fragmented_vector.h"

#include <boost/test/data/monomorphic.hpp>
#include <boost/test/data/test_case.hpp>
#include <boost/test/unit_test.hpp>

#include <initializer_list>
#include <limits>
#include <type_traits>
#include <vector>

using fv_int = fragmented_vector<int>;

static_assert(std::forward_iterator<fv_int::iterator>);
static_assert(std::forward_iterator<fv_int::const_iterator>);

namespace test_details {

struct fragmented_vector_accessor {
    // perform an internal consistency check of the vector structure
    template<typename T, size_t S>
    static void check_consistency(const fragmented_vector<T, S>& v) {
        BOOST_REQUIRE(v._size <= v._capacity);
        BOOST_REQUIRE(v.size() < std::numeric_limits<size_t>::max() / 2);
        BOOST_REQUIRE(v._capacity < std::numeric_limits<size_t>::max() / 2);
        BOOST_REQUIRE(v._root_end <= v._frags.size());

        size_t calc_size = 0, calc_cap = 0;

        for (size_t i = 0; i < v._root_end; ++i) {
            auto& f = v._frags[i];

            calc_size += f.size();
            calc_cap += f.capacity();

            if (i + 1 < v._root_end) {
                if (f.size() < v.elems_per_frag) {
                    throw std::runtime_error(fmt::format(
                      "fragment {} is undersized ({} < {})",
                      i,
                      f.size(),
                      v.elems_per_frag));
                }
            }
        }

        if (calc_size != v.size()) {
            throw std::runtime_error(fmt::format(
              "calculated size is wrong ({} != {})", calc_size, v.size()));
        }

        if (calc_cap != v._capacity) {
            throw std::runtime_error(fmt::format(
              "calculated capacity is wrong ({} != {})",
              calc_size,
              v._capacity));
        }

        for (auto i = v._root_end; i < v._frags.size(); ++i) {
            if (!v._frags[i].empty()) {
                throw std::runtime_error(
                  fmt::format("unused fragments are not empty @frag={}", i));
            }
        }
    }
};
} // namespace test_details

/**
 * Proxy that applies a consistency check before deference
 */
template<typename T, size_t S>
struct checker {
    using underlying = fragmented_vector<T, S>;

    underlying* operator->() {
        test_details::fragmented_vector_accessor::check_consistency(u);
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

template<typename T>
static void
test_equal(std::vector<T>& truth, fragmented_vector<T, 1024>& other) {
    BOOST_REQUIRE(!truth.empty());
    BOOST_REQUIRE_EQUAL_COLLECTIONS(
      truth.begin(), truth.end(), other.begin(), other.end());
    BOOST_REQUIRE_EQUAL(truth.empty(), other.empty());
    BOOST_REQUIRE_EQUAL(truth.size(), other.size());
    BOOST_REQUIRE_EQUAL(truth.back(), other.back());
}

BOOST_DATA_TEST_CASE(
  fragmented_vector_test,
  boost::unit_test::data::make({-1, 0, 2500}),
  reserve_mem) {
    std::vector<int64_t> truth;
    auto other = [&]() -> fragmented_vector<int64_t, 1024> {
        if (reserve_mem < 0) {
            return {};
        }
        return {fragmented_vector_reserve, size_t(reserve_mem)};
    }();

    for (int64_t i = 0; i < 2500; i++) {
        truth.push_back(i);
        other.push_back(i);
        test_equal(truth, other);

        other = serde::from_iobuf<decltype(other)>(
          serde::to_iobuf(std::move(other)));
        test_equal(truth, other);
    }

    for (int64_t i = 0; i < 1234; i++) {
        truth.pop_back();
        other.pop_back();
        test_equal(truth, other);

        other = serde::from_iobuf<decltype(other)>(
          serde::to_iobuf(std::move(other)));
        test_equal(truth, other);
    }

    for (int64_t i = 0; i < 123; i++) {
        truth.push_back(i);
        other.push_back(i);
        test_equal(truth, other);

        other = serde::from_iobuf<decltype(other)>(
          serde::to_iobuf(std::move(other)));
        test_equal(truth, other);
    }

    for (int64_t i = 0; i < 1389; i++) {
        test_equal(truth, other);
        truth.pop_back();
        other.pop_back();

        other = serde::from_iobuf<decltype(other)>(
          serde::to_iobuf(std::move(other)));
    }

    BOOST_REQUIRE_EQUAL(truth.size(), other.size());
    BOOST_REQUIRE_EQUAL(truth.empty(), other.empty());

    for (int i = 0; i < 2000; i++) {
        truth.push_back(random_generators::get_int<int64_t>(1000, 3000));
        other.push_back(truth.back());

        other = serde::from_iobuf<decltype(other)>(
          serde::to_iobuf(std::move(other)));
        test_equal(truth, other);
    }
    BOOST_REQUIRE_EQUAL(truth.size(), 2000);
    test_equal(truth, other);

    BOOST_REQUIRE_EQUAL(
      truth, std::vector<int64_t>(other.begin(), other.end()));

    for (int i = 0; i < 6000; i++) {
        auto val = random_generators::get_int<int64_t>(0, 4000);

        auto it = std::lower_bound(truth.begin(), truth.end(), val);
        auto it2 = std::lower_bound(other.begin(), other.end(), val);
        BOOST_REQUIRE_EQUAL(it == truth.end(), it2 == other.end());
        BOOST_REQUIRE_EQUAL(
          std::distance(truth.begin(), it), std::distance(other.begin(), it2));
        BOOST_REQUIRE_EQUAL(
          std::distance(it, truth.end()), std::distance(it2, other.end()));

        it = std::upper_bound(truth.begin(), truth.end(), val);
        it2 = std::upper_bound(other.begin(), other.end(), val);
        BOOST_REQUIRE_EQUAL(it == truth.end(), it2 == other.end());
        BOOST_REQUIRE_EQUAL(
          std::distance(truth.begin(), it), std::distance(other.begin(), it2));
        BOOST_REQUIRE_EQUAL(
          std::distance(it, truth.end()), std::distance(it2, other.end()));

        it = std::find(truth.begin(), truth.end(), val);
        it2 = std::find(other.begin(), other.end(), val);
        BOOST_REQUIRE_EQUAL(it == truth.end(), it2 == other.end());
        BOOST_REQUIRE_EQUAL(
          std::distance(truth.begin(), it), std::distance(other.begin(), it2));
        BOOST_REQUIRE_EQUAL(
          std::distance(it, truth.end()), std::distance(it2, other.end()));
    }
}

template<typename T = int, size_t S = 8>
static checker<T, S> make(std::initializer_list<T> in) {
    checker<T, S> ret;
    for (auto& e : in) {
        ret->push_back(e);
    }
    return ret;
}

BOOST_AUTO_TEST_CASE(fragmented_vector_iterator_types) {
    using vtype = fragmented_vector<int64_t, 8>;
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

/**
 * Get a fragmented vector for elements of size E, with max_fragment_size F.
 */
template<size_t ES, size_t F>
using sized_frag = fragmented_vector<std::array<char, ES>, F>;

BOOST_AUTO_TEST_CASE(fragmented_vector_fragment_sizing) {
    BOOST_CHECK_EQUAL((sized_frag<7, 32>::elements_per_fragment()), 4);
    BOOST_CHECK_EQUAL((sized_frag<8, 32>::elements_per_fragment()), 4);
    BOOST_CHECK_EQUAL((sized_frag<9, 32>::elements_per_fragment()), 2);
    BOOST_CHECK_EQUAL((sized_frag<31, 32>::elements_per_fragment()), 1);
    BOOST_CHECK_EQUAL((sized_frag<32, 32>::elements_per_fragment()), 1);
}

BOOST_AUTO_TEST_CASE(fragmented_vector_iterator_arithmetic) {
    auto v = make<int64_t, 8>({0, 1, 2, 3});

    auto b = v->begin();

    BOOST_CHECK_EQUAL(*(b + 0), 0);
    BOOST_CHECK_EQUAL(*(b + 1), 1);
    BOOST_CHECK_EQUAL(*(b + 2), 2);
    BOOST_CHECK_EQUAL(*(b + 3), 3);

    auto e = v->end();

    BOOST_CHECK((e - 0) == e);

    BOOST_CHECK_EQUAL(*(e - 1), 3);
    BOOST_CHECK_EQUAL(*(e - 2), 2);
    BOOST_CHECK_EQUAL(*(e - 3), 1);
    BOOST_CHECK_EQUAL(*(e - 4), 0);
}

BOOST_AUTO_TEST_CASE(fragmented_vector_iterator_comparison) {
    auto v = make<int64_t, 8>({0, 1, 2, 3});

    auto b = v->begin();

    BOOST_CHECK(b == b);
    BOOST_CHECK(b <= b);
    BOOST_CHECK(!(b < b));
    BOOST_CHECK(!(b > b));
    BOOST_CHECK(!(b != b));

    auto b1 = b + 1;

    BOOST_CHECK(b <= b1);
    BOOST_CHECK(b < b1);
    BOOST_CHECK(b1 >= b);
    BOOST_CHECK(b1 > b);
    BOOST_CHECK(b1 >= b);
}

BOOST_AUTO_TEST_CASE(fragmented_vector_sort) {
    auto v = make<int64_t, 8>({3, 2, 1});
    auto expected = make<int64_t, 8>({1, 2, 3});

    std::sort(v->begin(), v->end());

    BOOST_CHECK_EQUAL(v, expected);
}

BOOST_DATA_TEST_CASE(
  fragmented_vector_vector_clear,
  boost::unit_test::data::make({false, true}),
  release_mem) {
    auto v = make<int, 8>({});

    BOOST_CHECK_EQUAL(v->size(), 0);

    v->push_back(0);
    BOOST_CHECK_EQUAL(v->size(), 1);

    v->push_back(1);
    BOOST_CHECK_EQUAL(v->size(), 2);

    v->clear(release_mem);
    BOOST_CHECK_EQUAL(v->size(), 0);

    v = make<int, 8>({5, 5, 5, 5});
    BOOST_CHECK_EQUAL(v->size(), 4);

    v.u = std::vector{1, 2, 3};
    BOOST_CHECK_EQUAL(v->size(), 3);
}

BOOST_DATA_TEST_CASE(
  fragmented_vector_vector_assign,
  boost::unit_test::data::make({false, true}),
  use_assign) {
    std::vector vin0{1, 2, 3};
    std::vector vin1{4, 5};

    checker<int, 8> v;
    BOOST_CHECK_EQUAL(v, (make({})));

    if (use_assign) {
        v.get() = std::vector{1};
    } else {
        v.get().copy_from(std::vector{1});
    }
    BOOST_CHECK_EQUAL(v, (make({1})));

    if (use_assign) {
        v.get() = std::vector{2, 3, 4};
    } else {
        v.get().copy_from(std::vector{2, 3, 4});
    }
    BOOST_CHECK_EQUAL(v, (make({2, 3, 4})));
}
