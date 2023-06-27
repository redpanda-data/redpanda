// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "utils/fragmented_vector.h"
#include "vassert.h"

#include <seastar/testing/perf_tests.hh>

#include <boost/test/tools/old/interface.hpp>

#include <algorithm>
#include <random>

struct begin_end_base {
    static auto begin(auto& v) { return std::begin(v); }
    static auto end(auto& v) { return std::end(v); }
};

struct std_vec : begin_end_base {
    template<typename T>
    using type = std::vector<T>;

    template<typename T>
    static T copy(const T& t) {
        return t;
    }
};

struct frag_vec : begin_end_base {
    template<typename T>
    using type = fragmented_vector<T>;

    template<typename T>
    static T copy(const T& t) {
        return t.copy();
    }
};

// struct frag_vec {
//     template<typename T>
//     using type = fragmented_vector<T>;

//     template<typename T>
//     static T copy(const T& t) {
//         return t.copy();
//     }
// };

constexpr size_t ITERS = 400;
// minimum number of total elements, if we go smaller than this the
// test gets less valid as the branch predictor memorizes everything
constexpr size_t MIN_ELEMS = 100000;

template<size_t N, typename V, typename E = std::int64_t, typename F>
size_t work_on_vec(F f) {
    constexpr size_t vec_count = MIN_ELEMS / N + 1;
    using vec_type = V::template type<E>;

    std::mt19937 engine; // we want fixed seed NOLINT
    std::uniform_int_distribution<E> dist;

    std::array<vec_type, vec_count> vecs;

    for (auto& v : vecs) {
        for (size_t i = 0; i < N; ++i) {
            v.push_back(dist(engine));
        }
    }

    vassert(vecs[0].size() == N && vecs.back().size() == N, "bad size");

    for (auto i = 0; i < ITERS; i++) {
        vec_type vtmp(V::copy(vecs[i % vec_count]));
        perf_tests::start_measuring_time();
        f(vtmp);
        perf_tests::stop_measuring_time();
    }

    return ITERS * N; // per element timing
}

template<size_t N, typename V>
auto sort_vector() {
    return work_on_vec<N, V>([](auto& v) { std::sort(v.begin(), v.end()); });
}

template<size_t N, typename V>
auto nth_vector_mid() {
    return work_on_vec<N, V>([](auto& v) {
        auto b = v.begin();
        auto e = v.end();
        std::nth_element(b, e, b + (e - b) / 2);
    });
}

template<size_t N, typename V>
auto nth_vector_end() {
    return work_on_vec<N, V>([](auto& v) {
        auto b = v.begin();
        auto e = v.end();
        std::nth_element(b, e, e - 1);
    });
}

template<size_t N, typename V>
auto nth_vector_end_unchecked() {
    return work_on_vec<N, V>([](auto& v) {
        auto b = v.begin_unchecked();
        auto e = v.end_unchecked();
        std::nth_element(b, e, e - 1);
    });
}

static constexpr size_t ELEM_COUNT = 1000;

PERF_TEST(containers, std_vec_sort) {
    return sort_vector<ELEM_COUNT, std_vec>();
}

PERF_TEST(containers, frag_vec_sort) {
    return sort_vector<ELEM_COUNT, frag_vec>();
}

PERF_TEST(containers, std_vec_nth_mid) {
    return nth_vector_mid<ELEM_COUNT, std_vec>();
}

PERF_TEST(containers, frag_vec_nth_mid) {
    return nth_vector_mid<ELEM_COUNT, frag_vec>();
}

PERF_TEST(containers, std_vec_nth_end) {
    return nth_vector_end<ELEM_COUNT, std_vec>();
}

PERF_TEST(containers, frag_vec_nth_end) {
    return nth_vector_end<ELEM_COUNT, frag_vec>();
}

PERF_TEST(containers, frag_vec_nth_end_unchecked) {
    return nth_vector_end_unchecked<ELEM_COUNT, frag_vec>();
}
