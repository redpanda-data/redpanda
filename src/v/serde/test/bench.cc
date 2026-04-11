// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "serde/envelope.h"
#include "serde/rw/envelope.h"
#include "serde/rw/iobuf.h"

#include <seastar/testing/perf_tests.hh>

#include <utility>
#include <vector>

namespace {

/// Pre-generate N inputs via factory(), then time only the work() loop.
/// Results from work() are collected and destroyed after
/// stop_measuring_time() so that destruction cost is excluded.
template<size_t N, typename Factory, typename Work>
size_t bench_with_setup(Factory factory, Work work) {
    using input_t = decltype(factory());
    using result_t = decltype(work(std::declval<input_t>()));
    std::vector<input_t> inputs;
    inputs.reserve(N);
    for (size_t i = 0; i < N; ++i) {
        inputs.push_back(factory());
    }
    std::vector<result_t> results;
    results.reserve(N);
    perf_tests::start_measuring_time();
    for (auto& input : inputs) {
        results.emplace_back(work(std::move(input)));
    }
    perf_tests::stop_measuring_time();
    perf_tests::do_not_optimize(results);
    return N;
}

struct small_t
  : public serde::
      envelope<small_t, serde::version<3>, serde::compat_version<2>> {
    int8_t a = 1;
    // char __a_padding;
    int16_t b = 2;
    int32_t c = 3;
    int64_t d = 4;

    auto serde_fields() { return std::tie(a, b, c, d); }
};
static_assert(sizeof(small_t) == 16, "one more byte for padding");

static constexpr size_t small_inner_iters = 10000;

} // namespace

PERF_TEST(small, serialize) {
    return bench_with_setup<small_inner_iters>(
      [] { return small_t{}; }, [](small_t s) { return serde::to_iobuf(s); });
}

PERF_TEST(small, deserialize) {
    return bench_with_setup<small_inner_iters>(
      [] { return serde::to_iobuf(small_t{}); },
      [](iobuf&& b) { return serde::from_iobuf<small_t>(std::move(b)); });
}

struct big_t
  : public serde::envelope<big_t, serde::version<3>, serde::compat_version<2>> {
    small_t s;
    iobuf data;
    auto serde_fields() { return std::tie(s, data); }
};

inline big_t gen_big(size_t data_size, size_t chunk_size) {
    const size_t chunks = data_size / chunk_size;
    big_t ret{.s = small_t{}};
    for (size_t i = 0; i < chunks; ++i) {
        auto c = ss::temporary_buffer<char>(chunk_size);
        ret.data.append(std::move(c));
    }
    return ret;
}

static constexpr size_t big_1mb_inner_iters = 100;
static constexpr size_t big_10mb_inner_iters = 10;

template<size_t N>
size_t serialize_big(size_t data_size, size_t chunk_size) {
    return bench_with_setup<N>(
      [=] { return gen_big(data_size, chunk_size); },
      [](big_t&& b) {
          auto o = iobuf();
          serde::write(o, std::move(b));
          return o;
      });
}

template<size_t N>
size_t deserialize_big(size_t data_size, size_t chunk_size) {
    return bench_with_setup<N>(
      [=] {
          auto o = iobuf();
          serde::write(o, gen_big(data_size, chunk_size));
          return o;
      },
      [](iobuf&& o) { return serde::from_iobuf<big_t>(std::move(o)); });
}

PERF_TEST(big_1mb, serialize) {
    return serialize_big<big_1mb_inner_iters>(1 << 20, 1 << 15);
}

PERF_TEST(big_1mb, deserialize) {
    return deserialize_big<big_1mb_inner_iters>(1 << 20, 1 << 15);
}

PERF_TEST(big_10mb, serialize) {
    return serialize_big<big_10mb_inner_iters>(10 << 20, 1 << 15);
}

PERF_TEST(big_10mb, deserialize) {
    return deserialize_big<big_10mb_inner_iters>(10 << 20, 1 << 15);
}
