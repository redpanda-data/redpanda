// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "reflection/adl.h"

#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include <seastar/testing/perf_tests.hh>

struct small_t {
    int8_t a = 1;
    // char __a_padding;
    int16_t b = 2;
    int32_t c = 3;
    int64_t d = 4;
};
static_assert(sizeof(small_t) == 16, "one more byte for padding");

PERF_TEST(small, serialize) {
    perf_tests::start_measuring_time();
    auto o = reflection::to_iobuf(small_t{});
    perf_tests::do_not_optimize(o);
    perf_tests::stop_measuring_time();
}
PERF_TEST(small, deserialize) {
    auto b = reflection::to_iobuf(small_t{});
    perf_tests::start_measuring_time();
    auto result = reflection::adl<small_t>().from(std::move(b));
    perf_tests::do_not_optimize(result);
    perf_tests::stop_measuring_time();
}

struct big_t {
    small_t s;
    iobuf data;
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

inline void serialize_big(size_t data_size, size_t chunk_size) {
    big_t b = gen_big(data_size, chunk_size);
    auto o = iobuf();
    perf_tests::start_measuring_time();
    reflection::serialize(o, std::move(b));
    perf_tests::do_not_optimize(o);
    perf_tests::stop_measuring_time();
}

inline void deserialize_big(size_t data_size, size_t chunk_size) {
    big_t b = gen_big(data_size, chunk_size);
    auto o = iobuf();
    reflection::serialize(o, std::move(b));
    perf_tests::start_measuring_time();
    auto result = reflection::adl<big_t>().from(std::move(o));
    perf_tests::do_not_optimize(result);
    perf_tests::stop_measuring_time();
}

PERF_TEST(big_1mb, serialize) {
    serialize_big(1 << 20 /*1MB*/, 1 << 15 /*32KB*/);
}

PERF_TEST(big_1mb, deserialize) {
    return deserialize_big(1 << 20 /*1MB*/, 1 << 15 /*32KB*/);
}

PERF_TEST(big_10mb, serialize) {
    serialize_big(10 << 20 /*10MB*/, 1 << 15 /*32KB*/);
}

PERF_TEST(big_10mb, deserialize) {
    return deserialize_big(10 << 20 /*10MB*/, 1 << 15 /*32KB*/);
}
