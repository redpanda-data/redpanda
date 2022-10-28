// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/hash.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "model/adl_serde.h"
#include "model/fundamental.h"
#include "random/generators.h"
#include "reflection/absl/btree_map.h"
#include "reflection/absl/flat_hash_map.h"
#include "reflection/absl/node_hash_map.h"
#include "reflection/seastar/circular_buffer.h"
#include "reflection/std/map.h"
#include "reflection/std/vector.h"

#include <seastar/testing/thread_test_case.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_map.h>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <map>
#include <vector>

namespace rand_gen = random_generators;

template<typename T>
struct random_type {
    T gen() { return T(); }
};

template<>
struct random_type<int32_t> {
    int32_t gen() { return random_generators::get_int<int32_t>(); }
};

template<>
struct random_type<model::ntp> {
    model::ntp gen() {
        auto ns = rand_gen::gen_alphanum_string(5);
        auto topic = rand_gen::gen_alphanum_string(10);
        auto partition_id = rand_gen::get_int(0, 20);
        return model::ntp(ns, topic, partition_id);
    }
};

template<typename T>
T make_random_collection_vec() {
    const auto size = rand_gen::get_int(0, 5);
    T collection;
    collection.reserve(size);
    for (auto i = 0; i < size; ++i) {
        collection.emplace_back(random_type<typename T::value_type>{}.gen());
    }
    return collection;
}

template<typename T>
T make_random_collection_map() {
    T collection;
    for (auto i = 0; i < rand_gen::get_int(0, 5); ++i) {
        collection.emplace(
          random_type<typename T::key_type>{}.gen(),
          random_type<typename T::mapped_type>{}.gen());
    }
    return collection;
}

template<typename T>
bool ser_deser_verify_hash(T&& type) {
    // Serialize
    iobuf out;
    reflection::async_adl<T>{}.to(out, std::forward<T>(type)).get();
    const auto originals_hash = std::hash<iobuf>{}(out);

    // Deserialize
    iobuf_parser in(std::move(out));
    auto result = reflection::async_adl<T>{}.from(in).get0();

    // Reserialize
    iobuf second_out;
    reflection::async_adl<T>{}.to(second_out, std::move(result)).get();
    const auto seconds_hash = std::hash<iobuf>{}(second_out);
    return originals_hash == seconds_hash;
}

template<typename T>
bool ser_deser_verify(T type) {
    // Serialize
    iobuf out;
    reflection::async_adl<T>{}.to(out, type).get();

    // Deserialize
    iobuf_parser in(std::move(out));
    auto result = reflection::async_adl<T>{}.from(in).get0();
    return result == type;
}

SEASTAR_THREAD_TEST_CASE(test_async_adl_collection_vec) {
    BOOST_REQUIRE_EQUAL(
      true,
      ser_deser_verify_hash(
        make_random_collection_vec<std::vector<model::ntp>>()));
    BOOST_REQUIRE_EQUAL(
      true,
      ser_deser_verify_hash(
        make_random_collection_vec<ss::circular_buffer<model::ntp>>()));
}

SEASTAR_THREAD_TEST_CASE(test_async_adl_collection_map) {
    bool v = ser_deser_verify_hash(
      make_random_collection_map<std::map<int32_t, model::ntp>>());
    bool w = ser_deser_verify_hash(
      make_random_collection_map<absl::btree_map<int32_t, model::ntp>>());
    bool x = ser_deser_verify(
      make_random_collection_map<absl::flat_hash_map<int32_t, model::ntp>>());
    bool y = ser_deser_verify(
      make_random_collection_map<absl::node_hash_map<int32_t, model::ntp>>());

    BOOST_REQUIRE(v);
    BOOST_REQUIRE(w);
    BOOST_REQUIRE(x);
    BOOST_REQUIRE(y);
}
