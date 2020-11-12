// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "model/adl_serde.h"
#include "model/fundamental.h"
#include "random/generators.h"
#include "reflection/async_adl.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

namespace rand_gen = random_generators;

model::ntp make_random_ntp() {
    auto ns = rand_gen::gen_alphanum_string(5);
    auto topic = rand_gen::gen_alphanum_string(10);
    auto partition_id = rand_gen::get_int(0, 20);
    return model::ntp(ns, topic, partition_id);
}

std::vector<model::ntp> make_random_collection() {
    const auto size = rand_gen::get_int(0, 5);
    std::vector<model::ntp> ntps;
    ntps.reserve(size);
    for (auto i = 0; i < size; ++i) {
        ntps.emplace_back(make_random_ntp());
    }
    return ntps;
}

SEASTAR_THREAD_TEST_CASE(test_async_adl_collection) {
    using ntp_vec = std::vector<model::ntp>;
    // Serialize
    iobuf out;
    auto original_vec = make_random_collection();
    reflection::async_adl<ntp_vec>{}.to(out, original_vec).get();
    const auto originals_hash = std::hash<iobuf>{}(out);

    // Deserialize
    iobuf_parser in(std::move(out));
    auto result = reflection::async_adl<ntp_vec>{}.from(in).get0();
    BOOST_REQUIRE_EQUAL(original_vec, result);

    // Reserialize
    iobuf second_out;
    reflection::async_adl<ntp_vec>{}.to(second_out, std::move(result)).get();
    const auto seconds_hash = std::hash<iobuf>{}(second_out);
    BOOST_REQUIRE_EQUAL(originals_hash, seconds_hash);
}
