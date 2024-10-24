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
#include "test_utils/randoms.h"

#include <seastar/testing/thread_test_case.hh>

SEASTAR_THREAD_TEST_CASE(collections_interop) {
    auto vector = tests::random_vector(
      []() { return random_generators::gen_alphanum_string(32); }, 1024);
    ss::chunked_fifo<ss::sstring> fifo;
    fragmented_vector<ss::sstring> f_vector;
    std::copy(vector.begin(), vector.end(), std::back_inserter(fifo));
    std::copy(vector.begin(), vector.end(), std::back_inserter(f_vector));

    auto serialized_vector = reflection::to_iobuf(vector);
    auto serialized_fifo = reflection::to_iobuf(std::move(fifo));
    auto serialized_f_vector = reflection::to_iobuf(f_vector.copy());

    // check if serialized representation is the same
    BOOST_REQUIRE(serialized_vector == serialized_fifo);
    BOOST_REQUIRE(serialized_vector == serialized_f_vector);
}
