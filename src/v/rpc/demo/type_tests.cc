// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "reflection/adl.h"
#include "rpc/demo/demo_utils.h"
#include "rpc/demo/types.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

using namespace demo; // NOLINT

SEASTAR_THREAD_TEST_CASE(roundtrip_interspersed) {
    auto b = reflection::to_iobuf(gen_interspersed_request(1 << 20, 1 << 15));
    BOOST_REQUIRE_EQUAL(b.size_bytes(), (1 << 20) + 80 /*80bytes overhead*/);
    auto expected = reflection::adl<interspersed_request>{}.from(std::move(b));
    BOOST_REQUIRE_EQUAL(expected.data._three.y.size_bytes(), (1 << 20) / 8);
}
