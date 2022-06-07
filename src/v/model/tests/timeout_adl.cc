// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf_parser.h"
#include "cluster/types.h"
#include "model/timeout_clock.h"
#include "reflection/adl.h"

#include <boost/test/unit_test.hpp>

#include <chrono>

BOOST_AUTO_TEST_CASE(test_timeout_clock_is_ms_1) {
    const model::timeout_clock::duration d{std::chrono::milliseconds{-16}};
    iobuf buf;
    reflection::adl<model::timeout_clock::duration>{}.to(buf, d);
    iobuf_parser p{buf.share(0, buf.size_bytes())};
    auto res = reflection::adl<std::chrono::milliseconds>{}.from(p);
    BOOST_REQUIRE_EQUAL(res, d);
};

BOOST_AUTO_TEST_CASE(test_timeout_clock_is_ms_2) {
    model::timeout_clock::duration d{std::chrono::milliseconds{16}};
    iobuf buf;
    reflection::adl<std::chrono::milliseconds>{}.to(
      buf, std::chrono::duration_cast<std::chrono::milliseconds>(d));
    iobuf_parser p{buf.share(0, buf.size_bytes())};
    auto res = reflection::adl<model::timeout_clock::duration>{}.from(p);
    BOOST_REQUIRE_EQUAL(res, d);
};
