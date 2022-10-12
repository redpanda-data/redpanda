// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf_parser.h"
#include "model/adl_serde.h"
#include "model/timeout_clock.h"
#include "reflection/adl.h"
#include "utils/to_string.h"

#include <boost/test/data/monomorphic.hpp>
#include <boost/test/data/test_case.hpp>
#include <boost/test/unit_test.hpp>

#include <chrono>

namespace bdata = boost::unit_test::data;
using namespace std::chrono_literals;

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

// In https://github.com/redpanda-data/redpanda/pull/5051
// the implementation of adl<model::timeout_clock::duration> was refactored
// This is roughly the old implementation.
struct adl_legacy_duration {
    using rep = model::timeout_clock::rep;
    using duration = model::timeout_clock::duration;

    void to(iobuf& out, duration dur) {
        // This is a clang bug that cause ss::cpu_to_le to become ambiguous
        // because rep has type of long long
        // adl<rep>{}.to(out, dur.count());
        reflection::adl<uint64_t>{}.to(
          out,
          std::chrono::duration_cast<std::chrono::milliseconds>(dur).count());
    }
    duration from(iobuf_parser& in) {
        // This is a clang bug that cause ss::cpu_to_le to become ambiguous
        // because rep has type of long long
        // auto rp = adl<rep>{}.from(in);
        auto rp = reflection::adl<uint64_t>{}.from(in);
        return std::chrono::duration_cast<duration>(
          std::chrono::milliseconds{rp});
    }
};

constexpr auto durations = std::to_array(
  {0ms, 16ms, -16ms, 16777216ms, -16777216ms});

// Check that the old implementation and the new implementation are equivalent
BOOST_DATA_TEST_CASE(test_timeout_clock_legacy, bdata::make(durations), _d) {
    model::timeout_clock::duration d{_d};
    // Old serialise, new deserialise
    {
        iobuf buf;
        adl_legacy_duration{}.to(buf, d);
        iobuf_parser p{buf.share(0, buf.size_bytes())};
        auto res = reflection::adl<model::timeout_clock::duration>{}.from(p);
        BOOST_REQUIRE_EQUAL(res, d);
    }
    // New serialise, old deserialise
    {
        iobuf buf;
        reflection::adl<model::timeout_clock::duration>{}.to(buf, d);
        iobuf_parser p{buf.share(0, buf.size_bytes())};
        auto res = adl_legacy_duration{}.from(p);
        BOOST_REQUIRE_EQUAL(res, d);
    }
};
