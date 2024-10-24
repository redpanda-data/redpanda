// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "reflection/adl.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>
// utils
#include "test_types.h"

SEASTAR_THREAD_TEST_CASE(roundtrip_pod) {
    auto expected = reflection::adl<pod>{}.from(reflection::to_iobuf(pod{}));
    auto [x, y, z] = reflection::to_tuple(expected);
    BOOST_REQUIRE_EQUAL(x, 1);
    BOOST_REQUIRE_EQUAL(y, 2);
    BOOST_REQUIRE_EQUAL(z, 3);
}

SEASTAR_THREAD_TEST_CASE(roundtrip_with_fragmented_buffer) {
    std::cout.setf(std::ios::unitbuf);
    auto b = iobuf();
    {
        complex_custom src{};
        src.oi.append(ss::temporary_buffer<char>(55));
        reflection::serialize(b, std::move(src));
    }
    auto expected = reflection::adl<complex_custom>{}.from(std::move(b));
    BOOST_REQUIRE_EQUAL(55, expected.oi.size_bytes());
}
SEASTAR_THREAD_TEST_CASE(roundtrip_pod_with_vector) {
    auto expected = reflection::adl<pod_with_vector>{}.from(
      reflection::to_iobuf(pod_with_vector{}));
    BOOST_REQUIRE_EQUAL(expected.v.size(), 3);
}
SEASTAR_THREAD_TEST_CASE(roundtrip_pod_with_array) {
    auto expected = reflection::adl<pod_with_array>{}.from(
      reflection::to_iobuf(pod_with_array{}));
    for (size_t i = 0; i < expected.v.size(); ++i) {
        BOOST_REQUIRE(expected.v[i] != 0);
    }
}
SEASTAR_THREAD_TEST_CASE(roundtrip_iobuf_vector) {
    std::cout.setf(std::ios::unitbuf);
    auto b = iobuf();
    {
        test_rpc_header it;
        std::vector<ss::temporary_buffer<char>> vi;
        vi.push_back(ss::temporary_buffer<char>(87));
        kv x;
        x.k = "foobar";
        x.v = iobuf(std::move(vi));
        it.hdrs.push_back(std::move(x));
        reflection::serialize(b, std::move(it));
    }
    const size_t expected_size =
      /*
        struct kv {
        sstring k;              ---------------  sizeof(int32_t) + 6
        iobuf v; --------  sizeof(int32_t) + 87 bytes
        };
        struct test_rpc_header {
        int32_t size = 42;       ---------------- sizeof(int32_t)
        uint64_t checksum = 66;   ---------------- sizeof(int64_t)
        std::vector<kv> hdrs;    ---------------- sizeof(int32_t)
        };

        Total:  4 + 6 + 4 + 87 + 4 + 8 + 4 ........  117 bytes
      */
      117;

    //  test
    BOOST_CHECK_EQUAL(b.size_bytes(), expected_size);
    auto expected = reflection::adl<test_rpc_header>{}.from(std::move(b));
    auto b2 = iobuf();
    reflection::serialize(b2, std::move(expected));
    BOOST_CHECK_EQUAL(b2.size_bytes(), expected_size);
}

SEASTAR_THREAD_TEST_CASE(iobuf_roundtrip_test_pod_with_array) {
    auto buf = reflection::to_iobuf(pod_with_array());
    auto res = reflection::adl<pod_with_array>{}.from(std::move(buf));
    BOOST_REQUIRE_EQUAL(res.pit.x, 1);
    BOOST_REQUIRE_EQUAL(res.pit.y, 2);
    BOOST_REQUIRE_EQUAL(res.pit.z, 3);
    BOOST_REQUIRE_EQUAL(res.v[0], 1);
    BOOST_REQUIRE_EQUAL(res.v[1], 2);
    BOOST_REQUIRE_EQUAL(res.v[2], 3);
};

SEASTAR_THREAD_TEST_CASE(iobuf_roundtrip_test_pod_with_vector) {
    auto buf = reflection::to_iobuf(pod_with_vector());
    auto res = reflection::adl<pod_with_vector>{}.from(std::move(buf));
    BOOST_REQUIRE_EQUAL(res.pit.x, 1);
    BOOST_REQUIRE_EQUAL(res.pit.y, 2);
    BOOST_REQUIRE_EQUAL(res.pit.z, 3);
    BOOST_REQUIRE_EQUAL(res.v[0], 1);
    BOOST_REQUIRE_EQUAL(res.v[1], 2);
    BOOST_REQUIRE_EQUAL(res.v[2], 3);
};
