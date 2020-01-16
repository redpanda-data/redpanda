#include "rpc/deserialize.h"
#include "rpc/serialize.h"
#include "rpc/source.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

// utils
#include "rpc/test/test_types.h"

SEASTAR_THREAD_TEST_CASE(roundtrip_pod) {
    auto in = make_iobuf_input_stream(rpc::serialize(pod{}));
    auto rsource = rpc::source(in);
    auto expected = rpc::deserialize<pod>(rsource).get0();
    auto [x, y, z] = reflection::to_tuple(expected);
    BOOST_REQUIRE_EQUAL(x, 1);
    BOOST_REQUIRE_EQUAL(y, 2);
    BOOST_REQUIRE_EQUAL(z, 3);
}
SEASTAR_THREAD_TEST_CASE(roundtrip_pod_with_checksum) {
    auto in = make_iobuf_input_stream(rpc::serialize(pod{}));
    auto rsource = rpc::source(in);
    auto expected = rpc::deserialize<pod>(rsource).get0();
    auto [x, y, z] = reflection::to_tuple(expected);
    BOOST_REQUIRE_EQUAL(x, 1);
    BOOST_REQUIRE_EQUAL(y, 2);
    BOOST_REQUIRE_EQUAL(z, 3);
    BOOST_REQUIRE_EQUAL(rsource.checksum(), 8345071225529513226);
}
SEASTAR_THREAD_TEST_CASE(roundtrip_packed_struct) {
    auto in = make_iobuf_input_stream(rpc::serialize(very_packed_pod{}));
    auto rsource = rpc::source(in);
    auto expected = rpc::deserialize<very_packed_pod>(rsource).get0();
    auto [x, y] = reflection::to_tuple(expected);
    BOOST_REQUIRE_EQUAL(x, 1);
    BOOST_REQUIRE_EQUAL(y, 2);
}

SEASTAR_THREAD_TEST_CASE(roundtrip_with_fragmented_buffer) {
    std::cout.setf(std::ios::unitbuf);
    auto b = iobuf();
    {
        complex_custom src;
        src.oi.append(ss::temporary_buffer<char>(55));
        rpc::serialize(b, std::move(src));
    }
    const size_t src_size = b.size_bytes();
    ss::input_stream<char> in = make_iobuf_input_stream(std::move(b));
    auto rsource = rpc::source(in);
    auto expected = rpc::deserialize<complex_custom>(rsource).get0();
    BOOST_REQUIRE_EQUAL(55, expected.oi.size_bytes());
}
SEASTAR_THREAD_TEST_CASE(roundtrip_pod_with_vector) {
    auto b = iobuf();
    {
        pod_with_vector src;
        rpc::serialize(b, std::move(src));
    }
    auto in = make_iobuf_input_stream(std::move(b));
    auto rsource = rpc::source(in);
    auto expected = rpc::deserialize<pod_with_vector>(rsource).get0();
    BOOST_REQUIRE_EQUAL(expected.v.size(), 3);
}
SEASTAR_THREAD_TEST_CASE(roundtrip_pod_with_array) {
    auto b = iobuf();
    {
        pod_with_array src;
        rpc::serialize(b, std::move(src));
    }
    auto in = make_iobuf_input_stream(std::move(b));
    auto rsource = rpc::source(in);
    auto expected = rpc::deserialize<pod_with_array>(rsource).get0();
    BOOST_REQUIRE_EQUAL(expected.v.size(), 3);
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
        rpc::serialize(b, std::move(it));
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
    auto in = make_iobuf_input_stream(std::move(b));
    auto rsource = rpc::source(in);
    auto expected = rpc::deserialize<test_rpc_header>(rsource).get0();
    auto b2 = iobuf();
    rpc::serialize(b2, std::move(expected));
    BOOST_CHECK_EQUAL(b2.size_bytes(), expected_size);
}

SEASTAR_THREAD_TEST_CASE(iobuf_roundtrip_test_pod_with_array) {
    auto buf = rpc::serialize(pod_with_array());
    auto res = rpc::deserialize<pod_with_array>(std::move(buf)).get0();
    BOOST_REQUIRE_EQUAL(res.pit.x, 1);
    BOOST_REQUIRE_EQUAL(res.pit.y, 2);
    BOOST_REQUIRE_EQUAL(res.pit.z, 3);
    BOOST_REQUIRE_EQUAL(res.v[0], 1);
    BOOST_REQUIRE_EQUAL(res.v[1], 2);
    BOOST_REQUIRE_EQUAL(res.v[2], 3);
};

SEASTAR_THREAD_TEST_CASE(iobuf_roundtrip_test_pod_with_vector) {
    auto buf = rpc::serialize(pod_with_vector());
    auto res = rpc::deserialize<pod_with_vector>(std::move(buf)).get0();
    BOOST_REQUIRE_EQUAL(res.pit.x, 1);
    BOOST_REQUIRE_EQUAL(res.pit.y, 2);
    BOOST_REQUIRE_EQUAL(res.pit.z, 3);
    BOOST_REQUIRE_EQUAL(res.v[0], 1);
    BOOST_REQUIRE_EQUAL(res.v[1], 2);
    BOOST_REQUIRE_EQUAL(res.v[2], 3);
};
