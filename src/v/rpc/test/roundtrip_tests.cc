#include "rpc/deserialize.h"
#include "rpc/serialize.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

// utils
#include "rpc/test/bytes_ostream_utils.h"
#include "rpc/test/test_types.h"

SEASTAR_THREAD_TEST_CASE(roundtrip_pod) {
    auto b = bytes_ostream();
    pod src;
    rpc::serialize(b, src);
    auto in = rpc::make_input_stream(std::move(b));
    pod expected;
    // poison the values - after serialization must match `src`
    expected.x = -42;
    expected.y = -42;
    expected.z = -42;
    rpc::deserialize(in, expected).get();
    BOOST_REQUIRE(rpc::to_tuple(src) == rpc::to_tuple(expected));
}
SEASTAR_THREAD_TEST_CASE(roundtrip_packed_struct) {
    auto b = bytes_ostream();
    very_packed_pod src;
    rpc::serialize(b, src);
    auto in = rpc::make_input_stream(std::move(b));
    very_packed_pod expected;
    // poison the values - after serialization must match `src`
    expected.x = -42;
    expected.y = -42;
    rpc::deserialize(in, expected).get();
    BOOST_REQUIRE(rpc::to_tuple(src) == rpc::to_tuple(expected));
}

SEASTAR_THREAD_TEST_CASE(roundtrip_with_fragmented_buffer) {
    std::cout.setf(std::ios::unitbuf);
    auto b = bytes_ostream();
    complex_custom src;
    std::vector<temporary_buffer<char>> v;
    v.emplace_back(temporary_buffer<char>(55));
    std::memset(v[0].get_write(), 0, v[0].size());
    src.oi = std::move(fragmented_temporary_buffer(std::move(v), 55));
    rpc::serialize(b, src);
    const size_t src_size = b.size_bytes();
    input_stream<char> in = rpc::make_input_stream(std::move(b));
    complex_custom expected;
    rpc::deserialize(in, expected).get();
    BOOST_REQUIRE_EQUAL(55, expected.oi.size_bytes());
}
SEASTAR_THREAD_TEST_CASE(roundtrip_pod_with_vector) {
    auto b = bytes_ostream();
    pod_with_vector src;
    rpc::serialize(b, src);
    auto in = rpc::make_input_stream(std::move(b));
    pod_with_vector expected;
    // poison the values - after serialization must match `src`
    expected.v = {};
    rpc::deserialize(in, expected).get();
    BOOST_REQUIRE(rpc::to_tuple(src.pit) == rpc::to_tuple(expected.pit));
    for (size_t i = 0; i < src.v.size(); ++i) {
        BOOST_REQUIRE_EQUAL(src.v[i], expected.v[i]);
    }
}
SEASTAR_THREAD_TEST_CASE(roundtrip_pod_with_array) {
    auto b = bytes_ostream();
    pod_with_array src;
    rpc::serialize(b, src);
    auto in = rpc::make_input_stream(std::move(b));
    pod_with_array expected;
    // poison the values - after serialization must match `src`
    expected.v = {0, 0, 0};
    rpc::deserialize(in, expected).get();
    BOOST_REQUIRE(rpc::to_tuple(src.pit) == rpc::to_tuple(expected.pit));
    for (size_t i = 0; i < src.v.size(); ++i) {
      BOOST_REQUIRE_EQUAL(src.v[i], expected.v[i]);
    }
}
