#define BOOST_TEST_MODULE rpc

#include "bytes/bytes_ostream.h"
#include "rpc/arity.h"
#include "rpc/serialize.h"
#include "utils/fragmented_temporary_buffer.h"

#include <boost/test/included/unit_test.hpp>

#include <vector>

struct pod {
    int16_t x = 1;
    int32_t y = 2;
    int64_t z = 3;
};
struct [[gnu::packed]] very_packed_pod {
    unaligned<int16_t> x = 1;
    unaligned<int8_t> y = 2;
};
struct complex_custom {
    pod pit;
    fragmented_temporary_buffer oi;
};
BOOST_AUTO_TEST_CASE(serialize_pod) {
    auto b = bytes_ostream();
    pod it;
    rpc::serialize(b, it);
    BOOST_CHECK_EQUAL(b.size_bytes(), sizeof(it));
}
BOOST_AUTO_TEST_CASE(serialize_packed_struct) {
    auto b = bytes_ostream();
    very_packed_pod it;
    rpc::serialize(b, it);
    BOOST_CHECK_EQUAL(b.size_bytes(), 3);
}

BOOST_AUTO_TEST_CASE(verify_airty) {
    BOOST_CHECK_EQUAL(rpc::arity<pod>(), 3);
    BOOST_CHECK_EQUAL(rpc::arity<complex_custom>(), 2);
    BOOST_CHECK_EQUAL(rpc::arity<very_packed_pod>(), 2);
}
BOOST_AUTO_TEST_CASE(serialize_with_fragmented_buffer) {
    auto b = bytes_ostream();
    complex_custom it;
    std::vector<temporary_buffer<char>> v;
    v.emplace_back(temporary_buffer<char>(55));
    it.oi = std::move(fragmented_temporary_buffer(std::move(v), 55));
    rpc::serialize_packed(b, it);
    BOOST_CHECK_EQUAL(
      b.size_bytes(),
      55 + sizeof(it.pit)
        + sizeof(int32_t) /*size prefix of fragmented_buffer*/);
}
