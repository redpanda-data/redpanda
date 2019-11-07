#include "rpc/demo/demo_utils.h"
#include "rpc/demo/types.h"
#include "rpc/deserialize.h"
#include "rpc/serialize.h"
#include "rpc/source.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

// utils
#include "rpc/test/bytes_ostream_utils.h"

using namespace demo; // NOLINT

SEASTAR_THREAD_TEST_CASE(roundtrip_interspersed) {
    auto b = bytes_ostream();
    rpc::serialize(b, gen_interspersed_request(1 << 20, 1 << 15));
    BOOST_REQUIRE_EQUAL(b.size_bytes(), (1 << 20) + 80 /*80bytes overhead*/);
    auto in = rpc::make_input_stream(std::move(b));
    auto rsource = rpc::source(in);
    auto expected = rpc::deserialize<interspersed_request>(rsource).get0();
    BOOST_REQUIRE_EQUAL(expected.data._three.y.size_bytes(), (1 << 20) / 8);
}
