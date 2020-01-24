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
