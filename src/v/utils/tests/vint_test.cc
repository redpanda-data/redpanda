#include "bytes/bytes.h"
#include "utils/vint.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

#include <array>
#include <cstdint>
#include <iostream>
#include <random>

namespace {

void check_roundtrip_sweep(int64_t count) {
    for (int64_t i = -count; i < count; i += 100000) {
        const auto b = vint::to_bytes(i);
        const auto view = bytes_view(b);
        const auto [deserialized, _] = vint::deserialize(view);
        BOOST_REQUIRE_EQUAL(deserialized, i);
        BOOST_REQUIRE_EQUAL(b.size(), vint::vint_size(i));
    }
}

} // namespace

SEASTAR_THREAD_TEST_CASE(sanity_signed_sweep_64) {
    check_roundtrip_sweep(100000000);
}
