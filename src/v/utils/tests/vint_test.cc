#include "bytes/bytes.h"
#include "utils/vint.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

#include <array>
#include <cstdint>
#include <iostream>
#include <random>

namespace {

std::mt19937 random_generator() {
    std::random_device rd;
    // In case of errors, replace the seed with a fixed value to get a
    // deterministic run.
    auto seed = rd();
    std::cout << "Random seed: " << seed << "\n";
    return std::mt19937(seed);
}

void check_roundtrip_sweep(int count) {
    auto verify_round_trip = [](vint::value_type value) {
        static auto encoding_buffer = ss::uninitialized_string<bytes>(
          vint::max_length);
        const auto size = vint::serialize(value, encoding_buffer.begin());
        const auto view = bytes_view(encoding_buffer.data(), size);
        const auto [deserialized, _] = vint::deserialize(view);
        BOOST_REQUIRE_EQUAL(deserialized, value);
        BOOST_REQUIRE_EQUAL(size, vint::vint_size(value));
    };
    std::uniform_int_distribution<vint::value_type> distribution;
    auto rng = random_generator();
    for (int i = -100; i < count; ++i) {
        verify_round_trip(distribution(rng));
    }
}

} // namespace

SEASTAR_THREAD_TEST_CASE(sanity_signed_sweep_32) {
    check_roundtrip_sweep(1000);
}

SEASTAR_THREAD_TEST_CASE(sanity_signed_sweep_64) {
    check_roundtrip_sweep(1000);
}
