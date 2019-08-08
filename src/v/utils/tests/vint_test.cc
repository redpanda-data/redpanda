#define BOOST_TEST_MODULE utils

#include "bytes/bytes.h"
#include "utils/vint.h"

#include <boost/test/included/unit_test.hpp>

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

void check_roundtrip_sweep(std::size_t count) {
    auto verify_round_trip = [](vint::value_type value) {
        static bytes encoding_buffer(
          bytes::initialized_later(), vint::max_length);
        const auto size = vint::serialize(
          value, encoding_buffer.begin());
        const auto view = bytes_view(encoding_buffer.data(), size);
        const auto [deserialized, _] = vint::deserialize(view);
        BOOST_REQUIRE_EQUAL(deserialized, value);
    };
    std::uniform_int_distribution<vint::value_type> distribution;
    auto rng = random_generator();
    for (std::size_t i = 0; i < count; ++i) {
        verify_round_trip(distribution(rng));
    }
}

} // namespace

BOOST_AUTO_TEST_CASE(sanity_signed_sweep_32) {
    check_roundtrip_sweep(100'000);
}

BOOST_AUTO_TEST_CASE(sanity_signed_sweep_64) {
    check_roundtrip_sweep(100'000);
}
