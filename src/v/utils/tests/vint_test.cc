#define BOOST_TEST_MODULE utils

#include "utils/vint.h"

#include "bytes/bytes.h"

#include <boost/test/unit_test.hpp>

#include <array>
#include <cstdint>
#include <iostream>
#include <random>

namespace {

std::mt19937 random_generator() {
    std::random_device rd;
    // In case of errors, replace the seed with a fixed value to get a deterministic run.
    auto seed = rd();
    std::cout << "Random seed: " << seed << "\n";
    return std::mt19937(seed);
}

template<typename Integer>
void check_roundtrip_sweep(std::size_t count) {
    auto verify_round_trip = [] (Integer value) {
        static bytes encoding_buffer(bytes::initialized_later(), sizeof(Integer));
        const auto size = internal::vint_base<Integer>::serialize(value, encoding_buffer.begin());
        const auto view = bytes_view(encoding_buffer.data(), size);
        const auto deserialized = internal::vint_base<Integer>::deserialize(view);
        BOOST_REQUIRE_EQUAL(deserialized, value);
    };
    std::uniform_int_distribution<Integer> distribution;
    auto rng = random_generator();
    for (std::size_t i = 0; i < count; ++i) {
        verify_round_trip(distribution(rng));
    }
}

}

BOOST_AUTO_TEST_CASE(sanity_signed_sweep_32) {
    check_roundtrip_sweep<int32_t>(100'000);
}

BOOST_AUTO_TEST_CASE(sanity_signed_sweep_64) {
    check_roundtrip_sweep<int64_t>(100'000);
}
