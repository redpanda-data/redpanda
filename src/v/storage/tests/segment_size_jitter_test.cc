#include <boost/test/tools/old/interface.hpp>

#include <array>
#define BOOST_TEST_MODULE storage
#include "storage/segment_utils.h"

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_CASE(test_segment_size_jitter_calculation) {
    std::array<size_t, 5> sizes = {1_GiB, 2_GiB, 100_MiB, 300_MiB, 10_GiB};
    for (auto original_size : sizes) {
        for (int i = 0; i < 100; ++i) {
            auto new_sz = storage::internal::jitter_segment_size(
              original_size, 5);
            BOOST_REQUIRE_GE(new_sz, 0.95 * original_size);
            BOOST_REQUIRE_LE(new_sz, 1.05 * original_size);
        }
    }
};
