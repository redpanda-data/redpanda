#define BOOST_TEST_MODULE xxhash
#include "random/fast_prng.h"

#include <boost/test/unit_test.hpp>

#include <set>
#include <utility>

BOOST_AUTO_TEST_CASE(fast_prng_basic_gen_100_unique_rands) {
    fast_prng rng;
    std::set<uint32_t> test;
    for (auto i = 0; i < 100; ++i) {
        uint32_t x = rng();
        BOOST_CHECK(!test.count(x));
        test.insert(x);
    }
}
