#define BOOST_TEST_MODULE xxhash
#include "hashing/xx.h"

#include <boost/test/unit_test.hpp>

#include <utility>

BOOST_AUTO_TEST_CASE(incremental_same_as_array) {
    incremental_xxhash64 inc;
    inc.update(1);
    inc.update(2);
    inc.update(42);
    std::array<int, 3> arr = {1, 2, 42};
    BOOST_CHECK_EQUAL(inc.digest(), xxhash_64(arr));
}

BOOST_AUTO_TEST_CASE(digest_idempotency) {
  incremental_xxhash64 inc;
  inc.update(1);
  inc.digest();
  inc.update(2);
  inc.digest();
  inc.update(42);
  inc.digest();
  std::array<int, 3> arr = {1, 2, 42};

  const auto arr_hash = xxhash_64(arr);
  BOOST_CHECK_EQUAL(inc.digest(), arr_hash);
  for (auto i = 0; i < 10; ++i) {
    BOOST_CHECK_EQUAL(inc.digest(), arr_hash);
  }
}
