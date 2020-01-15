v_cc_library(
  NAME rprandom
  HRDS
    fast_prng.h
    generators.h
)
rp_test(
  UNIT_TEST
  BINARY_NAME basic_fast_random_test
  SOURCES random_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::rprandom
)
