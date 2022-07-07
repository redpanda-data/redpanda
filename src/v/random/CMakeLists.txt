v_cc_library(
  NAME rprandom
  HRDS
    fast_prng.h
    generators.h
  SRCS
    "generators.cc"
  DEPS
    absl::random_seed_sequences
    Seastar::seastar
)
rp_test(
  UNIT_TEST
  BINARY_NAME basic_fast_random_test
  SOURCES random_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v::bytes v::rprandom
  LABELS random
)
