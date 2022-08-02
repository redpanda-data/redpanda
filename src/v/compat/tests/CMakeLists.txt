rp_test(
  UNIT_TEST
  BINARY_NAME test_compat
  SOURCES compat_test.cc
  LIBRARIES v::seastar_testing_main v::compat)
