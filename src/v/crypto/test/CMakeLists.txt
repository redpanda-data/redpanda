rp_test(
    UNIT_TEST
    GTEST
    BINARY_NAME gtest_crypto
    SOURCES
      digest_tests.cc
      hmac_tests.cc
      key_tests.cc
      random_tests.cc
      signature_tests.cc
    LIBRARIES
      v::crypto
      v::gtest_main
    LABELS crypto
    ARGS "-- -c 1"      
)
