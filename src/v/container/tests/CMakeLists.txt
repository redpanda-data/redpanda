rp_test(
  UNIT_TEST
  BINARY_NAME container_thread
  SOURCES
    fragmented_vector_async_test.cc
  LIBRARIES v::seastar_testing_main
  ARGS "-- -c 1"
)

rp_test(
  UNIT_TEST
  BINARY_NAME container_unit
  SOURCES
    fragmented_vector_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::random v::serde
)
