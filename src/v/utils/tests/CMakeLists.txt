
rp_test(
  UNIT_TEST
  BINARY_NAME vint_test
  SOURCES vint_test.cc
  LIBRARIES v::seastar_testing_main v::bytes
)

rp_test(
  UNIT_TEST
  BINARY_NAME remote_test
  SOURCES remote_test.cc
  LIBRARIES v::seastar_testing_main

  ARGS "-- -c 2"
)
rp_test(
  UNIT_TEST
  BINARY_NAME string_switch_test
  SOURCES string_switch_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework
)
rp_test(
  UNIT_TEST
  BINARY_NAME named_type_test
  SOURCES named_type_tests.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::utils
  )
rp_test(
  UNIT_TEST
  BINARY_NAME constexpr_string_switch
  SOURCES constexpr_string_switch.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework
  )
rp_test(
  UNIT_TEST
  BINARY_NAME retry_test
  SOURCES retry_test.cc
  LIBRARIES v::seastar_testing_main
  ARGS "-- -c 2"
  )

rp_test(
  UNIT_TEST
  BINARY_NAME directory_walker
  SOURCES directory_walker_test.cc
  LIBRARIES v::seastar_testing_main
)

rp_test(
  UNIT_TEST
  BINARY_NAME outcome_utils_test
  SOURCES outcome_utils_test.cc
  LIBRARIES v::seastar_testing_main
)
rp_test(
  UNIT_TEST
  BINARY_NAME tristate_test
  SOURCES tristate_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework
)

rp_test(
  UNIT_TEST
  BINARY_NAME state_crc_file_test
  SOURCES state_crc_file_test.cc
  LIBRARIES v::seastar_testing_main v::utils
)

rp_test(
  UNIT_TEST
  BINARY_NAME base64_test
  SOURCES base64_test.cc
  LIBRARIES v::seastar_testing_main v::utils
)

rp_test(
  UNIT_TEST
  BINARY_NAME timed_mutex_test
  SOURCES timed_mutex_test.cc
  LIBRARIES v::seastar_testing_main v::utils
)
