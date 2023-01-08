v_cc_library(
  NAME http_test_utils
  SRCS registered_urls.cc http_imposter.cc
  DEPS Seastar::seastar_testing v::http)

rp_test(
  UNIT_TEST
  BINARY_NAME test_http_client
  SOURCES http_client_test.cc framing_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main Boost::unit_test_framework v::http_test_utils
  ARGS "-- -c 1"
  LABELS http
)

rp_test(
  UNIT_TEST
  BINARY_NAME test_http_imposter
  SOURCES
  http_imposter_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main Boost::unit_test_framework v::http_test_utils
  LABELS http_imposter
  ARGS "-- -c 1"
)
