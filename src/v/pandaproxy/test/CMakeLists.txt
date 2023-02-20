rp_test(
  UNIT_TEST
  BINARY_NAME pandaproxy_unit
  SOURCES
    errors.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::pandaproxy_common v::kafka_protocol
  LABELS pandaproxy
)

rp_test(
  UNIT_TEST
  BINARY_NAME pandaproxy_single_thread
  SOURCES
    one_shot.cc
    kafka_client_cache.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v::rphashing v::http v::pandaproxy_common v::kafka_protocol
  ARGS "-- -c 1"
  LABELS pandaproxy
)

rp_test(
  FIXTURE_TEST
  BINARY_NAME pandaproxy
  SOURCES
    test_config_utils.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v::application
  LABELS pandaproxy
)
