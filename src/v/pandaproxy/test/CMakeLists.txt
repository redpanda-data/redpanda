rp_test(
  UNIT_TEST
  BINARY_NAME pandaproxy_fixture
  SOURCES
    fetch.cc
    list_topics.cc
    produce.cc
    consumer_group.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v::application v::rest_application v::http
  LABELS pandaproxy
)
