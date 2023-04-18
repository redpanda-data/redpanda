rp_test(
  FIXTURE_TEST
  BINARY_NAME pandaproxy_rest
  SOURCES
    fetch.cc
    list_topics.cc
    produce.cc
    consumer_group.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v::application v::http v::storage_test_utils
  LABELS pandaproxy
)
