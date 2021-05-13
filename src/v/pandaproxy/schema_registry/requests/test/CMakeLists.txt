rp_test(
  UNIT_TEST
  BINARY_NAME pandaproxy_schema_registry_requests_unit
  SOURCES
    post_subject_versions.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v::pandaproxy_schema_registry v::utils
  LABELS pandaproxy
)
