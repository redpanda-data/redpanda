rp_test(
  UNIT_TEST
  BINARY_NAME pandaproxy_schema_registry_unit
  SOURCES
    sanitize_avro.cc
    util.cc
    storage.cc
    store.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v_pandaproxy_schema_registry
  LABELS pandaproxy
)

rp_test(
  UNIT_TEST
  BINARY_NAME pandaproxy_schema_registry_single_thread
  SOURCES
    sharded_store.cc
    consume_to_store.cc
    compatibility_store.cc
    compatibility_3rdparty.cc
    compatibility_avro.cc
    compatibility_protobuf.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v_pandaproxy_schema_registry
  ARGS "-- -c 1"
  LABELS pandaproxy
)

rp_test(
  FIXTURE_TEST
  BINARY_NAME pandaproxy_schema_registry
  SOURCES
    get_schema_types.cc
    post_subjects_subject_version.cc
    delete_subject_endpoints.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v::application
  LABELS pandaproxy
)
