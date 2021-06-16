rp_test(
  UNIT_TEST
  BINARY_NAME pandaproxy_schema_registry_unit
  SOURCES
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
    one_shot.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main
  ARGS "-- -c 1"
  LABELS pandaproxy
)

rp_test(
  UNIT_TEST
  BINARY_NAME pandaproxy_schema_registry_fixture
  SOURCES
    get_schema_types.cc
    post_subjects_subject_version.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v::application
  # TODO(Ben): Make schema_registry properly sharded
  ARGS "-- -c 1"
  LABELS pandaproxy
)
