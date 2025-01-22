rp_test(
  UNIT_TEST
  BINARY_NAME pandaproxy_schema_registry_unit
  SOURCES
    sanitize_avro.cc
    util.cc
    storage.cc
    store.cc
    schema_id_cache.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v_pandaproxy_schema_registry
  LABELS pandaproxy
)

rp_test(
  UNIT_TEST
  BINARY_NAME pandaproxy_schema_registry_multi_thread
  SOURCES
    mt_sharded_store.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v_pandaproxy_schema_registry
  ARGS "-- -c 4"
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
    test_json_schema.cc
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

set(testdata_dir "${CMAKE_CURRENT_SOURCE_DIR}/testdata")
file(GLOB proto_test_files "${testdata_dir}/protobuf/*.proto")

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME pandaproxy_schema_registry_protobuf_rendering
  SOURCES
    protobuf_rendering.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::gtest_main v_pandaproxy_schema_registry
  INPUT_FILES
    "${proto_test_files}"
  ARGS "-- -c 1"
  LABELS pandaproxy
)
