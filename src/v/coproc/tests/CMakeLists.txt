set(fixture_srcs
    utils/event_publisher_utils.cc
    utils/supervisor.cc
    utils/wasm_event_generator.cc
    utils/batch_utils.cc
    fixtures/coproc_cluster_fixture.cc
    fixtures/coproc_test_fixture.cc
    fixtures/coproc_bench_fixture.cc
    fixtures/fiber_mock_fixture.cc
)

set(fixture_deps
    v::coproc v::storage_test_utils v::application
)

rp_test(
  FIXTURE_TEST
  BINARY_NAME coproc_unstable
  SOURCES
    ${fixture_srcs}
    retry_logic_tests.cc
    topic_ingestion_policy_tests.cc
    failure_recovery_tests.cc
    pacemaker_tests.cc
    autocreate_topic_tests.cc
    coproc_bench_tests.cc
    offset_storage_utils_tests.cc
    wasm_event_tests.cc
    event_handler_tests.cc
    event_listener_tests.cc
    kafka_api_materialized_tests.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main ${fixture_deps}
  LABELS coproc disable_on_ci
  )
