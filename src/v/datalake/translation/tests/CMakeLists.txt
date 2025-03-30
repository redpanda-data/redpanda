set (srcs
    state_machine_test.cc
    scheduler_fixture.cc
    scheduler_tests.cc
    fair_scheduling_policy_tests.cc
    partition_translator_tests.cc
)

rp_test(
  UNIT_TEST
  GTEST
  TIMEOUT 2000
  BINARY_NAME datalake_translation_tests
  SOURCES ${srcs}
  LIBRARIES  v::datalake_translation v::features v::raft_fixture v::gtest_main v::model_test_utils
  LABELS datalake
  ARGS "-- -c 1 -m 2G"
)

rp_test(
  UNIT_TEST
  GTEST
  TIMEOUT 2000
  BINARY_NAME translated_log_offset_tests
  SOURCES translated_log_offset_test.cc
  LIBRARIES  v::datalake_translation v::features v::raft_fixture v::gtest_main v::storage v::storage_test_utils
  LABELS datalake
  ARGS "-- -c 1"
)

rp_test(
  FIXTURE_TEST
  GTEST
  BINARY_NAME gtest_record_multiplexer
  SOURCES gtest_record_multiplexer_test.cc
  LIBRARIES
    v::cloud_io_utils
    v::datalake_writer
    v::datalake_translation
    v::datalake_test_utils
    v::gtest_main
    v::model_test_utils
    v::schema
    v::s3_imposter
  LABELS datalake
  ARGS "-- -c 1"
)

rp_test(
  FIXTURE_TEST
  GTEST
  BINARY_NAME datalake_cloud
  SOURCES
    record_multiplexer_test.cc
    translation_task_test.cc
  LIBRARIES
    Boost::iostreams
    v::datalake_test_utils
    v::datalake_translation_probe
    v::gtest_main
    v::datalake_writer
    v::datalake_translation
    v::cloud_io_utils
    v::s3_imposter
    v::model_test_utils
  LABELS datalake
  ARGS "-- -c 1"
)

rp_test(
  BENCHMARK_TEST
  BINARY_NAME record_multiplexer
  SOURCES record_multiplexer_bench.cc
  LIBRARIES
    Seastar::seastar_perf_testing
    Boost::unit_test_framework
    v::cloud_io_utils
    v::application
    v::datalake_test_utils
    v::iceberg_test_utils
    v::schema
    v::s3_imposter
  ARGS "-c 1 --duration=1 --runs=1 --memory=4G"
  LABELS datalake
)
