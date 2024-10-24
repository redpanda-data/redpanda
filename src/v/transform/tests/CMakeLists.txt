v_cc_library(
  NAME
    transform_test_fixture
  HDRS 
    test_fixture.h
  SRCS 
    test_fixture.cc
  DEPS
    v::transform
    Seastar::seastar
    v::model_test_utils
    v::gtest_main
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME
    transform_processor
  SOURCES
    transform_processor_test.cc
  LIBRARIES 
    v::gtest_main
    v::transform_test_fixture
  ARGS "-- -c 1"
  LABELS transform
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME
    transform_manager
  SOURCES
    transform_manager_test.cc
  LIBRARIES 
    v::gtest_main
    v::transform_test_fixture
  ARGS "-- -c 1"
  LABELS transform
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME
    transform_commit_batcher
  SOURCES
    commit_batcher_test.cc
  LIBRARIES 
    v::gtest_main
    v::transform
  ARGS "-- -c 1"
  LABELS transform
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME
    transfer_queue_test
  SOURCES
    transfer_queue_test.cc
  LIBRARIES 
    v::gtest_main
    v::model_test_utils
    v::transform
  ARGS "-- -c 1"
  LABELS transform
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME
    memory_limiter_test
  SOURCES
    memory_limiter_test.cc
  LIBRARIES
    v::gtest_main
    v::model_test_utils
    v::transform
  ARGS "-- -c 1"
  LABELS transform
)
