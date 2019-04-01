find_package(GTest REQUIRED CONFIG)
include(smfc_generator)
smfc_gen(
  CPP
  TARGET_NAME raft_fbs
  OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  SOURCES raft.fbs)


add_library (raft STATIC
  ${raft_fbs}
  raft_log_service.cc
  raft_client_cache.cc
  )
target_include_directories(raft
  PUBLIC ${CMAKE_CURRENT_BINARY_DIR}
  )
target_link_libraries(raft
  rpfs
  )
rp_test(
  UNIT_TEST
  BINARY_NAME raft_client_cache_bitflags
  SOURCES client_cache_bits_tests.cc
  LIBRARIES GTest::gtest raft
  INCLUDES ${PROJECT_SOURCE_DIR}/src/v
)
rp_test(
  INTEGRATION_TEST
  BINARY_NAME raft_client_cache_timeout_test
  SOURCES client_cache_timeout_tests.cc
  LIBRARIES  raft
  INCLUDES ${PROJECT_SOURCE_DIR}/src/v
  ARGS "-c 1"
)
