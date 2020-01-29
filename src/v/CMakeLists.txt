include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# libraries
add_subdirectory(finjector)
add_subdirectory(hashing)
add_subdirectory(random)
add_subdirectory(bytes)
add_subdirectory(compression)
add_subdirectory(utils)
add_subdirectory(syschecks)
add_subdirectory(rpc)
add_subdirectory(model)
add_subdirectory(config)
add_subdirectory(storage)
add_subdirectory(raft)
add_subdirectory(cluster)
add_subdirectory(kafka)
add_subdirectory(reflection)

# main executables
add_subdirectory(redpanda)


rp_test(
  UNIT_TEST
  BINARY_NAME outcome_tests
  SOURCES outcome_tests.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework
  )
