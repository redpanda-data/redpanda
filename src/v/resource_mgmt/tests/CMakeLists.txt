rp_test(
  UNIT_TEST
  BINARY_NAME resource_mgmt
  SOURCES
    available_memory_test.cc
  LIBRARIES v::seastar_testing_main v::resource_mgmt v::config
  LABELS resource_mgmt
)

# NB: Some of these rely on global state (low watermark of available_memory) so need to run in a separate binary
# They use a non-threadsafe logger so we have to run with -c1
# We set a fixed memory limit which makes them faster and more reliable as they
# are trying to get below a certain limit
rp_test(
        UNIT_TEST
        BINARY_NAME test_memory_sampling
        SOURCES memory_sampling_tests.cc
        DEFINITIONS BOOST_TEST_DYN_LINK
        LIBRARIES v::seastar_testing_main v::application
        LABELS memory_sampling
        SKIP_BUILD_TYPES "Debug"
        ARGS "-- -c1 -m1G"
)
