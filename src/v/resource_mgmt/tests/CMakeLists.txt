rp_test(
  UNIT_TEST
  BINARY_NAME resource_mgmt
  SOURCES
    available_memory_test.cc
  LIBRARIES v::seastar_testing_main v::resource_mgmt v::config
  LABELS resource_mgmt
)
