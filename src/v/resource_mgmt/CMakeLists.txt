v_cc_library(
  NAME resource_mgmt
  SRCS
    available_memory.cc
  DEPS
    Seastar::seastar
  )

v_cc_library(
  NAME storage_resource_mgmt
  SRCS
    storage.cc
  DEPS
    Seastar::seastar
  )

add_subdirectory(tests)
