v_cc_library(
  NAME resource_mgmt
  SRCS
    available_memory.cc
  DEPS
    Seastar::seastar
  )

add_subdirectory(tests)
