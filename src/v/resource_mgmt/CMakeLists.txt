v_cc_library(
  NAME resource_mgmt
  SRCS
    available_memory.cc
    memory_sampling.cc
    cpu_profiler.cc
    logger.cc
  DEPS
    Seastar::seastar
  )

v_cc_library(
  NAME storage_resource_mgmt
  SRCS
    storage.cc
  DEPS
    Seastar::seastar
    v::cloud_storage
  )

add_subdirectory(tests)
