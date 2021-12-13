v_cc_library(
  NAME v8_engine
  SRCS
    environment.cc
    executor.cc
    script.cc
    data_policy.cc
    data_policy_table.cc
  DEPS
    Seastar::seastar
    v8_monolith
    absl::node_hash_map
    v::rphashing
    v::bytes)

add_subdirectory(tests)
