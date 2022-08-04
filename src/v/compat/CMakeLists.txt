v_cc_library(
  NAME compat
  SRCS
    run.cc
  DEPS
    Seastar::seastar
    v::json
    v::cluster
    v::utils
    v::raft)

add_subdirectory(tests)
