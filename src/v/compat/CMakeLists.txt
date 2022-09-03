v_cc_library(
  NAME compat
  SRCS
    run.cc
  DEPS
    Seastar::seastar
    v::json
    v::cluster
    v::utils
    v::raft
    v::model_test_utils)

add_subdirectory(tests)
