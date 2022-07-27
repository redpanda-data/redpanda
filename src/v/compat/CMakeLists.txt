v_cc_library(
  NAME compat
  SRCS
    run.cc
  DEPS
    Seastar::seastar
    v::json
    v::utils)

add_subdirectory(tests)
