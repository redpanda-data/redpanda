#enable_clang_tidy()

v_cc_library(
  NAME mvlog
  SRCS
    batch_collector.cc
  DEPS
    Seastar::seastar
    v::base
    v::bytes
    v::storage
)
add_subdirectory(tests)
