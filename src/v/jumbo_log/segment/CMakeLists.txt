enable_clang_tidy()

v_cc_library(
  NAME jumbo_log_segment
  SRCS
    sparse_index.cc
  DEPS
    v::bytes
    v::hashing
    v::ssx
)
