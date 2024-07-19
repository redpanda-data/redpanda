enable_clang_tidy()

v_cc_library(
  NAME jumbo_log_segment_reader
  SRCS
    segment_reader.cc
  DEPS
    v::bytes
    v::hashing
    v::jumbo_log_segment
    v::ssx
)
