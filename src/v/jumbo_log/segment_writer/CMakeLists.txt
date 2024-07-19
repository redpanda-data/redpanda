enable_clang_tidy()

v_cc_library(
  NAME jumbo_log_segment_writer
  SRCS
    segment_writer.cc
  DEPS
    v::bytes
    v::hashing
    v::jumbo_log_segment
    v::model
    v::ssx
)
