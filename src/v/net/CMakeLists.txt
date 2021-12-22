v_cc_library(
  NAME net
  SRCS
    batched_output_stream.cc
  DEPS
    Seastar::seastar
  )
