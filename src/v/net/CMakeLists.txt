v_cc_library(
  NAME net
  SRCS
    batched_output_stream.cc
    dns.cc
  DEPS
    Seastar::seastar
  )
