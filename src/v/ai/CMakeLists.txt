v_cc_library(
  NAME ai
  SRCS
    service.cc
  DEPS
    Seastar::seastar
    llama
  )
