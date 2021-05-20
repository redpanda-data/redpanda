v_cc_library(
  NAME v8_engine
  SRCS
    environment.cc
  DEPS
    Seastar::seastar
    v8_monolith)
