v_cc_library(
  NAME pandaproxy_client
  SRCS
    client.cc
  DEPS
    v::kafka
    v::ssx
  )
