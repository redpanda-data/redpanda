v_cc_library(
  NAME pandaproxy_client
  SRCS
    client.cc
    configuration.cc
    logger.cc
  DEPS
    v::kafka
    v::ssx
  )
