v_cc_library(
  NAME pandaproxy_client
  SRCS
    broker.cc
    client.cc
    configuration.cc
    logger.cc
  DEPS
    v::kafka
    v::ssx
  )
