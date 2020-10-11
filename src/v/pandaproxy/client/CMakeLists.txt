v_cc_library(
  NAME pandaproxy_client
  SRCS
    broker.cc
    brokers.cc
    client.cc
    configuration.cc
    logger.cc
  DEPS
    v::kafka
    v::ssx
  )

add_subdirectory(test)
