v_cc_library(
  NAME pandaproxy_client
  SRCS
    assignment_plans.cc
    broker.cc
    brokers.cc
    client.cc
    configuration.cc
    consumer.cc
    fetcher.cc
    logger.cc
    producer.cc
  DEPS
    v::kafka
    v::ssx
  )

add_subdirectory(test)
