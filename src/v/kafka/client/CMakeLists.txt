v_cc_library(
  NAME kafka_client
  SRCS
    assignment_plans.cc
    broker.cc
    brokers.cc
    client.cc
    client_fetch_batch_reader.cc
    configuration.cc
    consumer.cc
    fetcher.cc
    fetch_session.cc
    partitioners.cc
    producer.cc
    topic_cache.cc
    sasl_client.cc
  DEPS
    v::kafka_protocol
    v::ssx
    v::storage # batch builder
    v::cluster # log cert reload helper
  )

add_subdirectory(test)
