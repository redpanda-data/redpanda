libkafka_client = library('kafka_client',
  'assignment_plans.cc',
  'broker.cc',
  'brokers.cc',
  'client.cc',
  'client_fetch_batch_reader.cc',
  'configuration.cc',
  'consumer.cc',
  'fetcher.cc',
  'fetch_session.cc',
  'partitioners.cc',
  'producer.cc',
  'topic_cache.cc',
  'sasl_client.cc',
  implicit_include_directories: false,
  dependencies: [kafka_protocol, config, cluster])

kafka_client = declare_dependency(
  dependencies: [kafka_protocol, config, cluster],
  link_with: libkafka_client)
