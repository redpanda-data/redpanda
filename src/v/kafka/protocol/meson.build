subdir('schemata')

libkafka_protocol = library('kafka_protocol',
  'errors.cc',
  'batch_reader.cc',
  'kafka_batch_adapter.cc',
  'logger.cc',
  'flex_versions.cc',
  'types.cc',
  kafka_protocol_h,
  kafka_protocol_cc,
  dependencies: [common, bytes, model, compression, storage],
  implicit_include_directories: false)

kafka_protocol = declare_dependency(
  sources: kafka_protocol_h,
  dependencies: [common, bytes, model, compression, storage],
  link_with: libkafka_protocol)
