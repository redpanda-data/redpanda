seastar_generate_swagger(
  TARGET schema_registry_swagger
  VAR schema_registry_file
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/../api/api-doc/schema_registry.json
  OUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/../api/api-doc/
)

find_package(Protobuf REQUIRED)
find_package(Avro)

v_cc_library(
  NAME pandaproxy_schema_registry
  SRCS
    api.cc
    configuration.cc
    handlers.cc
    error.cc
    service.cc
    seq_writer.cc
    sharded_store.cc
    types.cc
    avro.cc
    protobuf.cc
    validation.cc
    ${schema_registry_file}
  DEPS
    v::pandaproxy_common
    v::pandaproxy_parsing
    v::pandaproxy_json
    v::kafka_client
    v::kafka_protocol
    v::ssx
    v::utils
    v::pandaproxy_schema_registry_protobuf
    Avro::avro
    Boost::iostreams
    protobuf::libprotobuf
    protobuf::libprotoc
  )

add_subdirectory(test)
add_subdirectory(requests)
add_subdirectory(protobuf)
