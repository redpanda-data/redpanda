seastar_generate_swagger(
  TARGET schema_registry_swagger
  VAR schema_registry_file
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/../api/api-doc/schema_registry.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/../api/api-doc/schema_registry.json.h
)

find_package(protobuf)

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
  DEPS
    v::pandaproxy_common
    v::pandaproxy_parsing
    v::pandaproxy_json
    v::kafka_client
    v::kafka
    v::ssx
    v::utils
    avrocpp_s
    protobuf::libprotobuf
    protobuf::libprotoc
  )

add_dependencies(v_pandaproxy_schema_registry schema_registry_swagger)

add_subdirectory(test)
add_subdirectory(requests)
