seastar_generate_swagger(
  TARGET schema_registry_swagger
  VAR schema_registry_file
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/../api/api-doc/schema_registry.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/../api/api-doc/schema_registry.json.h
)

v_cc_library(
  NAME pandaproxy_schema_registry
  SRCS
    client_fetch_batch_reader.cc
    configuration.cc
    handlers.cc
    error.cc
    service.cc
    avro.cc
  DEPS
    v::pandaproxy_common
    v::pandaproxy_parsing
    v::pandaproxy_json
    v::kafka_client
    v::kafka
    v::ssx
    v::utils
    avrocpp_s
  )

add_dependencies(v_pandaproxy_schema_registry schema_registry_swagger)

add_subdirectory(test)
add_subdirectory(requests)
