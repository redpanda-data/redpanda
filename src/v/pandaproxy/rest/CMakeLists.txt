seastar_generate_swagger(
  TARGET rest_swagger
  VAR rest_file
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/../api/api-doc/rest.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/../api/api-doc/rest.json.h
)

v_cc_library(
  NAME pandaproxy_rest
  SRCS
    api.cc
    configuration.cc
    handlers.cc
    proxy.cc
  DEPS
    v::pandaproxy_common
    v::pandaproxy_parsing
    v::pandaproxy_json
    v::kafka_client
    v::syschecks
    v::kafka
    v::ssx
    v::utils
  )

add_dependencies(v_pandaproxy_rest rest_swagger)

add_subdirectory(test)
