seastar_generate_swagger(
  TARGET rest_swagger
  VAR rest_file
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/../api/api-doc/rest.json
  OUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/../api/api-doc/
)

v_cc_library(
  NAME pandaproxy_rest
  SRCS
    api.cc
    configuration.cc
    handlers.cc
    proxy.cc
    ${rest_file}
  DEPS
    v::pandaproxy_common
    v::pandaproxy_parsing
    v::pandaproxy_json
    v::kafka_client
    v::kafka_protocol
    v::syschecks
    v::ssx
    v::utils
  )

add_subdirectory(test)
