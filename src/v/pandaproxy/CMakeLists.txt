set (swagger_deps
  get_topics_names
  get_topics_records
  post_topics_name
  create_consumer
  remove_consumer
  subscribe_consumer
  consumer_fetch
  get_consumer_offsets
  post_consumer_offsets
)

foreach(swagger_dep ${swagger_deps})
  seastar_generate_swagger(
    TARGET ${swagger_dep}_swagger
    VAR ${swagger_dep}_file
    IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/api/api-doc/${swagger_dep}.json
    OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/api/api-doc/${swagger_dep}.json.h
  )
endforeach()

v_cc_library(
  NAME pandaproxy
  SRCS
    configuration.cc
    error.cc
    handlers.cc
    logger.cc
    probe.cc
    proxy.cc
    server.cc
  DEPS
    v::pandaproxy_parsing
    v::pandaproxy_json
    v::kafka_client
    v::syschecks
    v::kafka
    v::ssx
    v::utils
  )

foreach(swagger_dep ${swagger_deps})
  add_dependencies(v_pandaproxy ${swagger_dep}_swagger)
endforeach()

add_subdirectory(json)
add_subdirectory(parsing)
add_subdirectory(test)
