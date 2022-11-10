v_cc_library(
  NAME pandaproxy_common
  SRCS
    error.cc
    logger.cc
    probe.cc
    server.cc
    kafka_client_cache.cc
    config_utils.cc
  DEPS
    v::pandaproxy_parsing
    v::pandaproxy_json
    v::kafka_client
    v::syschecks
    v::kafka
    v::ssx
    v::utils
  )

add_subdirectory(test)
add_subdirectory(rest)
add_subdirectory(schema_registry)
add_subdirectory(json)
add_subdirectory(parsing)
