set(swags
  config
  cluster_config
  raft
  security
  status
  features
  broker
  partition
  hbadger
  transaction
  cluster
  debug
  usage
  shadow_indexing
  transform 
  recovery 
  migration
  debug_bundle)

set(swag_files)
foreach(swag ${swags})
  seastar_generate_swagger(
    TARGET ${swag}_swagger
    VAR ${swag}_swagger_files
    IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/admin/api-doc/${swag}.json
    OUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/admin/api-doc/
  )
  list(APPEND swag_files ${${swag}_swagger_files})
endforeach()

# Client facing utilities
v_cc_library(
  NAME clientutil
  SRCS
    admin/cluster_config_schema_util.cc
    ${cluster_config_swagger_files}
  DEPS
    v::config
)

v_cc_library(
  NAME application
  SRCS
    ${swag_files}
    admin/cluster_config_schema_util.cc
    admin/server.cc
    admin/debug.cc
    admin/transform.cc
    admin/security.cc
    admin/recovery.cc
    admin/transaction.cc
    admin/partition.cc
    admin/usage.cc
    admin/kafka.cc
    admin/util.cc
    admin/migrations.cc
    admin/topics.cc
    admin/data_migration_utils.cc
    admin/debug_bundle.cc
    cli_parser.cc
    application.cc
    monitor_unsafe.cc
  DEPS
    Seastar::seastar
    v::crypto
    v::cluster
    v::datalake
    v::debug_bundle
    v::kafka_recovery
    v::finjector
    v::kafka
    v::kafka_recovery
    v::metrics
    v::migrations
    v::pandaproxy_rest
    v::pandaproxy_schema_registry
    v::storage_resource_mgmt
    v::syschecks
    v::transform
    v::version 
    v::wasm
    v::cloud_topics_reconciler
  )

add_subdirectory(tests)

add_executable(redpanda
    main.cc)
target_link_libraries(redpanda PUBLIC v::application v::raft v::kafka)
set_property(TARGET redpanda PROPERTY POSITION_INDEPENDENT_CODE ON)

if(CMAKE_BUILD_TYPE MATCHES Release)
  include(CheckIPOSupported)
  check_ipo_supported(RESULT ltosupported OUTPUT error)
  if(ltosupported)
    set_property(TARGET redpanda PROPERTY INTERPROCEDURAL_OPTIMIZATION ON)
  endif()
endif()

install(TARGETS redpanda DESTINATION bin)
