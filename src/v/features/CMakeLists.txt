
v_cc_library(
  NAME features
  SRCS
    feature_table.cc
    feature_table_snapshot.cc
    feature_migrator.cc
    migrators/cloud_storage_config.cc
    logger.cc
  DEPS
    Seastar::seastar
    v::model
    v::config
  )

add_dependencies(v_features kafka_codegen_headers)

add_subdirectory(tests)
