
v_cc_library(
  NAME features
  SRCS
    feature_table.cc
    feature_table_snapshot.cc
    logger.cc
  DEPS
    Seastar::seastar
    v::model
    v::config
    v::version
    v::security
    v::metrics
  )

add_dependencies(v_features kafka_codegen_headers)

add_subdirectory(tests)
