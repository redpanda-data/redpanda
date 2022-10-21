
v_cc_library(
  NAME features
  SRCS
    feature_table.cc
    feature_migrator.cc
    migrators/cloud_storage_config.cc
    logger.cc
  DEPS
    Seastar::seastar
    v::model
  )
add_subdirectory(tests)
