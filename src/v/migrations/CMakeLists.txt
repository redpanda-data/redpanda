v_cc_library(
  NAME migrations
  SRCS
    cloud_storage_config.cc
    feature_migrator.cc
    rbac_migrator.cc
  DEPS
    Seastar::seastar
    v::model
    v::config
    v::compression
)
