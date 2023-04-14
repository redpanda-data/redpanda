v_cc_library(
  NAME migrations
  SRCS
    feature_migrator.cc
    cloud_storage_config.cc
  DEPS
    Seastar::seastar
    v::model
    v::config
)
