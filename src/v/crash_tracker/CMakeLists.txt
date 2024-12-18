v_cc_library(
  NAME crash_tracker
  SRCS
    api.cc
    service.cc
    logger.cc
  DEPS
    Seastar::seastar
    v::base
    v::bytes
    v::model
    v::serde
    v::hashing
)
