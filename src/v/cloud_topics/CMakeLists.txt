
v_cc_library(
  NAME cloud_topics
  SRCS
    logger.cc
  DEPS
    v::model
    v::serde
)
