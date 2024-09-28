
v_cc_library(
  NAME cloud_topics
  SRCS
    logger.cc
    types.cc
  DEPS
    v::model
    v::serde
)

add_subdirectory(reconciler)
