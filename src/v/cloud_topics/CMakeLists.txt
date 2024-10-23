
v_cc_library(
  NAME cloud_topics
  SRCS
    logger.cc
    types.cc
  DEPS
    v::model
    v::serde
    v::cloud_topics_reconciler
)

v_cc_library(
  NAME cloud_topics_app
  SRCS
    app.cc
  DEPS
    v::cloud_topics
)

add_subdirectory(dl_stm)
add_subdirectory(reconciler)
