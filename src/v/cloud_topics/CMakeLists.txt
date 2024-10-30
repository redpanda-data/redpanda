
v_cc_library(
  NAME cloud_topics_base
  SRCS
    dl_overlay.cc
    logger.cc
    types.cc
  DEPS
    v::model
    v::serde
)

v_cc_library(
  NAME cloud_topics_app
  SRCS
    app.cc
  DEPS
    v::cloud_topics_base
    v::cloud_topics_reconciler
)

add_subdirectory(dl_stm)
add_subdirectory(reconciler)
