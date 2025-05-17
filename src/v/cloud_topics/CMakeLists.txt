
v_cc_library(
  NAME cloud_topics_base
  SRCS
    batcher/aggregator.cc
    batcher/batcher.cc
    core/pipeline_stage.cc
    core/read_pipeline.cc
    core/read_request.cc
    core/serializer.cc
    core/write_pipeline.cc
    core/write_request.cc
    read_path/fetch_request_handler.cc
    read_path/placeholder_extent_reader.cc
    read_path/placeholder_extent.cc
    throttler/throttler.cc
    dl_overlay.cc
    dl_snapshot.cc
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
    app_impl.cc
  DEPS
    v::cloud_topics_base
    v::cloud_topics_reconciler
)

add_subdirectory(dl_stm)
add_subdirectory(reconciler)
