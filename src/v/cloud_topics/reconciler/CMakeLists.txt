v_cc_library(
  NAME cloud_topics_reconciler
  SRCS
    range_batch_consumer.cc
    reconciler.cc
  DEPS
    v::bytes
    v::container
    v::json
    v::strings
    v::utils
    v::cluster
)
