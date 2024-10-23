v_cc_library(
  NAME cloud_topics_dl_stm
  SRCS
    dl_stm.cc
    dl_stm_factory.cc
  DEPS
    v::cluster
    v::raft
)
