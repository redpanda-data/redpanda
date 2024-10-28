v_cc_library(
  NAME cloud_topics_dl_stm
  SRCS
    dl_stm_factory.cc
    dl_stm_state.cc
    dl_stm.cc
  DEPS
    v::cluster
    v::raft
)
