v_cc_library(
  NAME transform_stm
  SRCS
    transform_offsets_stm.cc
  DEPS
    v::cluster
    v::model
)

