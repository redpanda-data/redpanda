v_cc_library(
  NAME json
  SRCS
    json.cc
  DEPS
    Seastar::seastar
    v::model
)