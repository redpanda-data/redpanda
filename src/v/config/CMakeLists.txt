v_cc_library(
  NAME config
  SRCS
    configuration.cc
    base_property.cc
  DEPS
    Seastar::seastar
    v::model
)
add_subdirectory(tests)
