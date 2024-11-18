find_package(Protobuf REQUIRED)

v_cc_library(
  NAME protobuf_test_utils
  HDRS
    data_generator.h
  SRCS
    data_generator.cc
  DEPS
    protobuf::libprotobuf
    v::random
)
