find_package(Protobuf REQUIRED)

v_cc_library(
  NAME serde_protobuf
  SRCS
    parser.cc
  DEPS
    Seastar::seastar
    v::bytes
    v::container
    v::utils
    protobuf::libprotobuf
  )
