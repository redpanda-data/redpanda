find_package(Avro)


v_cc_library(
  NAME serde_avro
  SRCS
    parser.cc
  DEPS
    Avro::avro
    Seastar::seastar
    v::bytes
    v::container
    v::utils
)
add_subdirectory(tests)