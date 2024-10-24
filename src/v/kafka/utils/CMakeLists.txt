v_cc_library(
    NAME kafka_utils
    SRCS
      txn_reader.cc
    DEPS
      v::kafka
)

add_subdirectory(tests)