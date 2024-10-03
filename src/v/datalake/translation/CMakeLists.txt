v_cc_library(
    NAME datalake_translation
    SRCS
        partition_translator.cc
        state_machine.cc
    DEPS
        v::cluster
        v::model
        v::raft
        Seastar::seastar
)

add_subdirectory(tests)
