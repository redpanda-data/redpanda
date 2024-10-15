v_cc_library(
    NAME datalake_translation
    SRCS
        state_machine.cc
        partition_translator.cc
    DEPS
        v::cluster
        v::model
        v::raft
        Seastar::seastar
)