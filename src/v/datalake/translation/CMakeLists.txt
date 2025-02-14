v_cc_library(
    NAME datalake_translation
    SRCS
        deps.cc
        partition_translator_v2.cc
        state_machine.cc
        scheduling_policies.cc
        scheduling.cc
        utils.cc
    DEPS
        v::cluster
        v::datalake_writer
        v::datalake_coordinator
        v::kafka_utils
        v::model
        v::raft
        Seastar::seastar
)

add_subdirectory(tests)
