v_cc_library(
    NAME datalake_translation
    SRCS
        partition_translator.cc
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

v_cc_library(
    NAME datalake_translation_probe
    SRCS
        translation_probe.cc
    DEPS
        v::metrics
)

add_subdirectory(tests)
