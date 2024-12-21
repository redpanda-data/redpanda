v_cc_library(
    NAME datalake_translation
    SRCS
        partition_translator.cc
        state_machine.cc
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
