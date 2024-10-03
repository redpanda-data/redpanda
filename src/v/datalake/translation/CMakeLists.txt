v_cc_library(
    NAME datalake_translation
    SRCS
        state_machine.cc
    DEPS
        v::model
        v::raft
        Seastar::seastar
)

add_subdirectory(tests)
