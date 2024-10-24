v_cc_library(
    NAME base
    DEPS
      Seastar::seastar
)
add_subdirectory(tests)
