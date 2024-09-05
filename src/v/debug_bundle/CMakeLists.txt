v_cc_library(
    NAME debug_bundle
    SRCS
      error.cc
      types.cc
    DEPS
      v::utils
)

add_subdirectory(tests)
