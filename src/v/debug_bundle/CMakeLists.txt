v_cc_library(
    NAME debug_bundle
    SRCS
      debug_bundle_service.cc
      error.cc
      types.cc
    DEPS
      v::config
      v::utils
)

add_subdirectory(tests)
