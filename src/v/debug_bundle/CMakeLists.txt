v_cc_library(
    NAME debug_bundle
    SRCS
      debug_bundle_service.cc
      error.cc
      probe.cc
      types.cc
      utils.cc
    DEPS
      v::config
      v::utils
)

add_subdirectory(tests)
