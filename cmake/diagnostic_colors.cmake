if(NOT REDPANDA_NO_COLORS_IN_OUTPUT)
  # Force colored output from compilers even when output is piped (not a tty)
  # as is the case when building with ninja. For reference see these 2 issues:
  # https://github.com/ninja-build/ninja/issues/174
  # https://github.com/ninja-build/ninja/issues/814
  if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    list(APPEND BASE_C_FLAGS_LIST -fdiagnostics-color=always)
    list(APPEND BASE_CXX_FLAGS_LIST -fdiagnostics-color=always)
  elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    list(APPEND BASE_C_FLAGS_LIST -fcolor-diagnostics)
    list(APPEND BASE_CXX_FLAGS_LIST -fcolor-diagnostics)
  endif()
endif()
