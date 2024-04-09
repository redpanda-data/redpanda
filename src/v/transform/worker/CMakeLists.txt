v_cc_library(
  NAME transform_worker
  SRCS
    worker.cc
    main.cc
  DEPS
    v::transform_worker_rpc
    v::rpc
    v::version
    v::model
    v::syschecks
    v::wasm
)

add_subdirectory(rpc)

add_executable(transform_worker worker.cc)
target_link_libraries(transform_worker PUBLIC v::transform_worker)
set_property(TARGET transform_worker PROPERTY POSITION_INDEPENDENT_CODE ON)

if(CMAKE_BUILD_TYPE MATCHES Release)
  include(CheckIPOSupported)
  check_ipo_supported(RESULT ltosupported OUTPUT error)
  if(ltosupported)
    set_property(TARGET transform_worker PROPERTY INTERPROCEDURAL_OPTIMIZATION ON)
  endif()
endif()

install(TARGETS transform_worker DESTINATION bin)
