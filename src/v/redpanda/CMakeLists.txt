add_subdirectory(admin)

v_cc_library(
  NAME application
  SRCS 
    application.cc
    $<TARGET_OBJECTS:v::admin_server>
  DEPS
    Seastar::seastar
    v::cluster
    v::syschecks
    v::kafka
    v::coproc
    v::pandaproxy_rest
    v::pandaproxy_schema_registry
    v::archival
  )

add_executable(redpanda
    main.cc
  )
target_link_libraries(redpanda PUBLIC v::application v::raft v::kafka)
set_property(TARGET redpanda PROPERTY POSITION_INDEPENDENT_CODE ON)

if(CMAKE_BUILD_TYPE MATCHES Release)
  include(CheckIPOSupported)
  check_ipo_supported(RESULT ltosupported OUTPUT error)
  if(ltosupported)
    set_property(TARGET redpanda PROPERTY INTERPROCEDURAL_OPTIMIZATION ON)
  endif()
endif()
install(TARGETS redpanda DESTINATION bin)
