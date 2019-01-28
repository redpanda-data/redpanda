include(smfc_generator)
smfc_gen(
    CPP
    TARGET_NAME redpanda_gened
    OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/src/v
    SOURCES redpanda.fbs
    )
# Library needed for service tests
add_library(redpanda_services
  ${redpanda_gened}
  redpanda_service.cc
  )
target_link_libraries(redpanda_services PUBLIC rpchains)
target_include_directories(redpanda_services
  PUBLIC ${CMAKE_CURRENT_BINARY_DIR}
  PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/..
  PUBLIC ${CMAKE_SOURCE_DIR}/src/v
  )

add_executable(redpanda
  ${redpanda_gened}
  main.cc
  )
target_link_libraries(redpanda PUBLIC redpanda_services)

install(TARGETS redpanda DESTINATION bin)
add_subdirectory(api)
add_subdirectory(bamboo)
add_subdirectory(tests)
