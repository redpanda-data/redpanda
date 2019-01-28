include(smfc_generator)
smfc_gen(
    CPP
    TARGET_NAME bmtl_gened
    OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/src/rp
    SOURCES bmtl.fbs
    )
# Library needed for service tests
add_library(bmtl_services
  ${bmtl_gened}
  bmtl_service.cc
  )
target_link_libraries(bmtl_services PUBLIC rpchains)
target_include_directories(bmtl_services
  PUBLIC ${CMAKE_CURRENT_BINARY_DIR}
  PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/..
  PUBLIC ${CMAKE_SOURCE_DIR}/src/rp
  )

add_executable(bmtl
  ${bmtl_gened}
  main.cc
  )
target_link_libraries(bmtl PUBLIC bmtl_services)

install(TARGETS bmtl DESTINATION bin)
add_subdirectory(api)
add_subdirectory(bmtlctl)
add_subdirectory(tests)
