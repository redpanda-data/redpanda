include(smfc_generator)
smfc_gen(
    CPP
    TARGET_NAME redpanda_api_gened
    OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/src/v
    SOURCES ../redpanda.fbs
    )
add_library(redpanda_api
  ${redpanda_api_gened}
  client_opts.cc
  client.cc
  )
target_link_libraries (redpanda_api PUBLIC rpchains)
target_include_directories(redpanda_api
  PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/..
  PUBLIC ${CMAKE_SOURCE_DIR}/src/v
  )


install(FILES client.h DESTINATION include/redpanda)
install(TARGETS redpanda_api DESTINATION lib)
