include(smfc_generator)
smfc_gen(
    CPP
    TARGET_NAME bmtl_api_gened
    OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/src/rp
    SOURCES ../bmtl.fbs
    )
add_library(bmtl_api
  ${bmtl_api_gened}
  client_opts.cc
  client.cc
  )
target_link_libraries (bmtl_api PUBLIC rpchains)
target_include_directories(bmtl_api
  PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/..
  PUBLIC ${CMAKE_SOURCE_DIR}/src/rp
  )


install(FILES client.h DESTINATION include/bmtl)
install(TARGETS bmtl_api DESTINATION lib)
