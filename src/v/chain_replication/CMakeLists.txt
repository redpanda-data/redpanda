include(smfc_generator)
smfc_gen(
    CPP
    TARGET_NAME cr_gened
    OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/src/v
    SOURCES chain_replication.fbs
    )
add_library (rpchains STATIC
  ${cr_gened}
  chain_replication_service.cc
  smf_specializations.cc
  )
target_link_libraries (rpchains
  PUBLIC rpfs)
target_include_directories(rpchains
  PUBLIC ${CMAKE_CURRENT_BINARY_DIR}
  PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/..
  # hack - needed because transitively, we include
  # generated files from this module
  PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/../filesystem
  PUBLIC ${CMAKE_SOURCE_DIR}/src/v
  )
install(TARGETS rpchains DESTINATION lib)
