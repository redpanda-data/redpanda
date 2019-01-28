smfc_gen(
    CPP
    TARGET_NAME paxos_gened
    OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/src/rp
    SOURCES caspaxos.fbs
    )
add_library(caspaxos STATIC
  ${paxos_gened}
  )
target_link_libraries(caspaxos
  PUBLIC rpfs)
target_include_directories(caspaxos
  PUBLIC ${CMAKE_CURRENT_BINARY_DIR}
  PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/..
  # hack - needed because transitively, we include
  # generated files from this module
  PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/../filesystem
  PUBLIC ${CMAKE_SOURCE_DIR}/src/rp
  )
install(TARGETS caspaxos DESTINATION lib)
