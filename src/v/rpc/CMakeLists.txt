add_library (rpc STATIC
  types.cc
  netbuf.cc
  server.cc
  client.cc
  connection.cc
  batched_output_stream.cc
  probes.cc
  logger.cc
  )
target_link_libraries(rpc PUBLIC Seastar::seastar bytes rphashing utils)
add_subdirectory(test)
add_subdirectory(demo)
