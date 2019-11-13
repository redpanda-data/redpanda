add_library (model STATIC
  model.cc
  record_batch_reader.cc
  )

target_link_libraries(model
  PUBLIC
  Seastar::seastar
  )

add_subdirectory(tests)
