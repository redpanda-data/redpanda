add_library (model STATIC
  model.cc
  record_batch_reader.cc
  )
target_include_directories(model PUBLIC ${PROJECT_SOURCE_DIR}/src/v)

target_link_libraries(model
  PUBLIC
  Seastar::seastar
  )
