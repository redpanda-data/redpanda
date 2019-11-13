add_library(groups STATIC member.cc group.cc)
target_include_directories(groups PUBLIC ${PROJECT_SOURCE_DIR}/src/v)

target_link_libraries(groups
  PUBLIC
    Seastar::seastar
    bytes
    cluster
    model
    storage
    requests
  )
