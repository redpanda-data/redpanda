add_library(redpanda_config
  redpanda_config.cc
)

target_link_libraries(config
    PUBLIC
    Seastar::seastar
)

target_include_directories(config
  PUBLIC ${CMAKE_SOURCE_DIR}/src/v
)

add_subdirectory(tests)