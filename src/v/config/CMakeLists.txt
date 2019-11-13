add_library(config
  configuration.cc
  base_property.cc
)

target_link_libraries(config
    PUBLIC
    Seastar::seastar
)

target_include_directories(config
  PUBLIC ${CMAKE_SOURCE_DIR}/src/v
)

add_subdirectory(tests)