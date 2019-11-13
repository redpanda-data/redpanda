add_library(config
  configuration.cc
  base_property.cc
)

target_link_libraries(config
    PUBLIC
    Seastar::seastar
)

add_subdirectory(tests)
