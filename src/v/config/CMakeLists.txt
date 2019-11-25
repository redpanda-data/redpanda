add_library(config
  configuration.cc
  base_property.cc
)

target_link_libraries(config
    PUBLIC
    Seastar::seastar
    model
)

add_subdirectory(tests)
