add_library(syschecks INTERFACE)
target_include_directories(syschecks INTERFACE ${PROJECT_SOURCE_DIR}/src/v)
target_link_libraries(syschecks INTERFACE utils)
