cmake_minimum_required(VERSION 3.22)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

project(
  RedpandaDataTransformExamples
  VERSION 0.1
  LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_compile_options(-Wall)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-exceptions")

include(FetchContent)
set(FETCHCONTENT_QUIET FALSE)

FetchContent_Declare(
  redpanda-transform-sdk
  SOURCE_DIR "${CMAKE_CURRENT_LIST_DIR}/.."
)

FetchContent_MakeAvailable(redpanda-transform-sdk)

add_executable(
  identity_transform
  identity.cc
)
target_link_libraries(
  identity_transform
  Redpanda::transform_sdk
)


add_executable(
  identity_logging_transform
  identity_logging.cc
)
target_link_libraries(
  identity_logging_transform
  Redpanda::transform_sdk
)

add_executable(
  schema_registry_transform
  schema_registry.cc
)
target_link_libraries(
  schema_registry_transform
  Redpanda::transform_sdk
)
