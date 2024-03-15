cmake_minimum_required(VERSION 3.22)

project(
    redpanda-transform-sdk
    VERSION 0.0.0
    DESCRIPTION "Redpanda Data Transforms SDK for C++"
    HOMEPAGE_URL "https://redpanda.com/"
    LANGUAGES CXX
)

if(PROJECT_IS_TOP_LEVEL)
  set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
  set(CMAKE_CXX_STANDARD 23)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
  set(CMAKE_CXX_EXTENSIONS OFF)
  add_compile_options(-Wall)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++ -fno-exceptions")
  add_link_options(-fsanitize=address,leak,undefined)
  add_compile_options(-fsanitize=address,leak,undefined)
  add_executable(test_binary "${CMAKE_CURRENT_SOURCE_DIR}/src/transform_sdk.cc")
  set_target_properties(test_binary PROPERTIES COMPILE_FLAGS "-DREDPANDA_TRANSFORM_SDK_ENABLE_TESTING" )
  target_include_directories(test_binary PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/include")
  enable_testing()
  add_test(NAME sdk_test COMMAND test_binary)
endif()

set_source_files_properties(
  "${CMAKE_CURRENT_SOURCE_DIR}/include/redpanda/transform_sdk.h"
  PROPERTIES LANGUAGE CXX
)

add_library(
  redpanda_transform_sdk
  "${CMAKE_CURRENT_SOURCE_DIR}/src/transform_sdk.cc"
)
target_include_directories(
  redpanda_transform_sdk 
  PUBLIC 
  "${CMAKE_CURRENT_SOURCE_DIR}/include"
)
add_library(Redpanda::transform_sdk ALIAS redpanda_transform_sdk)
