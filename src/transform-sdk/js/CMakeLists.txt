cmake_minimum_required(VERSION 3.22)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

project(RedpandaDataTransformJavaScriptSDK
    VERSION 0.1
    DESCRIPTION "Redpanda Data Transform JavaScript SDK"
    LANGUAGES C CXX)

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_compile_options(-Wall)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++ -fno-exceptions")

if(Redpanda_ENABLE_SANITIZERS)
  add_link_options(-fsanitize=address,leak,undefined)
  add_compile_options(-fsanitize=address,leak,undefined)
endif()

include(FetchContent)
set(FETCHCONTENT_QUIET FALSE)

FetchContent_Declare(
  quickjs
  GIT_REPOSITORY https://github.com/quickjs-ng/quickjs.git
  GIT_TAG        bb674c0c3b4c141be0e399b4c76e3530066a6dfd
)

FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG        9d43b27f7a873596496a2ea70721b3f9eb82df01
)

FetchContent_Declare(
  redpanda-transform-sdk
  SOURCE_DIR "${CMAKE_CURRENT_LIST_DIR}/../cpp/"
)

FetchContent_MakeAvailable(quickjs redpanda-transform-sdk googletest)

# JavaScript VM library
set_source_files_properties(js_vm.h PROPERTIES LANGUAGE CXX)
add_library(
  redpanda_js_vm
  js_vm.cc
)
target_link_libraries(
  redpanda_js_vm
  qjs
)
add_library(Redpanda::js_vm ALIAS redpanda_js_vm)

# Main entry point that defines JavaScript Data Transforms
add_executable(
  redpanda_js_transform 
  main.cc
)
target_link_libraries(
  redpanda_js_transform
  qjs Redpanda::transform_sdk Redpanda::js_vm
)

# Tests
enable_testing()

add_executable(
  js_vm_test
  js_vm_test.cc
)
target_link_libraries(
  js_vm_test
  GTest::gtest_main Redpanda::js_vm
)

include(GoogleTest)

gtest_discover_tests(js_vm_test)

