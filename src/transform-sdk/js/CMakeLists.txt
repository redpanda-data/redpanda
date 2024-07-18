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

if(CMAKE_BUILD_TYPE MATCHES Release)
  include(CheckIPOSupported)
  check_ipo_supported(RESULT ltosupported OUTPUT error)
  # NOTE(oren): rather a blunt instrument. CMake doesn't currently
  # offer the ability to customize the LTO setting, and thin LTO is not
  # sufficient for our use case, so we blanket enable full LTO for all
  # targets. Predicated on compiler ID becuase these options are specific
  # to clang.
  if(ltosupported AND (CMAKE_CXX_COMPILER_ID MATCHES "Clang"))
    add_compile_options(-flto=full)
    add_link_options(-flto=full)
  endif()
endif()

FetchContent_Declare(
  quickjs
  GIT_REPOSITORY https://github.com/quickjs-ng/quickjs.git
  GIT_TAG        f227746c6ebe7f0c7ddb0e5152d52cd41f5a7cdd
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

# NOTE(oren): Fall back to standard property if we decided LTO was needed 
# and happen to be building non-clang for some reason.
if (ltosupported AND NOT (CMAKE_CXX_COMPILER_ID MATCHES "Clang"))
  set_property(TARGET redpanda_js_transform PROPERTY INTERPROCEDURAL_OPTIMIZATION ON)
endif()

# Tests
enable_testing()

add_executable(
  js_vm_test
  js_vm_test.cc
)
target_link_libraries(
  js_vm_test
  GTest::gtest_main Redpanda::js_vm GTest::gmock
)

include(GoogleTest)

gtest_discover_tests(js_vm_test)

