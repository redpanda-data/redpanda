# Redirect control for internal Redpanda builds
if(VECTORIZED_CMAKE_DIR)
  cmake_minimum_required(VERSION 3.22)
  list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
  include(${VECTORIZED_CMAKE_DIR}/main.cmake)
  return()
endif()

cmake_minimum_required(VERSION 3.24)
project(redpanda LANGUAGES CXX)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# make "lld" be the default since a full statically linked build using "ld" will
# often fail due to being oom killed at the end of the build when many large
# targets are being linked in parallel.
include(CheckLinkerFlag)
set(Redpanda_LINKER "lld" CACHE STRING "Linker to use")
set(Redpanda_LINKER_FLAGS "-fuse-ld=${Redpanda_LINKER}")
check_linker_flag(CXX ${Redpanda_LINKER_FLAGS} HAVE_Redpanda_LINKER_FLAGS)
if(NOT HAVE_Redpanda_LINKER_FLAGS)
  message(FATAL_ERROR "Linker ${Redpanda_LINKER} not found or not supported")
endif()
add_link_options(${Redpanda_LINKER_FLAGS})

# enable sanitizers. will propogate to all targets, including dependencies
option(Redpanda_ENABLE_SANITIZERS "Enable sanitizers (address, leak, undefined)" OFF)
if(Redpanda_ENABLE_SANITIZERS)
  add_link_options(-fsanitize=address,leak,undefined)
  add_compile_options(-fsanitize=address,leak,undefined)
endif()

# We use enums as bitflags in seastar and Redpanda, which
# creating a constexpr enum value that is bitwise combination
# of flags can trigger this warning as the resulting "enum",
# isn't a valid value.
add_compile_options(-Wno-enum-constexpr-conversion)

include(dependencies)
include(v_library)
include(testing)

option(Redpanda_ENABLE_COVERAGE "Enable coverage" OFF)
if(Redpanda_ENABLE_COVERAGE)
  add_compile_options(-O0 -g --coverage)
  add_link_options(--coverage)
endif()

#
# Configure clang-tidy checks. By default checks will be enabled if the
# clang-tidy tool is found. Otherwise, it can be explicitly disabled. It's
# recommended to use FORCE_ON in contexts such as CI where we want to fail
# configuration if clang-tidy is not available.
#
set(Redpanda_ENABLE_CLANG_TIDY "ON" CACHE STRING
  "Enable clang-tidy checks if available. Can be ON, OFF, or FORCE_ON")

if(NOT Redpanda_ENABLE_CLANG_TIDY STREQUAL OFF)
  if(Redpanda_ENABLE_CLANG_TIDY STREQUAL FORCE_ON)
    find_program(CLANG_TIDY_COMMAND clang-tidy REQUIRED)
  else()
    find_program(CLANG_TIDY_COMMAND clang-tidy)
  endif()
  if(CLANG_TIDY_COMMAND)
    set(Redpanda_ENABLE_CLANG_TIDY ON)
  else()
    set(Redpanda_ENABLE_CLANG_TIDY OFF)
  endif()
endif()

add_subdirectory(src)
