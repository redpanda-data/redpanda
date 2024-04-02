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

#
# Configure ccache. Uses a tristate AUTO(default),ON,OFF. When set to AUTO,
# ccache will be preferred if a compiler launcher is not already configured.
# When set to ON, configuration will fail unless ccache will be used.
#
set(Redpanda_ENABLE_CCACHE AUTO CACHE STRING "Enable ccache")
set_property(CACHE Redpanda_ENABLE_CCACHE PROPERTY STRINGS AUTO ON OFF)
if(Redpanda_ENABLE_CCACHE OR Redpanda_ENABLE_CCACHE STREQUAL "AUTO")
  # handle compiler launcher already being set
  if(CMAKE_C_COMPILER_LAUNCHER OR CMAKE_CXX_COMPILER_LAUNCHER)
    if (Redpanda_ENABLE_CCACHE)
      message(FATAL_ERROR "Enabling ccache (ON): compiler launcher set")
    else()
      message(WARNING "Enabling ccache (AUTO): compiler launcher set")
    endif()
  endif()
  find_program(CCACHE_EXECUTABLE ccache)
  if (CCACHE_EXECUTABLE)
    message(STATUS "Building with ccache: ${CCACHE_EXECUTABLE}, CCACHE_DIR=$ENV{CCACHE_DIR}")
    list(PREPEND CMAKE_C_COMPILER_LAUNCHER "${CCACHE_EXECUTABLE}")
    list(PREPEND CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_EXECUTABLE}")
  elseif(Redpanda_ENABLE_CCACHE)
    message(FATAL_ERROR "Enabling ccache (ON): could not find ccache")
  endif()
endif()

#
## make "lld" be the default since a full statically linked build using "ld" will
## often fail due to being oom killed at the end of the build when many large
## targets are being linked in parallel.
#include(CheckLinkerFlag)
#set(Redpanda_LINKER "lld" CACHE STRING "Linker to use")
#set(Redpanda_LINKER_FLAGS "-fuse-ld=${Redpanda_LINKER}")
#check_linker_flag(CXX ${Redpanda_LINKER_FLAGS} HAVE_Redpanda_LINKER_FLAGS)
#if(NOT HAVE_Redpanda_LINKER_FLAGS)
#  message(FATAL_ERROR "Linker ${Redpanda_LINKER} not found or not supported")
#endif()
#add_link_options(${Redpanda_LINKER_FLAGS})
#
## enable sanitizers. will propogate to all targets, including dependencies
#option(Redpanda_ENABLE_SANITIZERS "Enable sanitizers (address, leak, undefined)" OFF)
#if(Redpanda_ENABLE_SANITIZERS)
#  add_link_options(-fsanitize=address,leak,undefined)
#  add_compile_options(-fsanitize=address,leak,undefined)
#endif()
#
#include(dependencies)
#include(v_library)
#include(testing)
#
#option(Redpanda_ENABLE_COVERAGE "Enable coverage" OFF)
#if(Redpanda_ENABLE_COVERAGE)
#  add_compile_options(-O0 -g --coverage)
#  add_link_options(--coverage)
#endif()
#
##
## Configure clang-tidy checks. By default checks will be enabled if the
## clang-tidy tool is found. Otherwise, it can be explicitly disabled. It's
## recommended to use FORCE_ON in contexts such as CI where we want to fail
## configuration if clang-tidy is not available.
##
#set(Redpanda_ENABLE_CLANG_TIDY "ON" CACHE STRING
#  "Enable clang-tidy checks if available. Can be ON, OFF, or FORCE_ON")
#
#if(NOT Redpanda_ENABLE_CLANG_TIDY STREQUAL OFF)
#  if(Redpanda_ENABLE_CLANG_TIDY STREQUAL FORCE_ON)
#    find_program(CLANG_TIDY_COMMAND clang-tidy REQUIRED)
#  else()
#    find_program(CLANG_TIDY_COMMAND clang-tidy)
#  endif()
#  if(CLANG_TIDY_COMMAND)
#    set(Redpanda_ENABLE_CLANG_TIDY ON)
#  else()
#    set(Redpanda_ENABLE_CLANG_TIDY OFF)
#  endif()
#endif()
#
#add_subdirectory(src)
