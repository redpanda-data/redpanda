cmake_minimum_required(VERSION 3.22)
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

# Redirect control for internal Redpanda builds
if(VECTORIZED_CMAKE_DIR)
  include(${VECTORIZED_CMAKE_DIR}/main.cmake)
  return()
endif()

project(redpanda LANGUAGES CXX)

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

include(dependencies)
include(v_library)
include(testing)

add_subdirectory(src)
