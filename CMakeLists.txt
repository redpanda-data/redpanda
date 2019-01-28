cmake_minimum_required(VERSION 3.9) # 3.5 required by seastar
list (APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
include (Cooking OPTIONAL)
project(REDPANDA VERSION "0.1.0" LANGUAGES CXX)
# https://cmake.org/cmake/help/v3.4/policy/CMP0065.html
cmake_policy(SET CMP0065 OLD)
# prefer as static as possible libs
# due to a bug w/ libnuma.a causing a:
# `request to allocate mask for invalid number: Invalid argument`
# we cannot set this at this time
# set(CMAKE_FIND_LIBRARY_SUFFIXES .a ${CMAKE_FIND_LIBRARY_SUFFIXES})
set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS 1)
set(CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} -v")
set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -std=gnu++17")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g")
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -g -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g -DNDEBUG")
set(CMAKE_CXX_FLAGS_MINSIZEREL "-Os -DNDEBUG")
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "RelWithDebInfo" CACHE STRING
      "Choose: Debug, Release, RelWithDebInfo, MinSizeRel." FORCE)
endif()
include(init_cpack)
include(ccache)
include(testing)
include(pgo)
add_subdirectory(src)
