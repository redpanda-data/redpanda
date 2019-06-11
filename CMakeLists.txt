cmake_minimum_required(VERSION 3.9.0)
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
project(REDPANDA VERSION "0.1.0" LANGUAGES CXX)
# https://cmake.org/cmake/help/v3.4/policy/CMP0065.html
cmake_policy(SET CMP0065 OLD)

# prefer static linking
#set(CMAKE_FIND_LIBRARY_SUFFIXES .a ${CMAKE_FIND_LIBRARY_SUFFIXES})
set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS 1)
set(CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} -v")

#ninja  1161.66s user 64.94s system 658% cpu 3:06.18 total
#ninja  825.41s user 49.15s system 586% cpu 2:29.21 total
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -gsplit-dwarf -Wl,--gdb-index")
# do not use split-dwarf on release builds because we would have
# to ship additional files to remote servers
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -g -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g -DNDEBUG")
set(CMAKE_CXX_FLAGS_MINSIZEREL "-Os -DNDEBUG")

# options
option(RP_ENABLE_GOLD_LINKER "Enable -fuse-ld=gold" ON)
option(V_MANAGE_DEPS "Use our own 3rdparty.cmake.in" ON)
option (RP_ENABLE_DPDK "Enable DPDK support" OFF)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# compiler specific options
if ("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
    if (RP_ENABLE_GOLD_LINKER)
        message(FATAL_ERROR
        "Gold linker is not supported by Clang, disable RP_ENABLE_GOLD_LINKER")
    endif()
    set(BASE_LD_FLAGS_LIST
        -stdlib=libc++
        -fuse-ld=lld
        -lc++fs
        -lc++experimental)
    set(BASE_CXX_FLAGS_LIST
        -Wno-unused-lambda-capture
        -Wno-unused-command-line-argument
        -D_LIBCPP_ENABLE_CXX17_REMOVED_AUTO_PTR
        -DSEASTAR_USE_STD_OPTIONAL_VARIANT_STRINGVIEW
        -I${CMAKE_CURRENT_BINARY_DIR}/v_deps_install/include/c++/v1
        -stdlib=libc++)
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    if(RP_ENABLE_GOLD_LINKER)
        set(BASE_LD_FLAGS_LIST -fuse-ld=gold)
    endif()
endif()

# join flag lists
string(JOIN " " BASE_CXX_FLAGS ${BASE_CXX_FLAGS_LIST})
string(JOIN " " BASE_LD_FLAGS ${BASE_LD_FLAGS_LIST})

set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} ${BASE_CXX_FLAGS}")

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "RelWithDebInfo" CACHE STRING
      "Choose: Debug, Release, RelWithDebInfo, MinSizeRel." FORCE)
endif()

if(V_MANAGE_DEPS)
  # don't export() the contents to registry
  set(CMAKE_EXPORT_NO_PACKAGE_REGISTRY ON CACHE INTERNAL "" FORCE)
  # disable system level registry /usr/local/share/cmake/*
  set(CMAKE_FIND_PACKAGE_NO_SYSTEM_PACKAGE_REGISTRY ON CACHE INTERNAL "" FORCE)
  # disable user package registry ~/.cmake/*
  set(CMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY ON CACHE INTERNAL "" FORCE)
  set(V_DEPS_BUILD_DIR ${CMAKE_CURRENT_BINARY_DIR}/v_deps_build)
  set(V_DEPS_INSTALL_DIR ${CMAKE_CURRENT_BINARY_DIR}/v_deps_install)
  configure_file(3rdparty.cmake.in ${V_DEPS_BUILD_DIR}/CMakeLists.txt @ONLY)
  execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
    RESULT_VARIABLE result
    WORKING_DIRECTORY ${V_DEPS_BUILD_DIR})
  if(result)
    message(FATAL_ERROR "CMake step for v::deps failed: ${result}")
  endif()
  execute_process(COMMAND ${CMAKE_COMMAND} --build .
    RESULT_VARIABLE result
    WORKING_DIRECTORY ${V_DEPS_BUILD_DIR})
  if(result)
    message(FATAL_ERROR "Build step for v::build failed: ${result}")
  endif()
  # wire up dependency search paths.
  list(APPEND CMAKE_PREFIX_PATH "${V_DEPS_INSTALL_DIR}")
  set(BASE_LD_FLAGS "${BASE_LD_FLAGS} -L${V_DEPS_INSTALL_DIR}/lib")
endif()

set(CMAKE_EXE_LINKER_FLAGS "${BASE_LD_FLAGS}")
set(CMAKE_SHARED_LINKER_FLAGS "${BASE_LD_FLAGS}")
set(CMAKE_MODULE_LINKER_FLAGS "${BASE_LD_FLAGS}")

# Look for libc++ only if we are using Clang
if ("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
    find_package(LibCxx)
endif()

# add code
include(init_cpack)
include(ccache)
include(testing)
include(pgo)
include(set_option)
add_subdirectory(src)
