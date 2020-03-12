cmake_minimum_required(VERSION 3.9.0)
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
project(REDPANDA VERSION "0.1.0" LANGUAGES CXX)
# https://cmake.org/cmake/help/v3.4/policy/CMP0065.html
cmake_policy(SET CMP0065 OLD)

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS 1)
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -v -Wl,--build-id=sha1")

#ninja  1161.66s user 64.94s system 658% cpu 3:06.18 total
#ninja  825.41s user 49.15s system 586% cpu 2:29.21 total
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -gsplit-dwarf")
set(CMAKE_C_FLAGS_DEBUG "-O0 -gsplit-dwarf")
# do not use split-dwarf on release builds because we would have
# to ship additional files to remote servers
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -g -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g -DNDEBUG")
set(CMAKE_CXX_FLAGS_MINSIZEREL "-Os -DNDEBUG")
set(CMAKE_C_FLAGS_RELEASE "-O2 -g -DNDEBUG")
set(CMAKE_C_FLAGS_RELWITHDEBINFO "-O2 -g -DNDEBUG")
set(CMAKE_C_FLAGS_MINSIZEREL "-Os -DNDEBUG")
set(Boost_USE_STATIC_LIBS OFF CACHE BOOL "")
set(Boost_NO_BOOST_CMAKE ON CACHE BOOL "")

# options
set(RP_MARCH
  "-march=westmere"
  CACHE STRING
  "-march override, use -march=armv8-a+crc+crypto or -march=westmere"
  )
option(V_MANAGE_DEPS "Use our own 3rdparty.cmake.in" ON)
option(V_DEPS_SKIP_BUILD "Do not build dependencies, assume they already exist" OFF)
option(V_DEPS_ONLY "Only build dependencies" OFF)
set(V_DEPS_INSTALL_DIR ${CMAKE_CURRENT_BINARY_DIR}/v_deps_install
  CACHE STRING "Managed dependencies install directory")
option (RP_ENABLE_DPDK "Enable DPDK support" OFF)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(BASE_LD_FLAGS_LIST ${RP_MARCH})
if ("${CMAKE_CXX_COMPILER_ID}" MATCHES "GNU")
  list(APPEND BASE_LD_FLAGS_LIST -fuse-ld=gold)
endif()

if (V_MANAGE_DEPS)
  list(APPEND BASE_LD_FLAGS_LIST
    -L${V_DEPS_INSTALL_DIR}/lib
    -L${V_DEPS_INSTALL_DIR}/lib64
    -I${V_DEPS_INSTALL_DIR}/include
  )
  set(PKG_CONFIG_PATH_LIST
    ${V_DEPS_INSTALL_DIR}/lib64/pkgconfig
    ${V_DEPS_INSTALL_DIR}/share/pkgconfig
    ${V_DEPS_INSTALL_DIR}/lib/pkgconfig
  )
endif()

set(BASE_CXX_FLAGS_LIST -fPIC)
set(BASE_C_FLAGS_LIST -fPIC)

# compiler specific options
if ("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
    list(APPEND BASE_LD_FLAGS_LIST
        -stdlib=libc++
        -fuse-ld=lld
        -lc++experimental)
    if (V_MANAGE_DEPS)
      set(LLVM_LIBCPP_INCLUDE_FLAG "-I${V_DEPS_INSTALL_DIR}/include/c++/v1")
    endif()
    list(APPEND BASE_CXX_FLAGS_LIST
        -Wno-unused-lambda-capture
        -Wno-unused-command-line-argument
        -D_LIBCPP_ENABLE_CXX17_REMOVED_AUTO_PTR
        -DSEASTAR_USE_STD_OPTIONAL_VARIANT_STRINGVIEW
        ${LLVM_LIBCPP_INCLUDE_FLAG}
        --stdlib=libc++)
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    if ("${CMAKE_BUILD_TYPE}" MATCHES "Debug")
        # Add gdb-index option only for GCC and Debug build
        set(CMAKE_CXX_FLAGS_DEBUG "-O0 -gsplit-dwarf -Wl,--gdb-index")
        set(CMAKE_C_FLAGS_DEBUG "-O0 -gsplit-dwarf -Wl,--gdb-index")
    endif()
endif()

# Enable sanitizers for 3rparties when building in debug 
# to prevent false positivies
# https://github.com/google/sanitizers/wiki/AddressSanitizerContainerOverflow#false-positives

if ("${CMAKE_BUILD_TYPE}" MATCHES "Debug")
    list(APPEND BASE_CXX_FLAGS_LIST
            -fsanitize=undefined
            -fsanitize=address
            )
    list(APPEND BASE_C_FLAGS_LIST
            -fsanitize=undefined
            -fsanitize=address
            )
    list(APPEND BASE_LD_FLAGS_LIST
            -fsanitize=undefined
            -fsanitize=address
            )
endif()

# join flag lists
string(JOIN " " BASE_C_FLAGS ${BASE_C_FLAGS_LIST})
string(JOIN " " BASE_CXX_FLAGS ${BASE_CXX_FLAGS_LIST})
string(JOIN " " BASE_LD_FLAGS ${BASE_LD_FLAGS_LIST})
string(JOIN ":" PKG_CONFIG_PATH ${PKG_CONFIG_PATH_LIST})
find_package(PkgConfig REQUIRED)
set(ENV{PKG_CONFIG_PATH}  "${PKG_CONFIG_PATH}")
set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} ${BASE_CXX_FLAGS}")
set(CMAKE_C_FLAGS  "${CMAKE_C_FLAGS} ${BASE_C_FLAGS}")

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "RelWithDebInfo" CACHE STRING
      "Choose: Debug, Release, RelWithDebInfo, MinSizeRel." FORCE)
endif()

if(V_DEPS_ONLY AND NOT V_MANAGE_DEPS)
    message(FATAL_ERROR
    "To build managed dependencies V_MANAGE_DEPS must be enabled")
endif()

if(V_MANAGE_DEPS)
  # don't export() the contents to registry
  set(CMAKE_EXPORT_NO_PACKAGE_REGISTRY ON CACHE INTERNAL "" FORCE)
  # disable system level registry /usr/local/share/cmake/*
  set(CMAKE_FIND_PACKAGE_NO_SYSTEM_PACKAGE_REGISTRY ON CACHE INTERNAL "" FORCE)
  # disable user package registry ~/.cmake/*
  set(CMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY ON CACHE INTERNAL "" FORCE)
  set(V_DEPS_BUILD_DIR ${CMAKE_CURRENT_BINARY_DIR}/v_deps_build)
  configure_file(3rdparty.cmake.in ${V_DEPS_BUILD_DIR}/CMakeLists.txt @ONLY)
  # wire up dependency search paths.
  list(APPEND CMAKE_PREFIX_PATH "${V_DEPS_INSTALL_DIR}")
  set(BASE_LD_FLAGS "${BASE_LD_FLAGS} -L${V_DEPS_INSTALL_DIR}/lib")
  if(NOT V_DEPS_SKIP_BUILD)
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
  endif()
endif()

set(CMAKE_EXE_LINKER_FLAGS "${BASE_LD_FLAGS} -pie")
set(CMAKE_SHARED_LINKER_FLAGS "${BASE_LD_FLAGS}")
set(CMAKE_MODULE_LINKER_FLAGS "${BASE_LD_FLAGS}")

if ("${CMAKE_BUILD_TYPE}" MATCHES "Debug")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=undefined -fsanitize=address")
  set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fsanitize=undefined -fsanitize=address")
  set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -fsanitize=undefined -fsanitize=address")
endif()

# Look for libc++ only if we are using Clang
if ("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
    find_package(LibCxx)
endif()

# from seastar/CMakeLists.txt. unfortunately this snippet doesn't appear to be
# installed along with the rest of seastar.
function (seastar_generate_swagger)
  set (one_value_args TARGET VAR IN_FILE OUT_FILE)
  cmake_parse_arguments (args "" "${one_value_args}" "" ${ARGN})
  get_filename_component (out_dir ${args_OUT_FILE} DIRECTORY)
  if(V_DEPS_ONLY)
    set (generator "${V_DEPS_INSTALL_DIR}/bin/seastar-json2code.py")
  else()
    find_program(GENERATOR "seastar-json2code.py")
    set (generator "${GENERATOR}")
  endif()

  add_custom_command (
    DEPENDS
      ${args_IN_FILE}
      ${generator}
    OUTPUT ${args_OUT_FILE}
    COMMAND ${CMAKE_COMMAND} -E make_directory ${out_dir}
    COMMAND ${generator} -f ${args_IN_FILE} -o ${args_OUT_FILE})

  add_custom_target (${args_TARGET}
    DEPENDS ${args_OUT_FILE})

  set (${args_VAR} ${args_OUT_FILE} PARENT_SCOPE)
endfunction ()

if(V_DEPS_ONLY)
  message("Finished building/installing external project dependencies.")
  return()
endif()

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# add code
include(ccache)
include(testing)
include(pgo)
include(set_option)
include(v_library)
add_subdirectory(src)
