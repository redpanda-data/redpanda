project(REDPANDA VERSION "0.1.0" LANGUAGES CXX)
# https://cmake.org/cmake/help/v3.4/policy/CMP0065.html
cmake_policy(SET CMP0065 OLD)

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS 1)

# CCACHE_DIR is initially provided by vtools
set(CCACHE_DIR $ENV{CCACHE_DIR} CACHE PATH "ccache directory")
set(ENV{CCACHE_DIR} ${CCACHE_DIR})
set(REDPANDA_DEPS_INSTALL_DIR ${CMAKE_CURRENT_BINARY_DIR}/deps_install
  CACHE STRING "Managed dependencies install directory")
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_UNITY_BUILD_BATCH_SIZE 10)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
list(APPEND BASE_LD_FLAGS_LIST
  -L${REDPANDA_DEPS_INSTALL_DIR}/lib
  -L${REDPANDA_DEPS_INSTALL_DIR}/lib64
  -fuse-ld=lld)
set(PKG_CONFIG_PATH_LIST
  ${REDPANDA_DEPS_INSTALL_DIR}/lib64/pkgconfig
  ${REDPANDA_DEPS_INSTALL_DIR}/share/pkgconfig
  ${REDPANDA_DEPS_INSTALL_DIR}/lib/pkgconfig
  )

list(APPEND BASE_CXX_FLAGS_LIST -fPIC)
list(APPEND BASE_C_FLAGS_LIST -fPIC)
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

# included here because it modifies the BASE_*_FLAGS_LIST variables used below
include(diagnostic_colors)

# join flag lists
string(JOIN " " BASE_C_FLAGS ${BASE_C_FLAGS_LIST})
string(JOIN " " BASE_CXX_FLAGS ${BASE_CXX_FLAGS_LIST})
string(JOIN " " BASE_LD_FLAGS ${BASE_LD_FLAGS_LIST})
string(JOIN ":" PKG_CONFIG_PATH ${PKG_CONFIG_PATH_LIST})
find_package(PkgConfig REQUIRED)
set(ENV{PKG_CONFIG_PATH}  "${PKG_CONFIG_PATH}")
set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} ${BASE_CXX_FLAGS}")
if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -fcoroutines")
endif()
set(CMAKE_C_FLAGS  "${CMAKE_C_FLAGS} ${BASE_C_FLAGS}")

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "RelWithDebInfo" CACHE STRING
      "Choose: Debug, Release, RelWithDebInfo, MinSizeRel." FORCE)
endif()

if(REDPANDA_DEPS_ONLY AND NOT V_MANAGE_DEPS)
    message(FATAL_ERROR
    "To build managed dependencies V_MANAGE_DEPS must be enabled")
endif()

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${BASE_LD_FLAGS} -pie")
set(CMAKE_SHARED_LINKER_FLAGS "${BASE_LD_FLAGS}")
set(CMAKE_MODULE_LINKER_FLAGS "${BASE_LD_FLAGS}")

# this needs to be here so that CMAKE_<LANG>_COMPILER_LAUNCHER
# is accessible in the configure_file call for the third-party dependencies
include(ccache)
include(distcc)
include(icecc)

# don't export() the contents to registry
set(CMAKE_EXPORT_NO_PACKAGE_REGISTRY ON CACHE INTERNAL "" FORCE)
# disable system level registry /usr/local/share/cmake/*
set(CMAKE_FIND_PACKAGE_NO_SYSTEM_PACKAGE_REGISTRY ON CACHE INTERNAL "" FORCE)
# disable user package registry ~/.cmake/*
set(CMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY ON CACHE INTERNAL "" FORCE)
set(REDPANDA_DEPS_BUILD_DIR ${CMAKE_CURRENT_BINARY_DIR}/deps_build)
# build type specific flags
string(TOUPPER ${CMAKE_BUILD_TYPE} BUILD_TYPE)
set(CMAKE_C_FLAGS_BUILD_TYPE ${CMAKE_C_FLAGS_${BUILD_TYPE}})
set(CMAKE_CXX_FLAGS_BUILD_TYPE ${CMAKE_CXX_FLAGS_${BUILD_TYPE}})
configure_file(cmake/oss.cmake.in ${REDPANDA_DEPS_BUILD_DIR}/CMakeLists.txt @ONLY)
# wire up dependency search paths.
list(APPEND CMAKE_PREFIX_PATH "${REDPANDA_DEPS_INSTALL_DIR}")
set(BASE_LD_FLAGS "${BASE_LD_FLAGS} -L${REDPANDA_DEPS_INSTALL_DIR}/lib")
if(NOT REDPANDA_DEPS_SKIP_BUILD)
  execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
    RESULT_VARIABLE result
    WORKING_DIRECTORY ${REDPANDA_DEPS_BUILD_DIR})
  if(result)
    message(FATAL_ERROR "CMake step for v::deps failed: ${result}")
  endif()
  execute_process(COMMAND ${CMAKE_COMMAND} --build .
    RESULT_VARIABLE result
    WORKING_DIRECTORY ${REDPANDA_DEPS_BUILD_DIR})
  if(result)
    message(FATAL_ERROR "Build step for v::build failed: ${result}")
  endif()
endif()

# from seastar/CMakeLists.txt. unfortunately this snippet doesn't appear to be
# installed along with the rest of seastar.
function (seastar_generate_swagger)
  set (one_value_args TARGET VAR IN_FILE OUT_FILE)
  cmake_parse_arguments (args "" "${one_value_args}" "" ${ARGN})
  get_filename_component (out_dir ${args_OUT_FILE} DIRECTORY)
  if(REDPANDA_DEPS_ONLY)
    set (generator "${REDPANDA_DEPS_INSTALL_DIR}/bin/seastar-json2code.py")
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

if(REDPANDA_DEPS_ONLY)
  message("Finished building/installing external project dependencies.")
  return()
endif()

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

find_package(Valgrind REQUIRED)

# add code
include(testing)
include(v_library)
add_subdirectory(src)
