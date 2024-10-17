include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

add_compile_options(-Wall)
add_compile_definitions(FMT_DEPRECATED_OSTREAM)

#
# When enabled clang-tidy will be invoked along side the compiler, and
# clang-tidy warnings will be reported as errors.
#
option(REDPANDA_RUN_CLANG_TIDY "Enable clang-tidy checks" OFF)
if(REDPANDA_RUN_CLANG_TIDY)
    find_program(CLANG_TIDY_COMMAND clang-tidy
        HINTS ${PROJECT_SOURCE_DIR}/vbuild/llvm/install/bin)
    if(NOT CLANG_TIDY_COMMAND)
        message(FATAL_ERROR "Could not find clang-tidy program")
    endif()
    set(clang_tidy_checks
        -*
        bugprone-use-after-move
        bugprone-assert-side-effect
        bugprone-assignment-in-if-condition
        bugprone-dangling-handle
        bugprone-sizeof-container
        bugprone-stringview-nullptr
        bugprone-unused-return-value
        bugprone-suspicious-string-compare
        # TODO: Reenable this after fixing all the noise (and nuking ADL)
        # cppcoreguidelines-rvalue-reference-param-not-moved
        misc-unused-using-decls
        misc-unused-alias-decls
        modernize-redundant-void-arg
        performance-implicit-conversion-in-loop
        performance-trivially-destructible
        performance-no-automatic-move
    )
    list(JOIN clang_tidy_checks "," clang_tidy_checks)
    set(CMAKE_CXX_CLANG_TIDY ${CLANG_TIDY_COMMAND}
        -warnings-as-errors=* -checks=${clang_tidy_checks})
    unset(clang_tidy_checks)
endif()

#
# The `enable_clang_tidy` macro should be used to turn on clang-tidy checks in
# sub-directories that are clang-tidy clean according to .clang-tidy-strict and
# .clang-tidy-strict-tests. These two files should also be symlinked with the
# standard name .clang-tidy into the compliant sub-directories.
#
# The clang-tidy checks above are being phased out and are currently only used
# in the buildkite private build.
#
macro(enable_clang_tidy)
  if(Redpanda_ENABLE_CLANG_TIDY)
    set(CMAKE_CXX_CLANG_TIDY ${CLANG_TIDY_COMMAND} -warnings-as-errors=*)
  endif()
endmacro()

# Redpanda's internal build system constructs a Python virtual environment
# executable using pex that contains all of the Python build dependencies. If
# this isn't found, then fall back to the system Python3 interpreter.
find_program(REDPANDA_PYTHON_EXECUTABLE "kafka-codegen-venv")

if(REDPANDA_PYTHON_EXECUTABLE)
    add_executable(RedpandaPython::Interpreter IMPORTED)
    set_property(TARGET RedpandaPython::Interpreter
        PROPERTY IMPORTED_LOCATION "${REDPANDA_PYTHON_EXECUTABLE}")
else()
    find_package(Python3 REQUIRED COMPONENTS Interpreter)
    add_executable(RedpandaPython::Interpreter ALIAS Python3::Interpreter)
    set(REDPANDA_PYTHON_EXECUTABLE ${Python3_EXECUTABLE})
endif()
message(STATUS "Using Python interpeter: ${REDPANDA_PYTHON_EXECUTABLE}")

foreach(PYTHON_DEPENDENCY jsonschema jinja2)
    execute_process(
        COMMAND ${REDPANDA_PYTHON_EXECUTABLE} -c "import ${PYTHON_DEPENDENCY}"
        RESULT_VARIABLE EXIT_CODE
        ERROR_QUIET
        OUTPUT_QUIET)
    if (NOT EXIT_CODE EQUAL 0)
        message(FATAL_ERROR "Required Python module not found: ${PYTHON_DEPENDENCY}")
    endif()
endforeach()

# The foundational base library is configured as a dependency of every other
# target. See base/README.md for more information.
add_subdirectory(base)
link_libraries(v::base)

# New style modules.
add_subdirectory(hashing)
add_subdirectory(random)
add_subdirectory(bytes)
add_subdirectory(compression)
add_subdirectory(http)
add_subdirectory(container)
add_subdirectory(strings)
add_subdirectory(crypto)
add_subdirectory(cloud_roles)
add_subdirectory(syschecks)
add_subdirectory(io)
add_subdirectory(ssx)
add_subdirectory(wasm)
add_subdirectory(transform)
add_subdirectory(net)
add_subdirectory(rpc)
add_subdirectory(debug_bundle)

# libraries
add_subdirectory(test_utils)
add_subdirectory(finjector)
add_subdirectory(iceberg)
add_subdirectory(utils)
add_subdirectory(metrics)
add_subdirectory(model)
add_subdirectory(json)
add_subdirectory(config)
add_subdirectory(storage)
add_subdirectory(raft)
add_subdirectory(cluster)
add_subdirectory(features)
add_subdirectory(kafka)
add_subdirectory(reflection)
add_subdirectory(pandaproxy)
add_subdirectory(cloud_storage_clients)
add_subdirectory(security)
add_subdirectory(serde)
add_subdirectory(cloud_storage)
add_subdirectory(compat)
add_subdirectory(rp_util)
add_subdirectory(resource_mgmt)
add_subdirectory(migrations)
add_subdirectory(datalake)
add_subdirectory(cloud_io)
add_subdirectory(cloud_topics)
add_subdirectory(schema)

option(ENABLE_GIT_VERSION "Build with Git metadata" OFF)

# If enabled, the git hash is included in the Redpanda binary, which will
# require relinking of at least the `redpanda` binary each time the hash
# changes (switching branches, creating a new commit, etc).
# Cannot be used when ENABLE_GIT_VERSION=off.
option(ENABLE_GIT_HASH "Build with Git hash in metadata" OFF)

if(${ENABLE_GIT_VERSION})
  include(GetGitRevisionDescription)
  if(${ENABLE_GIT_HASH})
    git_describe(GIT_VER --always --match=v*)
  else()
    # If displaying a version, make sure it also doesn't display a hash.
    git_describe(GIT_VER --always --abbrev=0 --match=v*)
  endif()
else()
  if(${ENABLE_GIT_HASH})
    message(FATAL_ERROR "ENABLE_GIT_HASH cannot be 'on' when ENABLE_GIT_VERSION is 'off'")
  endif()
  set(GIT_VER "no_version")
endif()

if(${ENABLE_GIT_HASH})
  include(GetGitRevisionDescription)
  get_git_head_revision(GIT_REFSPEC GIT_SHA1)
  git_local_changes(GIT_CLEAN_DIRTY)
  if("${GIT_CLEAN_DIRTY}" STREQUAL "DIRTY")
    set(GIT_CLEAN_DIRTY "-dirty")
  else()
    set(GIT_CLEAN_DIRTY "")
  endif()
else()
  set(GIT_SHA1 "000")
  set(GIT_CLEAN_DIRTY "-dev")
endif()

configure_file(version/version.cc.in version/version.cc @ONLY)
v_cc_library(
  NAME version
  HDRS version/version.h
  SRCS "${CMAKE_CURRENT_BINARY_DIR}/version/version.cc"
)

# main executables
add_subdirectory(redpanda)
