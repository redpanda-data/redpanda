include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

add_compile_options(-Wall)

# libraries
add_subdirectory(test_utils)
add_subdirectory(ssx)
add_subdirectory(finjector)
add_subdirectory(hashing)
add_subdirectory(random)
add_subdirectory(bytes)
add_subdirectory(compression)
add_subdirectory(utils)
add_subdirectory(syschecks)
add_subdirectory(net)
add_subdirectory(rpc)
add_subdirectory(model)
add_subdirectory(json)
add_subdirectory(coproc)
add_subdirectory(config)
add_subdirectory(storage)
add_subdirectory(raft)
add_subdirectory(cluster)
add_subdirectory(features)
add_subdirectory(kafka)
add_subdirectory(reflection)
add_subdirectory(pandaproxy)
add_subdirectory(http)
add_subdirectory(cloud_storage_clients)
add_subdirectory(archival)
add_subdirectory(security)
add_subdirectory(serde)
add_subdirectory(cloud_storage)
add_subdirectory(cloud_roles)
add_subdirectory(compat)
add_subdirectory(rp_util)
add_subdirectory(resource_mgmt)
add_subdirectory(migrations)

option(ENABLE_GIT_VERSION "Build with Git metadata" OFF)

# If enabled, the git hash is included in the Redpanda binary, which will
# require relinking of at least the `redpanda` binary each time the hash
# changes (switching branches, creating a new commit, etc).
# Cannot be used when ENABLE_GIT_VERSION=off.
option(ENABLE_GIT_HASH "Build with Git hash in metadata" OFF)

if(${ENABLE_GIT_VERSION})
  include(GetGitRevisionDescription)
  if(${ENABLE_GIT_HASH})
    git_describe(GIT_VER --always)
  else()
    # If displaying a version, make sure it also doesn't display a hash.
    git_describe(GIT_VER --always --abbrev=0)
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

configure_file(version.h.in version.h @ONLY)
# main executables
add_subdirectory(redpanda)


rp_test(
  UNIT_TEST
  BINARY_NAME outcome_tests
  SOURCES outcome_tests.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework
  )
