include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

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
add_subdirectory(rpc)
add_subdirectory(model)
add_subdirectory(json)
add_subdirectory(coproc)
add_subdirectory(config)
add_subdirectory(storage)
add_subdirectory(raft)
add_subdirectory(cluster)
add_subdirectory(kafka)
add_subdirectory(reflection)
add_subdirectory(pandaproxy)
add_subdirectory(http)
add_subdirectory(s3)

include(GetGitRevisionDescription)
get_git_head_revision(GIT_REFSPEC GIT_SHA1)
git_local_changes(GIT_CLEAN_DIRTY)
if("${GIT_CLEAN_DIRTY}" STREQUAL "DIRTY")
  set(GIT_CLEAN_DIRTY "-dirty")
else()
  set(GIT_CLEAN_DIRTY "")
endif()
git_describe(GIT_VER --always)
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
