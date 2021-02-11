# https://github.com/ceph/ceph/blob/master/CMakeLists.txt
# Use CCACHE_DIR environment variable
option(WITH_CCACHE "Build with ccache." ON)
if(WITH_CCACHE)
  find_program (CCACHE_FOUND ccache)
  if(CCACHE_FOUND)
    message(STATUS "Building with ccache: ${CCACHE_FOUND}, CCACHE_DIR=$ENV{CCACHE_DIR}")
    set(CMAKE_C_COMPILER_LAUNCHER ccache)
    set(CMAKE_CXX_COMPILER_LAUNCHER ccache)
  else(CCACHE_FOUND)
    message(WARNING "Can't find ccache. Is it installed?")
  endif(CCACHE_FOUND)
endif(WITH_CCACHE)
