# https://github.com/ceph/ceph/blob/master/CMakeLists.txt
# Use CCACHE_DIR environment variable
option(WITH_CCACHE "Build with ccache." ON)
if(WITH_CCACHE)
  find_program (CCACHE_EXECUTABLE ccache)
  if(CCACHE_EXECUTABLE)
    message(STATUS "Building with ccache: ${CCACHE_EXECUTABLE}, CCACHE_DIR=$ENV{CCACHE_DIR}")
    list(PREPEND CMAKE_C_COMPILER_LAUNCHER "${CCACHE_EXECUTABLE}")
    list(PREPEND CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_EXECUTABLE}")
  else(CCACHE_EXECUTABLE)
    message(WARNING "Can't find ccache. Is it installed?")
  endif(CCACHE_EXECUTABLE)
endif(WITH_CCACHE)
