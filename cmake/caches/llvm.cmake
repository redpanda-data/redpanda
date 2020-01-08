# used by /tools/build.py to seed the clang bootstrapping compiler options
set(CMAKE_BUILD_TYPE Release CACHE STRING "")
set(LLVM_INCLUDE_TESTS OFF CACHE BOOL "")
set(LLVM_INCLUDE_EXAMPLES OFF CACHE BOOL "")
set(LLVM_INCLUDE_UTILS OFF CACHE BOOL "")
set(LLVM_INCLUDE_DOCS OFF CACHE BOOL "")
set(LLVM_TARGETS_TO_BUILD X86 CACHE STRING "")
set(CLANG_INCLUDE_TESTS OFF CACHE BOOL "")
set(COMPILER_RT_INCLUDE_TESTS OFF CACHE BOOL "")
set(COMPILER_RT_BUILD_SANITIZERS ON CACHE BOOL "")
set(LLVM_ENABLE_ZLIB ON CACHE BOOL "")
set(LLVM_ENABLE_BACKTRACES OFF CACHE BOOL "")
set(CLANG_PLUGIN_SUPPORT OFF CACHE BOOL "")
set(COMPILER_RT_ENABLE_IOS OFF CACHE BOOL "")
set(COMPILER_RT_ENABLE_WATCHOS OFF CACHE BOOL "")
set(COMPILER_RT_ENABLE_TVOS OFF CACHE BOOL "")
set(LLVM_ENABLE_PROJECTS compiler-rt clang lld clang-tools-extra CACHE STRING "")
set(LLVM_USE_LINKER gold CACHE STRING "")
set(LLVM_BUILD_TOOLS OFF CACHE BOOL "")
set(LLVM_CCACHE_BUILD ON CACHE BOOL "")

# Linking LLVM binaries need lots of memory.
# In order to make it possible on commodity hardware it is critical
# to limit the number of liniking threads.
# CMake returns total physical memeory in MiB 1 GB ~ 953.674 MiB
# Single linking job consumes ~3 GB of RAM (3 GB ~ 2861 MiB)
cmake_host_system_information(
    RESULT available_memory
    QUERY TOTAL_PHYSICAL_MEMORY)
math(EXPR parallel_link_jobs "${available_memory} / 2861")
set(LLVM_PARALLEL_LINK_JOBS ${parallel_link_jobs} CACHE STRING "")
cmake_host_system_information(
    RESULT build_concurrency_factor
    QUERY NUMBER_OF_LOGICAL_CORES)

if(parallel_link_jobs GREATER build_concurrency_factor)
    set(parallel_link_jobs ${build_concurrency_factor})
endif()
set(LLVM_PARALLEL_LINK_JOBS ${parallel_link_jobs})
