# used by /tools/build.py to seed the clang bootstrapping compiler options
set(CMAKE_BUILD_TYPE Release CACHE STRING "")
set(LLVM_INCLUDE_TESTS OFF CACHE BOOL "")
set(LLVM_INCLUDE_EXAMPLES OFF CACHE BOOL "")
set(LLVM_INCLUDE_UTILS OFF CACHE BOOL "")
set(LLVM_INCLUDE_DOCS OFF CACHE BOOL "")
set(LLVM_TARGETS_TO_BUILD X86 CACHE STRING "")
set(CLANG_INCLUDE_TESTS OFF CACHE BOOL "")
set(COMPILER_RT_INCLUDE_TESTS OFF CACHE BOOL "")
set(COMPILER_RT_BUILD_SANITIZERS OFF CACHE BOOL "")
set(LLVM_ENABLE_ZLIB OFF CACHE BOOL "")
set(LLVM_ENABLE_BACKTRACES OFF CACHE BOOL "")
set(CLANG_PLUGIN_SUPPORT OFF CACHE BOOL "")
set(COMPILER_RT_ENABLE_IOS OFF CACHE BOOL "")
set(COMPILER_RT_ENABLE_WATCHOS OFF CACHE BOOL "")
set(COMPILER_RT_ENABLE_TVOS OFF CACHE BOOL "")
set(LLVM_ENABLE_PROJECTS compiler-rt clang lld CACHE STRING "")
set(LLVM_ENABLE_LTO OFF CACHE BOOL "")
set(LLVM_USE_LINKER gold CACHE STRING "")
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

set(CLANG_ENABLE_BOOTSTRAP ON CACHE BOOL "")
# Variables passed to stage2. 
# Compiler built in this stage will be used to build the software 
set(BOOTSTRAP_LLVM_ENABLE_LLD ON CACHE BOOL "")
set(BOOTSTRAP_CMAKE_BUILD_TYPE Release CACHE STRING "" )
set(BOOTSTRAP_LLVM_ENABLE_LTO ON CACHE BOOL "")
set(BOOTSTRAP_COMPILER_RT_BUILD_SANITIZERS ON CACHE BOOL "")
# For ARM support we need to add append ARM target here
set(BOOTSTRAP_LLVM_TARGETS_TO_BUILD X86 CACHE STRING "")
set(CLANG_BOOTSTRAP_PASSTHROUGH 
    CMAKE_INSTALL_PREFIX
    LLVM_PARALLEL_LINK_JOBS
    CACHE STRING "")

