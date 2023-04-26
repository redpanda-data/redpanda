cmake_minimum_required(VERSION 3.12.0)

project(Redpanda)

# set(CMAKE_FIND_DEBUG_MODE TRUE)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
# keep this minimal. modify `cmake/main.cmake` file instead
if(NOT VECTORIZED_CMAKE_DIR)
  include(main)
else()
  include(${VECTORIZED_CMAKE_DIR}/main.cmake)
endif()
