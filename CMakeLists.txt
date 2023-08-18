cmake_minimum_required(VERSION 3.12.0)
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

# Redirect control for internal Redpanda builds
if(VECTORIZED_CMAKE_DIR)
  include(${VECTORIZED_CMAKE_DIR}/main.cmake)
  return()
endif()

include(main)
