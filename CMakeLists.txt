# Redirect control for internal Redpanda builds
if(REDPANDA_CMAKE_DIR)
  cmake_minimum_required(VERSION 3.22)
  list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
  include(${REDPANDA_CMAKE_DIR}/main.cmake)
  return()
endif()

cmake_minimum_required(VERSION 3.24)
project(redpanda LANGUAGES CXX)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
include(main)
