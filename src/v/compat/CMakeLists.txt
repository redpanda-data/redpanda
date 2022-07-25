cmake_minimum_required(VERSION 3.12.0)

v_cc_library(
  NAME compat
  SRCS
    index_state_compat.cc
    remote_segment_index_compat.cc
    run.cc
  DEPS
    Seastar::seastar
    v::serde
    v::utils
    v::storage
    v::cloud_storage
)

add_custom_target(compat-check
  COMMAND /usr/bin/python3.8 ./check.py
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
