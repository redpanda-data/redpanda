v_cc_library(
  NAME seastar_testing_main
  SRCS seastar_testing_main.cc
  DEPS Seastar::seastar_testing v::random v::bytes_random)

find_package(GTest)

v_cc_library(
  NAME gtest_main
  SRCS
    gtest_main.cc
    gtest_utils.cc
    runfiles.cc
  DEPS Seastar::seastar Seastar::seastar_testing GTest::gtest GTest::gmock v::bytes_random)

add_subdirectory(tests)
