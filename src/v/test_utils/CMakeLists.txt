v_cc_library(
  NAME seastar_testing_main
  SRCS seastar_testing_main.cc
  DEPS Seastar::seastar_testing v::rprandom)

find_package(GTest)

v_cc_library(
  NAME gtest_main
  SRCS gtest_main.cc
  DEPS Seastar::seastar Seastar::seastar_testing GTest::gtest)

add_subdirectory(tests)
