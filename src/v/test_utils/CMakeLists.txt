v_cc_library(
  NAME seastar_testing_main
  SRCS seastar_testing_main.cc
  DEPS Seastar::seastar_testing v::random v::bytes_random)

find_package(GTest)

v_cc_library(
  NAME gtest_utils
  SRCS
    gtest_utils.cc
  DEPS Seastar::seastar Seastar::seastar_testing GTest::gtest GTest::gmock v::bytes_random
)

v_cc_library(
  NAME gtest_main
  SRCS
    gtest_main.cc
    runfiles.cc
  DEPS v_gtest_utils)


add_subdirectory(go/kreq-gen)
add_subdirectory(tests)
