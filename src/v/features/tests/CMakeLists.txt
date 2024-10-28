
set(srcs
    feature_table_test.cc)


rp_test(
  UNIT_TEST
  BINARY_NAME test_features
  SOURCES feature_table_test.cc
  LIBRARIES v::seastar_testing_main v::features
  LABELS features
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME test_enterprise_features
  SOURCES enterprise_features_test.cc
  LIBRARIES v::gtest_main v::enterprise_features v::config
  LABELS enterprise_features
)
