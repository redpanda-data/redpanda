
set(srcs
    feature_table_test.cc)


rp_test(
  UNIT_TEST
  BINARY_NAME test_features
  SOURCES feature_table_test.cc
  LIBRARIES v::seastar_testing_main v::features
  LABELS features
)