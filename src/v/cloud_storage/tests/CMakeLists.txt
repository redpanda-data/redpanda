
rp_test(
  UNIT_TEST
  BINARY_NAME test_cloud_storage
  SOURCES 
    manifest_test.cc 
    s3_imposter.cc 
    remote_test.cc 
    cache_test.cc  
    offset_translation_layer_test.cc 
    remote_segment_test.cc 
    remote_partition_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main Boost::unit_test_framework v::cloud_storage v::storage_test_utils
  ARGS "-- -c 1"
  LABELS cloud_storage
)



