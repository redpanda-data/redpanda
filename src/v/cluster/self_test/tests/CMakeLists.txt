rp_test(
    UNIT_TEST
    BINARY_NAME self_test_test
    SOURCES self_test_request_test.cc
    DEFINITIONS BOOST_TEST_DYN_LINK
    LIBRARIES v::seastar_testing_main v::cluster
    LABELS self_test
    ARGS "-- -c 1"
)

rp_test(
    UNIT_TEST
    BINARY_NAME self_test_framework_test
    SOURCES self_test_config_test.cc
    DEFINITIONS BOOST_TEST_DYN_LINK
    LIBRARIES Boost::unit_test_framework v::cluster
    LABELS self_test
    ARGS "-- -c 1"
)
