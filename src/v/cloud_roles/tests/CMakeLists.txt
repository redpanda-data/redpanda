rp_test(
    UNIT_TEST
    BINARY_NAME
        signature_test
    SOURCES
        signature_test.cc
    DEFINITIONS BOOST_TEST_DYN_LINK
    LIBRARIES
        v::seastar_testing_main
        Boost::unit_test_framework
        v::http
        v::cloud_roles
    ARGS "-- -c 1"
    LABELS cloud_roles
)
