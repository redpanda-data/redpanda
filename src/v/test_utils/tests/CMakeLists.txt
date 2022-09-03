rp_test(
    UNIT_TEST
    BINARY_NAME test_http_imposter
    SOURCES
    http_imposter_test.cc
    DEFINITIONS BOOST_TEST_DYN_LINK
    LIBRARIES v::seastar_testing_main Boost::unit_test_framework v::http
    LABELS http_imposter
    ARGS "-- -c 1"
)
