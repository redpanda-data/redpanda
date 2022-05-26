rp_test(
        UNIT_TEST
        BINARY_NAME test_cli_parser
        SOURCES cli_parser_tests.cc
        DEFINITIONS BOOST_TEST_DYN_LINK
        LIBRARIES Boost::unit_test_framework v::application
        LABELS cli_parse
)