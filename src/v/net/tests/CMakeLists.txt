
rp_test(
        UNIT_TEST
        BINARY_NAME net_connection_rate
        SOURCES
        connection_rate_test.cc
        DEFINITIONS BOOST_TEST_DYN_LINK
        LIBRARIES v::seastar_testing_main Boost::unit_test_framework v::net
        ARGS "-- -c 1"
        LABELS net
)

rp_test(
        UNIT_TEST
        BINARY_NAME test_conn_quota
        SOURCES conn_quota_test.cc
        LIBRARIES v::seastar_testing_main v::net v::config absl::hash absl::node_hash_map
        ARGS "-- -c 8"
        LABELS net
)

rp_test(
        UNIT_TEST
        BINARY_NAME dns
        SOURCES
        dns.cc
        DEFINITIONS BOOST_TEST_DYN_LINK
        LIBRARIES v::seastar_testing_main Boost::unit_test_framework v::net
        ARGS "-- -c 1"
        LABELS net
)

rp_test(
        UNIT_TEST
        BINARY_NAME net_connection
        SOURCES
        connection.cc
        DEFINITIONS BOOST_TEST_DYN_LINK
        LIBRARIES v::seastar_testing_main Boost::unit_test_framework v::net
        LABELS net
)
