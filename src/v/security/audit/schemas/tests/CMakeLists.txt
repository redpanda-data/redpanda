rp_test(
    UNIT_TEST
    BINARY_NAME security_audit_schemas_unit
    SOURCES
      ocsf_schemas_test.cc
    DEFINITIONS BOOST_TEST_DYN_LINK
    LIBRARIES Boost::unit_test_framework v::json v::version v::security_audit
    LABELS audit
)
