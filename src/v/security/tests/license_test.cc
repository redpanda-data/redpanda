// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "security/license.h"

#include <boost/date_time/gregorian/parsers.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/test/unit_test.hpp>
namespace security {

BOOST_AUTO_TEST_CASE(test_license_invalid_signature) {
    /// This license has been generated with a non matching signature, even
    /// though the contents section is valid
    static const auto license_contents_bad_signature
      = "eyJ2ZXJzaW9uIjogMCwgIm9yZyI6ICJyZWRwYW5kYS10ZXN0aW5nIiwgInR5cGUiOiAxLC"
        "AiZXhwaXJ5IjogIjIwMjItNy00In0=."
        "QOPTqwnMwV0hc4ZwsPBQkI9LPmwSgiHGWWxVGutk+"
        "THrXDtm2UTFxhFpGEYgdmBNCNLKKiBNfyMSsohBAMCr6U5k3211d7+X1++"
        "6ni3ykinJBhuLYE9+fNnHHxF/"
        "GnkOGzFwklOUKXR7CuSEMrmUt6cDLkoyjTAtE6ygi7T8hOpbEOf5B9IK72IgSsFw7phtEl"
        "uy9gFl+XkQZy5cx8roHd/G6PRgirN/"
        "zDeyC66vIjIU8ZNV4Ly+69asRRakvh6lnLbGIpfmWwUJKi9DX4DEGaw/"
        "WGYKJB5jzywOcoNdO/t4AT8UeCArICiPmrqCvISvJJk80OKiSEU3ChLNHJPRsQ==";
    BOOST_CHECK_THROW(
      make_license(license_contents_bad_signature),
      license_verifcation_exception);
}

BOOST_AUTO_TEST_CASE(test_license_malformed_content) {
    /// This license has been generated without the 'expiry' parameter, making
    /// it malformed
    static const auto license_contents_malformed_content
      = "eyJoYWkiOiAiMTIzIn0=."
        "KM7DNlb3Ja49xImVi6FnwewXXp2Skt72q7RV8xcBCQtlg7frEzTbQmu6eKq2scSU2zqOX5"
        "FBqJ1ZEZ6RaaSiEqVGrsvHfR8bh4qSSzWUP8ny+"
        "wcpei8zBfRUR2ulZv9rib3FPKDlNHC3Smtsyosim+"
        "i2O7A3ARPKHFFBFtKufKTihHPY87JxY8ytAXWNlCfaisaPst9XQtUmy4iJAx5QrJpC4Y03"
        "u9XOC0/cbwKzotMGP8TojU2V5/zlxMde/VYWI3ic5Jhp4x5rHTDNmV2eaUaDU8h3W55D/"
        "UJ/feVi5ba2wfBFto/0uZ1M7NvevmfmQ3UC3z6bJnmkdwqZ2TdpWQ==";
    BOOST_CHECK_THROW(
      make_license(license_contents_malformed_content),
      license_malformed_exception);
}

BOOST_AUTO_TEST_CASE(test_license_invalid_content) {
    /// This license was generated with an expiration date set to a date in the
    /// past, making it invalid
    static const auto license_contents_invalid_content
      = "eyJ2ZXJzaW9uIjogMCwgIm9yZyI6ICJyZWRwYW5kYS10ZXN0aW5nIiwgInR5cGUiOiAxLC"
        "AiZXhwaXJ5IjogIjE5OTktNy00In0=.RKLx88ZUtzAQofO3F8azuUn8k9q+"
        "tS37JvsjwZs7YHluupuAQXpQJk2qLVWlJeaMvjhaQTXNl6j7JEoKbUmJESnOjh5ghre64x"
        "YPF5jLkN+S1N0eoVp0eR7w13vo3RVwfkKWLZKM7JTXdXJiXHqvnXrtjXpCR5T+"
        "P39KJFDeOTcwY6ojcBJVcYidpvExfNx9S/"
        "N0Lw4txozdywaYT3W4xABr8k0KlXmf8Oag77qW3kAcKHmjin6R64GTrcDSx/"
        "SQY18KjPw9J9s2gZRXIHo6U0Jmsv6lNbDAkPtUAN+AQRTBN4ayQEz40yqxO279vz3U4UO/"
        "4SbXfVdZ524rEmTrMQ==";
    BOOST_CHECK_THROW(
      make_license(license_contents_invalid_content),
      license_invalid_exception);
}

BOOST_AUTO_TEST_CASE(test_license_valid_content) {
    const char* sample_valid_license = std::getenv("REDPANDA_SAMPLE_LICENSE");
    if (sample_valid_license == nullptr) {
        const char* is_on_ci = std::getenv("CI");
        BOOST_TEST_REQUIRE(
          !is_on_ci,
          "Expecting the REDPANDA_SAMPLE_LICENSE env var in the CI "
          "enviornment");
        return;
    }
    const ss::sstring license_str{sample_valid_license};
    const auto license = make_license(license_str);
    BOOST_CHECK_EQUAL(license.format_version, 0);
    BOOST_CHECK_EQUAL(license.type, license_type::enterprise);
    BOOST_CHECK_EQUAL(license.organization, "redpanda-testing");
    BOOST_CHECK(
      license.expiry == boost::gregorian::from_simple_string("2122-06-06"));
    BOOST_CHECK(!license.is_expired());
}
} // namespace security
