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

#include <chrono>

using namespace std::chrono_literals;

namespace security {

BOOST_AUTO_TEST_CASE(test_license_invalid_signature) {
    /// This license has been generated with a non matching signature, even
    /// though the contents section is valid
    static const auto license_contents_bad_signature
      = "eyJ2ZXJzaW9uIjogMCwgIm9yZyI6ICJyZWRwYW5kYS1jbG91ZCIsICJ0eXBlIjogMSwgIm"
        "V4cGlyeSI6IDE2NjA0OTg1MzZ9.dfadf/dfadfa+kkk/I/kk/"
        "934349asfkdw==";
    BOOST_CHECK_THROW(
      make_license(license_contents_bad_signature),
      license_verifcation_exception);
}

BOOST_AUTO_TEST_CASE(test_license_malformed_content) {
    /// This license has been generated without the 'expiry' parameter, making
    /// it malformed
    static const auto license_contents_malformed_content
      = "eyJ2ZXJzaW9uIjogMCwgIm9yZyI6ICJyZWRwYW5kYS1jbG91ZCIsICJ0eXBlIjogMX0=."
        "F2EHtQftac9+O3ucUijiJ6ta5nmoonEoZhr86FCA+"
        "4hAIQnetBcO1h7yD4OUHy7t9dS0hTz2BJU981G8i0Ud+v0+"
        "GRBII3VSZ1gL3W98QRGE1KiOjR11G3+8YQhSrFRJMHhXyYPEHiFKuYDCoIaozd2IhGYi/"
        "Gcnq/yWZRRDTcnhPOBQivkX5lQpTPorM+xO5ER4wrAROm2jp2lB/"
        "GDrco1f5iU9M3anIZo1F3rq4V0rnH/FJvwQW/"
        "7gwO+Ut06O3lWJoUZOTuwueyxopACRmWOm/"
        "DOYPZDkc8Xoui69EKVqRc4UOujbfOBYzhGq7wNlchJ0QOKUz9Bi/ZPoedOFAw==";
    BOOST_CHECK_THROW(
      make_license(license_contents_malformed_content),
      license_malformed_exception);
}

BOOST_AUTO_TEST_CASE(test_license_invalid_content) {
    /// This license was generated with an expiration date set to a date in the
    /// past, making it invalid
    static const auto license_contents_invalid_content
      = "eyJ2ZXJzaW9uIjogMCwgIm9yZyI6ICJyZWRwYW5kYS1jbG91ZCIsICJ0eXBlIjogMSwgIm"
        "V4cGlyeSI6IDE2NTg3NzA0Nzh9.m2fIYroOtmIEaJILcGUyDPPDLbJStO+"
        "20GnMbA9Gg9QHVMLihj4dgL7k4x+kKqRMSCVe5tkhkNS/"
        "2pzcIranGbWs7AlShBFsXKKA8rTYW1xhvVYw71gkPHkyCOXb3++tcmU5W0MSQM9r4/"
        "XTNHz7DfM4bvWsw9IM4tWeIC3U+SoiJ+ARXag7wxETo7JGgS4+AB7WIj3u9whVA1+"
        "6p9w0/"
        "LgRUeIoVru6frBAVHUCGl6x2npqoLTzMRT2d3YnFnI8ilBeQllq7bTAcNkQwXwKigfcBe2"
        "WSj/n77O/GNTlIhBVBtbBM2EcbZQMAhrSnTJJg5kcQMMg9oVjzg278cO+hw==";
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
    BOOST_CHECK(!license.is_expired());
    BOOST_CHECK_EQUAL(license.expiry.count(), 4813252273);
    BOOST_CHECK(
      license.expiration() == license::clock::time_point{4813252273s});
    BOOST_CHECK_EQUAL(
      license.checksum,
      "2730125070a934ca1067ed073d7159acc9975dc61015892308aae186f7455daf");
}
} // namespace security
