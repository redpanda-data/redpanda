// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "security/oidc_url_parser.h"

#include <boost/test/data/test_case.hpp>
#include <boost/test/unit_test.hpp>
#include <fmt/ostream.h>

#include <ostream>

namespace bdata = boost::unit_test::data;

using namespace std::chrono_literals;

struct url_test_data {
    std::string_view test;
    result<security::oidc::parsed_url> url;
    friend std::ostream& operator<<(std::ostream& os, url_test_data d) {
        fmt::print(os, "test: {}", d.test);
        return os;
    }
};
const auto url_data = std::to_array<url_test_data>(
  {// Valid URLs
   {"https://example.com/",
    security::oidc::parsed_url{"https", "example.com", 443, "/"}},
   {"http://localhost:8080/path",
    security::oidc::parsed_url{"http", "localhost", 8080, "/path"}},
   {"ftp://192.168.1.1/file",
    security::oidc::parsed_url{"ftp", "192.168.1.1", 21, "/file"}},
   {"https://[2001:0db8:85a3:0000:0000:8a2e:0370:7334]/resource",
    security::oidc::parsed_url{
      "https", "[2001:db8:85a3::8a2e:370:7334]", 443, "/resource"}},
   {"http://example.com:80",
    security::oidc::parsed_url{"http", "example.com", 80, "/"}},
   // Valid URLs with missing port (should use default)
   {"https://example.com/path",
    security::oidc::parsed_url{"https", "example.com", 443, "/path"}},
   {"http://localhost/",
    security::oidc::parsed_url{"http", "localhost", 80, "/"}},
   // Invalid URLs
   {"invalid_url", std::make_error_code(std::errc::invalid_argument)},
   {"ftp://[::1]:missing_port",
    std::make_error_code(std::errc::invalid_argument)}});
BOOST_DATA_TEST_CASE(test_parse_url, bdata::make(url_data), d) {
    auto res = security::oidc::parse_url(d.test);
    if (d.url.has_value()) {
        BOOST_REQUIRE(res.has_value());
        BOOST_REQUIRE_EQUAL(res.assume_value(), d.url.assume_value());
    } else {
        BOOST_REQUIRE(res.has_error());
        BOOST_REQUIRE_EQUAL(res.assume_error(), d.url.assume_error());
    }
}
