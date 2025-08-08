// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "security/license.h"
#include "security/tests/license_utils.h"

#include <gtest/gtest.h>

#include <chrono>

using namespace std::chrono_literals;

namespace security {

TEST(test_license, invalid_signature) {
    /// This license has been generated with a non matching signature, even
    /// though the contents section is valid
    static const auto license_contents_bad_signature
      = "eyJ2ZXJzaW9uIjogMCwgIm9yZyI6ICJyZWRwYW5kYS1jbG91ZCIsICJ0eXBlIjogMSwgIm"
        "V4cGlyeSI6IDE2NjA0OTg1MzZ9.dfadf/dfadfa+kkk/I/kk/"
        "934349asfkdw==";
    EXPECT_THROW(
      make_license(license_contents_bad_signature),
      license_verifcation_exception);
}

TEST(test_license, malformed_content) {
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
    EXPECT_THROW(
      make_license(license_contents_malformed_content),
      license_malformed_exception);
}

TEST(test_license, invalid_content) {
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
    EXPECT_THROW(
      make_license(license_contents_invalid_content),
      license_invalid_exception);
}

TEST(test_license, valid_content) {
    auto opt_license = testing::get_test_license("REDPANDA_SAMPLE_LICENSE");
    if (!opt_license.has_value()) {
        GTEST_SKIP() << testing::skip_no_license_msg;
        return;
    }
    const license license = std::move(opt_license.value());
    EXPECT_EQ(license.format_version, 0);
    EXPECT_EQ(license.get_type(), "enterprise");
    EXPECT_EQ(license.organization, "redpanda-testing");
    EXPECT_FALSE(license.is_expired());
    EXPECT_EQ(license.expiry.count(), 4813252273);
    EXPECT_EQ(license.expiration(), license::clock::time_point{4813252273s});
    EXPECT_EQ(license.products, std::vector<ss::sstring>{});
    EXPECT_EQ(
      license.checksum,
      "2730125070a934ca1067ed073d7159acc9975dc61015892308aae186f7455daf");
}

TEST(test_license, valid_content_v1) {
    auto opt_license = testing::get_test_license("REDPANDA_SAMPLE_LICENSE_V1");
    if (!opt_license.has_value()) {
        GTEST_SKIP() << testing::skip_no_license_msg;
        return;
    }
    const license license = std::move(opt_license.value());
    EXPECT_EQ(license.format_version, 1);
    EXPECT_EQ(license.get_type(), "testing_license");
    EXPECT_EQ(license.organization, "redpanda-testing");
    EXPECT_FALSE(license.is_expired());
    EXPECT_EQ(license.expiry.count(), 4344165449);
    EXPECT_EQ(license.expiration(), license::clock::time_point{4344165449s});
    EXPECT_EQ(license.products, std::vector<ss::sstring>{});
    EXPECT_EQ(
      license.checksum,
      "baba05c0557197d210966555bda6abf3fb54435959dbb5c8e7fd7c5805b29069");
}

TEST(test_license, valid_content_v1_products) {
    auto opt_license = testing::get_test_license(
      "REDPANDA_SAMPLE_LICENSE_V1_PRODUCTS");
    if (!opt_license.has_value()) {
        GTEST_SKIP() << testing::skip_no_license_msg;
        return;
    }
    const license license = std::move(opt_license.value());
    EXPECT_EQ(license.format_version, 1);
    EXPECT_EQ(license.get_type(), "testing_license");
    EXPECT_EQ(license.organization, "redpanda-testing");
    EXPECT_FALSE(license.is_expired());
    EXPECT_EQ(license.expiry.count(), 4344165449);
    EXPECT_EQ(license.expiration(), license::clock::time_point{4344165449s});
    EXPECT_EQ(
      license.products,
      (std::vector<ss::sstring>{"some_prod", "some_other_prod"}));
    EXPECT_EQ(
      license.checksum,
      "0937a2d8e4437a63373c1c1cb0f5f62c5cae9366fea1b00467b4c4eaab8ca4cf");
}
} // namespace security
