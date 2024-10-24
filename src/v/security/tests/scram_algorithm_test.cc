// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#define BOOST_TEST_MODULE kafka_security
#include "random/generators.h"
#include "security/scram_algorithm.h"
#include "utils/base64.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/algorithm/string.hpp>
#include <boost/test/unit_test.hpp>
#include <fmt/ostream.h>

namespace security {

/*
 * Test coverage includes all of the relevant test cases found in upstream
 * kafka's ScramMessagesTest. Some use cases are not included which are only
 * relevant for client implementations. Comments from kafka about any test case
 * are surrounded by <kafka>...</kafka>.
 */

static auto valid_extensions() {
    static const std::vector<ss::sstring> extensions = {
      "ext=val1",
      "anotherext=name1=value1 name2=another test value \"\'!$[]()",
      "first=val1,second=name1 = value ,third=123",
    };
    return extensions;
}

static auto invalid_extensions() {
    static const std::vector<ss::sstring> extensions = {
      "ext1=value", "ext", "ext=value1,value2", "ext=,", "ext =value"};
    return extensions;
}

static auto valid_reserved() {
    static const std::vector<ss::sstring> reserved = {
      "m=reserved-value",
      "m=name1=value1 name2=another test value \"\'!$[]()",
    };
    return reserved;
}

static auto invalid_reserved() {
    static const std::vector<ss::sstring> reserved = {
      "m", "m=name,value", "m=,"};
    return reserved;
}

static auto make_nonce() {
    return random_generators::gen_alphanum_string(30); // NOLINT
}

BOOST_AUTO_TEST_CASE(client_first_message_valid) {
    const auto nonce = make_nonce();

    // <kafka>Default format used by Kafka client: only user and nonce are
    // specified</kafka>
    {
        client_first_message m(bytes::from_string(
          ssx::sformat("n,,n=testuser,r={}", nonce).c_str()));
        BOOST_REQUIRE_EQUAL(m.username(), "testuser");
        BOOST_REQUIRE_EQUAL(m.nonce(), nonce);
        BOOST_REQUIRE_EQUAL(m.authzid(), "");
    }

    // <kafka>Username containing comma, encoded as =2C</kafka>
    {
        client_first_message m(bytes::from_string(
          ssx::sformat("n,,n=test=2Cuser,r={}", nonce).c_str()));
        BOOST_REQUIRE_EQUAL(m.username(), "test=2Cuser");
        BOOST_REQUIRE_EQUAL(m.nonce(), nonce);
        BOOST_REQUIRE_EQUAL(m.authzid(), "");
        BOOST_REQUIRE_EQUAL(m.username_normalized(), "test,user");
    }

    // <kafka>Username containing equals, encoded as =3D</kafka>
    {
        client_first_message m(bytes::from_string(
          ssx::sformat("n,,n=test=3Duser,r={}", nonce).c_str()));
        BOOST_REQUIRE_EQUAL(m.username(), "test=3Duser");
        BOOST_REQUIRE_EQUAL(m.nonce(), nonce);
        BOOST_REQUIRE_EQUAL(m.authzid(), "");
        BOOST_REQUIRE_EQUAL(m.username_normalized(), "test=user");
    }

    // <kafka>Optional authorization id specified</kafka>
    {
        client_first_message m(bytes::from_string(
          ssx::sformat("n,a=testauthzid,n=testuser,r={}", nonce).c_str()));
        BOOST_REQUIRE_EQUAL(m.username(), "testuser");
        BOOST_REQUIRE_EQUAL(m.nonce(), nonce);
        BOOST_REQUIRE_EQUAL(m.authzid(), "testauthzid");
        BOOST_REQUIRE(!m.token_authenticated());
    }

    // <kafka>Optional reserved value specified</kafka>
    for (auto reserved : valid_reserved()) {
        client_first_message m(bytes::from_string(
          ssx::sformat("n,,{},n=testuser,r={}", reserved, nonce).c_str()));
        BOOST_REQUIRE_EQUAL(m.username(), "testuser");
        BOOST_REQUIRE_EQUAL(m.nonce(), nonce);
        BOOST_REQUIRE_EQUAL(m.authzid(), "");
        BOOST_REQUIRE(!m.token_authenticated());
    }

    // <kafka>Optional extension specified</kafka>
    for (auto extension : valid_extensions()) {
        client_first_message m(bytes::from_string(
          ssx::sformat("n,,n=testuser,r={},{}", nonce, extension).c_str()));
        BOOST_REQUIRE_EQUAL(m.username(), "testuser");
        BOOST_REQUIRE_EQUAL(m.nonce(), nonce);
        BOOST_REQUIRE_EQUAL(m.authzid(), "");
        BOOST_REQUIRE(!m.token_authenticated());
    }

    // <kafka>optional tokenauth specified as extensions</kafka>
    {
        client_first_message m(bytes::from_string(
          ssx::sformat("n,,n=testuser,r={},tokenauth=true", nonce).c_str()));
        BOOST_REQUIRE_EQUAL(m.username(), "testuser");
        BOOST_REQUIRE_EQUAL(m.nonce(), nonce);
        BOOST_REQUIRE_EQUAL(m.authzid(), "");
        BOOST_REQUIRE(m.token_authenticated());
    }
}

BOOST_AUTO_TEST_CASE(client_first_message_invalid) {
    const auto nonce = make_nonce();

    // <kafka>Invalid entry in gs2-header</kafka>
    BOOST_REQUIRE_EXCEPTION(
      client_first_message(bytes::from_string(
        ssx::sformat("n,x=something,n=testuser,r={}", nonce).c_str())),
      scram_exception,
      [](const scram_exception& e) {
          return std::string(e.what()).find(
                   "Invalid SCRAM client first message")
                 != std::string::npos;
      });

    // <kafka>Invalid reserved entry</kafka>
    for (auto reserved : invalid_reserved()) {
        BOOST_REQUIRE_EXCEPTION(
          client_first_message(bytes::from_string(
            ssx::sformat("n,,{},n=testuser,r={}", reserved, nonce).c_str())),
          scram_exception,
          [](const scram_exception& e) {
              return std::string(e.what()).find(
                       "Invalid SCRAM client first message")
                     != std::string::npos;
          });
    }

    // <kafka>Invalid extension</kafka>
    for (auto extension : invalid_extensions()) {
        BOOST_REQUIRE_EXCEPTION(
          client_first_message(bytes::from_string(
            ssx::sformat("n,,n=testuser,r={},{}", nonce, extension).c_str())),
          scram_exception,
          [](const scram_exception& e) {
              return std::string(e.what()).find(
                       "Invalid SCRAM client first message")
                     != std::string::npos;
          });
    }
}

BOOST_AUTO_TEST_CASE(server_first_message_ctor) {
    const auto client_nonce = make_nonce();
    const auto server_nonce = make_nonce();
    auto salt = random_generators::get_bytes(30);
    auto iterations = 33;

    server_first_message m(client_nonce, server_nonce, salt, iterations);
    BOOST_REQUIRE_EQUAL(
      m.sasl_message(),
      ssx::sformat(
        "r={}{},s={},i={}",
        client_nonce,
        server_nonce,
        bytes_to_base64(salt),
        iterations));
}

BOOST_AUTO_TEST_CASE(client_final_message_valid) {
    auto random_base64_bytes = [] {
        auto bytes = random_generators::get_bytes(30);
        return bytes_to_base64(bytes);
    };

    auto nonce = random_generators::gen_alphanum_string(30);
    auto channel_binding = random_base64_bytes();
    auto proof = random_base64_bytes();

    // <kafka>Default format used by Kafka client: channel-binding, nonce and
    // proof are specified</kafka>
    {
        client_final_message m(bytes::from_string(
          ssx::sformat("c={},r={},p={}", channel_binding, nonce, proof)
            .c_str()));
        BOOST_REQUIRE_EQUAL(
          base64_to_bytes(channel_binding), m.channel_binding());
        BOOST_REQUIRE_EQUAL(base64_to_bytes(proof), m.proof());
        BOOST_REQUIRE_EQUAL(m.nonce(), nonce);
    }

    // <kafka>Optional extension specified</kafka>
    for (auto extension : valid_extensions()) {
        client_final_message m(bytes::from_string(
          ssx::sformat(
            "c={},r={},{},p={}", channel_binding, nonce, extension, proof)
            .c_str()));
        BOOST_REQUIRE_EQUAL(
          base64_to_bytes(channel_binding), m.channel_binding());
        BOOST_REQUIRE_EQUAL(base64_to_bytes(proof), m.proof());
        BOOST_REQUIRE_EQUAL(m.nonce(), nonce);
    }
}

BOOST_AUTO_TEST_CASE(client_final_message_invalid) {
    auto random_base64_bytes = [] {
        auto bytes = random_generators::get_bytes(30);
        return bytes_to_base64(bytes);
    };

    auto nonce = random_generators::gen_alphanum_string(30);
    auto channel_binding = random_base64_bytes();
    auto proof = random_base64_bytes();

    // <kafka>Invalid channel binding</kafka>
    BOOST_REQUIRE_EXCEPTION(
      client_final_message(bytes::from_string(
        ssx::sformat("c=ab,r={},p={}", nonce, proof).c_str())),
      scram_exception,
      [](const scram_exception& e) {
          return std::string(e.what()).find(
                   "Invalid SCRAM client final message")
                 != std::string::npos;
      });

    // <kafka>Invalid proof</kafka>
    BOOST_REQUIRE_EXCEPTION(
      client_final_message(bytes::from_string(
        ssx::sformat("c={},r={},p=123", channel_binding, nonce).c_str())),
      scram_exception,
      [](const scram_exception& e) {
          return std::string(e.what()).find(
                   "Invalid SCRAM client final message")
                 != std::string::npos;
      });

    // <kafka>Invalid extensions</kafka>
    for (auto extension : invalid_extensions()) {
        BOOST_REQUIRE_EXCEPTION(
          client_final_message(bytes::from_string(
            ssx::sformat(
              "c={},r={},{},p={}", channel_binding, nonce, extension, proof)
              .c_str())),
          scram_exception,
          [](const scram_exception& e) {
              return std::string(e.what()).find(
                       "Invalid SCRAM client final message")
                     != std::string::npos;
          });
    }
}

BOOST_AUTO_TEST_CASE(server_final_message_ctor) {
    auto signature = random_generators::get_bytes(30);

    {
        server_final_message m(std::nullopt, signature);
        BOOST_REQUIRE_EQUAL(
          m.sasl_message(), ssx::sformat("v={}", bytes_to_base64(signature)));
    }

    {
        server_final_message m("error message", signature);
        BOOST_REQUIRE_EQUAL(m.sasl_message(), "e=error message");
    }
}

BOOST_AUTO_TEST_CASE(validate_password) {
    ss::sstring password = "letmein";
    ss::sstring garbage = "letmeout";
    int iterations = 3;

    auto creds = scram_sha256::make_credentials(password, iterations);

    bool check_password = scram_sha256::validate_password(
      password, creds.stored_key(), creds.salt(), creds.iterations());
    bool check_garbage = scram_sha256::validate_password(
      garbage, creds.stored_key(), creds.salt(), creds.iterations());
    BOOST_REQUIRE_EQUAL(check_password, true);
    BOOST_REQUIRE_EQUAL(check_garbage, false);
}

} // namespace security
