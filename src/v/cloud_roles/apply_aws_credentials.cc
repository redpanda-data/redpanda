/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_roles/apply_aws_credentials.h"

#include "cloud_roles/logger.h"
#include "vlog.h"

struct aws_header_names {
    static constexpr boost::beast::string_view x_amz_security_token
      = "x-amz-security-token";
    static constexpr boost::beast::string_view x_amz_content_sha256
      = "x-amz-content-sha256";
};

/// Prefix used for testing, so that if testing against minio, tokens are
/// fetched but not injected to header. Minio does not accept random security
/// tokens with requests.
static constexpr std::string_view ci_test_token_prefix
  = "__REDPANDA_SKIP_IAM_TOKEN";

static bool is_test_token(std::string_view token) {
    return token.find(ci_test_token_prefix) == 0;
}

struct aws_signatures {
    /// SHA256 of an empty string (used in situation when the signed payload is
    /// has zero length)
    static constexpr std::string_view emptysig
      = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    static constexpr std::string_view unsigned_payload = "UNSIGNED-PAYLOAD";
};

cloud_roles::apply_aws_credentials::apply_aws_credentials(
  cloud_roles::aws_credentials credentials)
  : _signature{credentials.region, credentials.access_key_id, credentials.secret_access_key}
  , _session_token{credentials.session_token} {}

std::error_code cloud_roles::apply_aws_credentials::add_auth(
  http::client::request_header& header) const {
    if (_session_token) {
        auto st = *_session_token;
        if (!is_test_token(st())) {
            std::string_view token{st()};
            header.insert(
              aws_header_names::x_amz_security_token,
              {token.data(), token.size()});
        } else {
            vlog(clrl_log.trace, "not inserting test token to header: {}", st);
        }
    }

    auto sha256 = sha_for_verb(header.method());
    header.insert(
      aws_header_names::x_amz_content_sha256, {sha256.data(), sha256.size()});
    return _signature.sign_header(header, sha256);
}

std::string_view cloud_roles::apply_aws_credentials::sha_for_verb(
  boost::beast::http::verb verb) {
    switch (verb) {
    case boost::beast::http::verb::get:
        return aws_signatures::emptysig;
    case boost::beast::http::verb::head:
        return aws_signatures::emptysig;
    case boost::beast::http::verb::delete_:
        return aws_signatures::emptysig;
    case boost::beast::http::verb::post:
        return aws_signatures::unsigned_payload;
    case boost::beast::http::verb::put:
        return aws_signatures::unsigned_payload;
    default:
        throw std::runtime_error(
          fmt_with_ctx(fmt::format, "unsupported method: {}", verb));
    }
}

void cloud_roles::apply_aws_credentials::reset_creds(
  cloud_roles::credentials creds) {
    if (!std::holds_alternative<aws_credentials>(creds)) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "credential applier reset with incorrect credential type {}",
          creds));
    }
    auto aws_creds = std::get<aws_credentials>(creds);
    _signature = s3::signature_v4(
      aws_creds.region, aws_creds.access_key_id, aws_creds.secret_access_key);
    _session_token = aws_creds.session_token;
}

std::ostream&
cloud_roles::apply_aws_credentials::print(std::ostream& os) const {
    fmt::print(os, "apply_aws_credentials");
    return os;
}
