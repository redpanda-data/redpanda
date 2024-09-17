/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "aws_sts_refresh_impl.h"

#include "bytes/streambuf.h"
#include "cloud_roles/logger.h"
#include "request_response_helpers.h"
#include "utils/file_io.h"

#include <boost/algorithm/string/trim.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>

namespace cloud_roles {
/// The path to credentials in the XML response returned by STS
static constexpr std::string_view response_node_credential_path
  = "AssumeRoleWithWebIdentityResponse.AssumeRoleWithWebIdentityResult."
    "Credentials";

/// These variables injected by the AWS operator must be present on the redpanda
/// pod to ensure we can get credentials from STS.
/// ref:
/// https://docs.amazonaws.cn/en_us/eks/latest/userguide/specify-service-account-role.html
struct aws_injected_env_vars {
    static constexpr std::string_view role_arn = "AWS_ROLE_ARN";

    static constexpr std::string_view token_file_path
      = "AWS_WEB_IDENTITY_TOKEN_FILE";
};

struct sts_params {
    static constexpr std::string_view role_session_name = "redpanda_si";

    static constexpr std::string_view api_version = "2011-06-15";

    static constexpr std::chrono::seconds token_expiry_seconds{3599};
};

struct sts_response_schema {
    static constexpr std::string_view expiry = "Expiration";
    static constexpr std::string_view access_key_id = "AccessKeyId";
    static constexpr std::string_view secret_access_key = "SecretAccessKey";
    static constexpr std::string_view session_token = "SessionToken";
};

static const boost::beast::string_view content_type{
  "application/x-www-form-urlencoded"};

static constexpr std::string_view request_payload
  = "Action=AssumeRoleWithWebIdentity"
    "&DurationSeconds={duration_sec}"
    "&Version={version}"
    "&RoleSessionName={role_session_name}"
    "&RoleArn={role_arn}"
    "&WebIdentityToken={token}";

static ss::sstring load_from_env(std::string_view env_var) {
    auto env_value = std::getenv(env_var.data());
    if (!env_value) {
        throw std::runtime_error(fmt::format(
          "environment variable {} is not set, the STS client cannot function "
          "without this.",
          env_var));
    }
    return env_value;
}

static boost::property_tree::ptree iobuf_to_ptree(iobuf&& buf) {
    namespace pt = boost::property_tree;
    try {
        iobuf_istreambuf strbuf(buf);
        std::istream stream(&strbuf);
        pt::ptree res;
        pt::read_xml(stream, res);
        return res;
    } catch (...) {
        vlog(clrl_log.error, "!!parsing error {}", std::current_exception());
        throw;
    }
}

aws_sts_refresh_impl::aws_sts_refresh_impl(
  net::unresolved_address address,
  aws_region_name region,
  ss::abort_source& as,
  retry_params retry_params)
  : refresh_credentials::impl(
      std::move(address), std::move(region), as, retry_params)
  , _role{load_from_env(aws_injected_env_vars::role_arn)}
  , _token_file_path{load_from_env(aws_injected_env_vars::token_file_path)} {}

ss::future<api_response> aws_sts_refresh_impl::fetch_credentials() {
    // The content of the token file is periodically rotated by k8s, so we
    // read it fully each time to avoid stale tokens

    ss::sstring token{};
    try {
        token = co_await read_fully_to_string({_token_file_path});
    } catch (...) {
        vlog(
          clrl_log.error,
          "failed to read IRSA pod token from file {}",
          _token_file_path);
        throw;
    }

    boost::beast::http::request<boost::beast::http::string_body> assume_req;
    assume_req.method(boost::beast::http::verb::post);
    assume_req.target("/");

    assume_req.set(boost::beast::http::field::content_type, content_type);
    assume_req.set(
      boost::beast::http::field::host,
      fmt::format("{}:{}", address().host(), address().port()));
    assume_req.set(
      boost::beast::http::field::user_agent, "redpanda.vectorized.io");

    using namespace fmt::literals;

    boost::trim(token);
    ss::sstring body = fmt::format(
      request_payload,
      "duration_sec"_a = sts_params::token_expiry_seconds.count(),
      "version"_a = sts_params::api_version,
      "role_session_name"_a = sts_params::role_session_name,
      "role_arn"_a = _role,
      "token"_a = token);

    // STS requires a TLS enabled client by default, but in test mode where we
    // use the http imposter, we need to use a simple client.
    auto tls_enabled = refresh_credentials::client_tls_enabled::yes;
    if (address().port() != default_port) {
        tls_enabled = refresh_credentials::client_tls_enabled::no;
    }

    co_return co_await request_with_payload(
      co_await make_api_client("aws_sts", tls_enabled),
      std::move(assume_req),
      std::move(body));
}

api_response_parse_result aws_sts_refresh_impl::parse_response(iobuf resp) {
    auto root = iobuf_to_ptree(std::move(resp));

    auto creds_node_maybe = root.get_child_optional(
      response_node_credential_path.data());

    if (!creds_node_maybe) {
        return malformed_api_response_error{
          .missing_fields = {response_node_credential_path.data()}};
    }

    auto creds_node = *creds_node_maybe;

    std::vector<ss::sstring> missing_fields;
    for (const auto& key :
         {sts_response_schema::expiry,
          sts_response_schema::access_key_id,
          sts_response_schema::secret_access_key,
          sts_response_schema::session_token}) {
        if (!creds_node.count(key.data())) {
            missing_fields.emplace_back(key);
        }
    }

    if (!missing_fields.empty()) {
        return malformed_api_response_error{.missing_fields = missing_fields};
    }

    auto expiration = creds_node.get<std::string>(
      sts_response_schema::expiry.data());
    auto expiration_time = parse_timestamp(expiration);

    next_sleep_duration(calculate_sleep_duration(expiration_time));

    return aws_credentials{
      .access_key_id = public_key_str{creds_node.get<std::string>(
        sts_response_schema::access_key_id.data())},
      .secret_access_key = private_key_str{creds_node.get<std::string>(
        sts_response_schema::secret_access_key.data())},
      .session_token = s3_session_token{creds_node.get<std::string>(
        sts_response_schema::session_token.data())},
      .region = region(),
    };
}

std::ostream& aws_sts_refresh_impl::print(std::ostream& os) const {
    fmt::print(os, "aws_sts_refresh_impl{{address:{}}}", address());
    return os;
}

} // namespace cloud_roles
