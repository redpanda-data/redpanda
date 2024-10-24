/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "azure_aks_refresh_impl.h"

#include "http/utils.h"
#include "json/schema.h"
#include "request_response_helpers.h"
#include "thirdparty/ada/ada.h"
#include "utils/file_io.h"

#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/exception.hh>

#include <boost/algorithm/string/trim.hpp>
#include <rapidjson/error/en.h>

namespace {

// utility to wrap std::getenv for required environment variables
ss::sstring load_from_env(const char* env_var) {
    auto env_value = std::getenv(env_var);
    if (!env_value) {
        throw std::runtime_error(fmt::format(
          "environment variable {} is not set, the Azure AKS client cannot "
          "function "
          "without this.",
          env_var));
    }
    return env_value;
}

constexpr static auto env_var_azure_client_id = "AZURE_CLIENT_ID";
constexpr static auto env_var_azure_tenant_id = "AZURE_TENANT_ID";
constexpr static auto env_var_azure_federated_token_file
  = "AZURE_FEDERATED_TOKEN_FILE";
constexpr static auto env_var_azure_authority_host = "AZURE_AUTHORITY_HOST";

} // namespace

namespace cloud_roles {

azure_aks_refresh_impl::azure_aks_refresh_impl(
  net::unresolved_address address,
  aws_region_name region,
  ss::abort_source& as,
  retry_params retry_params)
  : refresh_credentials::impl(
      [&] {
          if (!address.host().empty()) {
              // non-empty host: it's an override that we should use
              return std::move(address);
          }

          // try to interpret AZURE_AUTHORITY_HOST as a URL, if it fails,
          // assume it's an hostname
          auto authority_host = load_from_env(env_var_azure_authority_host);
          if (auto url = ada::parse<ada::url>(authority_host);
              url.has_value()) {
              auto is_https = url->get_protocol() == "https:";
              // use port if it's set, otherwise fallback on the default 443 for
              // https and 80 for http
              auto port = url->port.value_or(is_https ? default_port : 80);
              auto hostname = url->get_hostname();
              return net::unresolved_address{hostname, port};
          }

          return net::unresolved_address{authority_host, default_port};
      }(),
      std::move(region),
      as,
      retry_params)
  , client_id_{load_from_env(env_var_azure_client_id)}
  , tenant_id_{load_from_env(env_var_azure_tenant_id)}
  , federated_token_file_{load_from_env(env_var_azure_federated_token_file)} {}

std::ostream& azure_aks_refresh_impl::print(std::ostream& os) const {
    fmt::print(os, "azure_aks_refresh_impl{{address:{}}}", address());
    return os;
}

ss::future<api_response> azure_aks_refresh_impl::fetch_credentials() {
    // https://learn.microsoft.com/en-us/entra/identity-platform/v2-oauth2-client-creds-grant-flow#third-case-access-token-request-with-a-federated-credential
    // load the encoded jwt from the token file (its content is rotated by k8s),
    //
    // compose:
    // POST /{tenant_id_}/oauth2/v2.0/token HTTP/1.1
    // Host: {authority_host_}:443
    // Content-Type: application/x-www-form-urlencoded
    //
    // scope=https%3A%2F%2Fgraph.microsoft.com%2F.default
    // &client_id={client_id_}
    // &client_assertion_type=urn%3Aietf%3Aparams%3Aoauth%3Aclient-assertion-type%3Ajwt-bearer
    // &client_assertion={content(federated_token_file_)}
    // &grant_type=client_credentials

    auto jwt_token_fut = co_await ss::coroutine::as_future(
      read_fully_to_string({federated_token_file_}));
    if (jwt_token_fut.failed()) {
        vlog(
          clrl_log.error,
          "failed to read OIDC pod token from file {}",
          federated_token_file_);
        co_return ss::coroutine::exception(jwt_token_fut.get_exception());
    }

    auto jwt_token = std::string{jwt_token_fut.get()};
    boost::trim(jwt_token);

    // setup header for www-form-urlencoded body
    auto access_token_req
      = boost::beast::http::request<boost::beast::http::string_body>{};
    access_token_req.method(boost::beast::http::verb::post);
    access_token_req.target(fmt::format("/{}/oauth2/v2.0/token", tenant_id_));
    access_token_req.set(
      boost::beast::http::field::host,
      fmt::format("{}:{}", address().host(), address().port())),
      access_token_req.set(
        boost::beast::http::field::user_agent, "redpanda.vectorized.io");
    access_token_req.set(
      boost::beast::http::field::content_type,
      "application/x-www-form-urlencoded");

    // compose the body of the request with the informations retrieved from the
    // env variables
    auto body = ssx::sformat(
      "scope={}"
      "&client_id={}"
      "&client_assertion_type=urn%3Aietf%3Aparams%3Aoauth%3Aclient-assertion-"
      "type%3Ajwt-bearer"
      "&client_assertion={}"
      "&grant_type=client_credentials",
      http::uri_encode(
        "https://storage.azure.com/.default", http::uri_encode_slash::yes),
      client_id_,
      jwt_token);

    // AKS requires a TLS enabled client by default, but in test mode where we
    // use the http imposter, we need to use a simple client.
    auto tls_enabled = refresh_credentials::client_tls_enabled::yes;
    if (address().port() != default_port) {
        tls_enabled = refresh_credentials::client_tls_enabled::no;
    }

    co_return co_await request_with_payload(
      co_await make_api_client("azure_aks_oidc", tls_enabled),
      std::move(access_token_req),
      std::move(body));
}

api_response_parse_result azure_aks_refresh_impl::parse_response(iobuf resp) {
    // schema for (an example)
    //  {
    //    "token_type": "Bearer",
    //    "expires_in": 3599,
    //    "access_token":
    //    "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6Ik1uQ19WWmNBVGZNNXBP..."
    //  }
    constexpr static auto success_schema_str = R"json(
{
  "$schema": "http://json-schema.org/draft-04/schema#",
  "type": "object",
  "properties": {
    "token_type": {
      "type": "string",
      "pattern": "Bearer"
    },
    "expires_in": {
      "type": "integer"
    },
    "access_token": {
      "type": "string"
    }
  },
  "required": [
    "token_type", 
    "expires_in",
    "access_token"
  ]
}
)json";

    auto maybe_jresp = parse_json_response_and_validate(
      success_schema_str, std::move(resp));

    return ss::visit(
      maybe_jresp,
      [](auto err_resp) -> api_response_parse_result {
          return std::move(err_resp);
      },
      [&](const json::Document& jresp) -> api_response_parse_result {
          next_sleep_duration(
            std::chrono::seconds{jresp["expires_in"].GetInt()});
          auto& access_token = jresp["access_token"];
          return abs_oauth_credentials{oauth_token_str{ss::sstring{
            access_token.GetString(), access_token.GetStringLength()}}};
      });
}
} // namespace cloud_roles
