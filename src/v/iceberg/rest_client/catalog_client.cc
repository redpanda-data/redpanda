/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/rest_client/catalog_client.h"

#include "bytes/iobuf_parser.h"
#include "http/request_builder.h"
#include "http/utils.h"
#include "iceberg/rest_client/parsers.h"

#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>

#include <absl/strings/str_join.h>
#include <absl/strings/strip.h>

namespace {

template<typename T>
T trim_slashes(std::optional<T> input, typename T::type default_value) {
    if (!input.has_value()) {
        return T{default_value};
    }
    return T{absl::StripSuffix(absl::StripPrefix(input.value()(), "/"), "/")};
}

} // namespace

namespace iceberg::rest_client {

expected<json::Document> parse_json(iobuf&& raw_response) {
    iobuf_parser p{std::move(raw_response)};
    auto raw_json = p.read_string(p.bytes_left());

    json::Document doc;
    doc.Parse(raw_json);

    if (doc.HasParseError()) {
        return tl::unexpected(json_parse_error{
          .context = "parse_json",
          .error = parse_error_msg{GetParseError_En(doc.GetParseError())}});
    }

    return doc;
}

catalog_client::catalog_client(
  client_source& client_source,
  ss::sstring endpoint,
  credentials credentials,
  std::optional<base_path> base_path,
  std::optional<prefix_path> prefix,
  std::optional<api_version> api_version,
  std::optional<oauth_token> token,
  std::unique_ptr<retry_policy> retry_policy)
  : _client_source{client_source}
  , _endpoint{std::move(endpoint)}
  , _credentials{std::move(credentials)}
  , _path_components{std::move(base_path), std::move(prefix), std::move(api_version)}
  , _oauth_token{std::move(token)}
  , _retry_policy{
      retry_policy ? std::move(retry_policy)
                   : std::make_unique<default_retry_policy>()} {}

ss::future<expected<oauth_token>>
catalog_client::acquire_token(retry_chain_node& rtc) {
    const auto token_request
      = http::request_builder{}
          .method(boost::beast::http::verb::post)
          .path(_path_components.token_api_path())
          .header("content-type", "application/x-www-form-urlencoded");
    auto payload = http::form_encode_data({
      {"grant_type", "client_credentials"},
      {"client_id", _credentials.client_id},
      {"client_secret", _credentials.client_secret},
      // TODO - parameterize this scope, the principal_role should be an input
      // to the catalog client
      {"scope", "PRINCIPAL_ROLE:ALL"},
    });
    co_return (co_await perform_request(rtc, token_request, std::move(payload)))
      .and_then(parse_json)
      .and_then(parse_oauth_token);
}

ss::sstring catalog_client::root_path() const {
    return _path_components.root_path();
}

ss::future<expected<ss::sstring>>
catalog_client::ensure_token(retry_chain_node& rtc) {
    const bool need_token = !_oauth_token.has_value()
                            || _oauth_token->expires_at
                                 < ss::lowres_clock::now();
    if (need_token) {
        co_return (co_await acquire_token(rtc))
          .and_then([this](auto t) -> expected<ss::sstring> {
              _oauth_token.emplace(t);
              return t.token;
          });
    }
    co_return _oauth_token->token;
}

ss::future<expected<iobuf>> catalog_client::perform_request(
  retry_chain_node& rtc,
  http::request_builder request_builder,
  std::optional<iobuf> payload) {
    if (payload.has_value()) {
        request_builder.with_content_length(payload.value().size_bytes());
    }

    auto request = request_builder.host(_endpoint).build();
    if (!request.has_value()) {
        co_return tl::unexpected(request.error());
    }

    std::vector<http_call_error> retriable_errors{};

    auto client_ptr = _client_source.get().acquire();
    while (true) {
        const auto permit = rtc.retry();
        if (!permit.is_allowed) {
            co_await client_ptr->shutdown_and_stop();
            co_return tl::unexpected(
              retries_exhausted{.errors = std::move(retriable_errors)});
        }

        std::optional<iobuf> request_payload;
        if (payload.has_value()) {
            request_payload.emplace(payload->copy());
        }
        auto response_f = co_await ss::coroutine::as_future(
          client_ptr->request_and_collect_response(
            std::move(request.value()), std::move(request_payload)));
        auto call_res = _retry_policy->should_retry(std::move(response_f));

        if (call_res.has_value()) {
            co_await client_ptr->shutdown_and_stop();
            co_return std::move(call_res->body);
        }

        auto& error = call_res.error();
        if (!error.can_be_retried) {
            co_await client_ptr->shutdown_and_stop();
            co_return tl::unexpected(std::move(error.err));
        }

        if (error.is_transport_error()) {
            co_await client_ptr->shutdown_and_stop();
            client_ptr = _client_source.get().acquire();
        }

        retriable_errors.emplace_back(std::move(error.err));
        co_await ss::sleep_abortable(permit.delay, rtc.root_abort_source());
    }
    co_await client_ptr->shutdown_and_stop();
}

path_components::path_components(
  std::optional<base_path> base,
  std::optional<prefix_path> prefix,
  std::optional<api_version> api_version)
  : _base{trim_slashes(std::move(base), "")}
  , _prefix{trim_slashes(std::move(prefix), "")}
  , _api_version{trim_slashes(std::move(api_version), "v1")} {}

ss::sstring path_components::root_path() const {
    std::vector<ss::sstring> parts;
    if (!_base().empty()) {
        parts.push_back(_base());
    }

    parts.push_back(_api_version());

    if (!_prefix().empty()) {
        parts.push_back(_prefix());
    }

    return absl::StrJoin(parts, "/") + "/";
}

ss::sstring path_components::token_api_path() const {
    std::vector<ss::sstring> parts;
    if (!_base().empty()) {
        parts.push_back(_base());
    }

    parts.push_back(_api_version());
    return absl::StrJoin(parts, "/") + "/oauth/tokens";
}

} // namespace iceberg::rest_client
