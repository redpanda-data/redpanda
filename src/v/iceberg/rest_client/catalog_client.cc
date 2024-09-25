/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "iceberg/rest_client/catalog_client.h"

#include "bytes/iobuf_parser.h"
#include "http/request_builder.h"
#include "http/utils.h"
#include "iceberg/rest_client/parsers.h"
#include "json/prettywriter.h"
#include "json/stringbuffer.h"

#include <seastar/coroutine/as_future.hh>

namespace {

ss::sstring pretty(const json::Document& doc) {
    json::StringBuffer sb;
    json::PrettyWriter<json::StringBuffer> w{sb};
    doc.Accept(w);
    return sb.GetString();
}

std::optional<std::string_view>
trim_slashes(std::optional<std::string_view> s) {
    if (!s.has_value()) {
        return s;
    }

    auto v = s.value();
    if (v.starts_with("/")) {
        v.remove_prefix(1);
    }

    if (v.ends_with("/")) {
        v.remove_suffix(1);
    }

    return v;
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
          .context = "", .error = GetParseError_En(doc.GetParseError())});
    }

    // TODO remove after debugging
    std::cerr << pretty(doc) << "\n";
    return doc;
}

catalog_client::catalog_client(
  http::abstract_client& client,
  std::string_view endpoint,
  credentials credentials,
  std::optional<std::string_view> base_url,
  std::optional<std::string_view> prefix,
  std::string_view api_version,
  std::unique_ptr<retry_policy> r)
  : _client{client}
  , _endpoint{endpoint}
  , _credentials{std::move(credentials)}
  , _base_url{trim_slashes(base_url).value_or("")}
  , _prefix{trim_slashes(prefix)}
  , _api_version{api_version}
  , _retry_policy{r ? std::move(r) : std::make_unique<default_retry_policy>()} {
}

ss::future<> catalog_client::acquire_token(retry_chain_node& rtc) {
    (co_await perform_request(
       rtc,
       http::request_builder{}
         .method(boost::beast::http::verb::post)
         .target(fmt::format("{}/{}/oauth/tokens", _base_url, _api_version))
         .header("content-type", "application/x-www-form-urlencoded"),
       http::form_encode_data({
         {"grant_type", "client_credentials"},
         {"client_id", _credentials.client_id},
         {"client_secret", _credentials.client_secret},
         {"scope", "PRINCIPAL_ROLE:ALL"},
       })))
      .and_then(parse_json)
      .and_then(parse_oauth_token)
      .map([this](auto token) { _oauth_token.emplace(std::move(token)); });

    if (!_oauth_token.has_value()) {
        throw std::runtime_error{"could not acquire token!"};
    }
}

ss::sstring catalog_client::token() const {
    return _oauth_token.value_or(oauth_token{}).token;
}

ss::sstring catalog_client::root_url() const {
    auto prefix = ss::sstring{};
    if (_prefix.has_value()) {
        prefix = fmt::format("{}/", _prefix.value());
    }
    return fmt::format("{}/{}/{}", _base_url, _api_version, prefix);
}

ss::future<> catalog_client::ensure_token_is_valid(retry_chain_node& rtc) {
    if (!_oauth_token.has_value()) {
        co_await acquire_token(rtc);
    }

    if (ss::lowres_clock::now() > _oauth_token->expires_at) {
        co_await acquire_token(rtc);
    }
}

ss::future<expected<iobuf>> catalog_client::perform_request(
  retry_chain_node& rtc,
  http::request_builder request_builder,
  std::optional<iobuf> payload) {
    if (payload.has_value()) {
        request_builder.with_size_of(payload.value());
    }

    auto request = request_builder.host(_endpoint).build();
    if (!request.has_value()) {
        co_return tl::unexpected(request.error());
    }

    std::vector<http_call_error> retriable_errors{};
    while (true) {
        if (!rtc.retry().is_allowed) {
            co_return tl::unexpected(
              retries_exhausted{.errors = std::move(retriable_errors)});
        }

        std::optional<iobuf> request_payload;
        if (payload.has_value()) {
            request_payload.emplace(payload->copy());
        }
        auto response_f = co_await ss::coroutine::as_future(
          _client.get().request_and_collect_response(
            std::move(request.value()), std::move(request_payload)));
        auto call_res = _retry_policy->should_retry(std::move(response_f));

        if (call_res.has_value()) {
            co_return std::move(call_res->body);
        }

        auto& error = call_res.error();
        if (!error.can_be_retried) {
            co_return tl::unexpected(error.err);
        }

        retriable_errors.emplace_back(std::move(error.err));
    }
}

} // namespace iceberg::rest_client
