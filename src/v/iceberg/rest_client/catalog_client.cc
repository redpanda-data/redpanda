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

#include "bytes/streambuf.h"
#include "http/request_builder.h"
#include "http/utils.h"
#include "iceberg/json_writer.h"
#include "iceberg/rest_client/entities.h"
#include "iceberg/rest_client/json.h"
#include "iceberg/table_requests_json.h"
#include "json/istreamwrapper.h"

#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>

#include <absl/strings/str_join.h>
#include <absl/strings/strip.h>
#include <rapidjson/error/en.h>

namespace {

template<typename T>
T trim_slashes(std::optional<T> input, typename T::type default_value) {
    if (!input.has_value()) {
        return T{default_value};
    }
    return T{absl::StripSuffix(absl::StripPrefix(input.value()(), "/"), "/")};
}

template<typename T>
iobuf serialize_payload_as_json(const T& payload) {
    json::chunked_buffer buf;
    iceberg::json_writer writer(buf);
    rjson_serialize(writer, payload);

    return std::move(buf).as_iobuf();
}
static constexpr std::string_view json_content_type = "application/json";
} // namespace

namespace iceberg::rest_client {
namespace {
template<typename Func>
auto parse_as_expected(std::string_view ctx, Func&& parse_func) {
    using ret_t = std::invoke_result_t<Func, const json::Document&>;
    return [f = std::forward<Func>(parse_func),
            ctx](const json::Document& document) -> expected<ret_t> {
        try {
            return f(document);
        } catch (...) {
            return tl::unexpected<domain_error>(json_parse_error{
              .context = ss::sstring(ctx),
              .error = parse_error_msg{fmt::format(
                "error parsing JSON - {}", std::current_exception())},
            });
        }
    };
}
} // namespace

expected<json::Document> parse_json(iobuf&& raw_response) {
    iobuf_istreambuf ibuf{raw_response};
    std::istream stream{&ibuf};
    json::IStreamWrapper wrapper(stream);

    json::Document doc;
    doc.ParseStream(wrapper);

    if (doc.HasParseError()) {
        return tl::unexpected(json_parse_error{
          .context = "parse_json",
          .error = parse_error_msg{GetParseError_En(doc.GetParseError())}});
    }

    return doc;
}

catalog_client::catalog_client(
  std::unique_ptr<http::abstract_client> http_client,
  ss::sstring endpoint,
  credentials credentials,
  std::optional<base_path> base_path,
  std::optional<prefix_path> prefix,
  std::optional<api_version> api_version,
  std::optional<oauth_token> token,
  std::unique_ptr<retry_policy> retry_policy)
  : _http_client(std::move(http_client))
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
      .and_then(parse_as_expected("oauth_token", parse_oauth_token));
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
              return t.access_token;
          });
    }
    co_return _oauth_token->access_token;
}

ss::future<expected<iobuf>> catalog_client::perform_request(
  retry_chain_node& rtc,
  http::request_builder request_builder,
  std::optional<iobuf> payload) {
    if (payload.has_value()) {
        request_builder.with_content_length(payload.value().size_bytes());
    }

    std::vector<http_call_error> retriable_errors{};

    while (true) {
        const auto permit = rtc.retry();
        if (!permit.is_allowed) {
            co_return tl::unexpected(
              retries_exhausted{.errors = std::move(retriable_errors)});
        }
        auto request = request_builder.host(_endpoint).build();
        if (!request.has_value()) {
            co_return tl::unexpected(request.error());
        }

        auto response_f = co_await ss::coroutine::as_future(
          _http_client->request_and_collect_response(
            std::move(request.value()),
            payload.has_value() ? std::make_optional(payload->copy())
                                : std::nullopt));

        auto call_res = _retry_policy->should_retry(std::move(response_f));

        if (call_res.has_value()) {
            co_return std::move(call_res->body);
        }

        auto& error = call_res.error();
        if (!error.can_be_retried) {
            co_return tl::unexpected(std::move(error.err));
        }

        retriable_errors.emplace_back(std::move(error.err));
        co_await ss::sleep_abortable(permit.delay, rtc.root_abort_source());
    }
}

ss::future<expected<create_namespace_response>>
catalog_client::create_namespace(
  create_namespace_request req, retry_chain_node& rtc) {
    auto token = co_await ensure_token(rtc);
    if (!token.has_value()) {
        co_return tl::unexpected(token.error());
    }
    auto http_request = namespaces{root_path()}
                          .create()
                          .with_bearer_auth(token.value())
                          .with_content_type(json_content_type);

    co_return (co_await perform_request(
                 rtc, http_request, serialize_payload_as_json(req)))
      .and_then(parse_json)
      .and_then(
        parse_as_expected("create_namespace", parse_create_namespace_response));
}

ss::future<expected<load_table_result>> catalog_client::create_table(
  const chunked_vector<ss::sstring>& ns,
  create_table_request req,
  retry_chain_node& rtc) {
    auto token = co_await ensure_token(rtc);
    if (!token.has_value()) {
        co_return tl::unexpected(token.error());
    }
    auto http_request = table{root_path(), ns}
                          .create()
                          .with_bearer_auth(token.value())
                          .with_content_type(json_content_type);

    co_return (co_await perform_request(
                 rtc, http_request, serialize_payload_as_json(req)))
      .and_then(parse_json)
      .and_then(parse_as_expected("create_table", parse_load_table_result));
}

ss::future<expected<load_table_result>> catalog_client::load_table(
  const chunked_vector<ss::sstring>& ns,
  const ss::sstring& table_name,
  retry_chain_node& rtc) {
    auto token = co_await ensure_token(rtc);
    if (!token.has_value()) {
        co_return tl::unexpected(token.error());
    }

    auto http_request
      = table(root_path(), ns).get(table_name).with_bearer_auth(token.value());

    co_return (co_await perform_request(rtc, http_request))
      .and_then(parse_json)
      .and_then(parse_as_expected("load_table", parse_load_table_result));
}

ss::future<expected<std::monostate>> catalog_client::drop_table(
  const chunked_vector<ss::sstring>& ns,
  const ss::sstring& table_name,
  std::optional<bool> purge_requested,
  retry_chain_node& rtc) {
    auto token = co_await ensure_token(rtc);
    if (!token.has_value()) {
        co_return tl::unexpected(token.error());
    }

    http::rest_client::rest_entity::optional_query_params params;
    if (purge_requested.has_value()) {
        params.emplace();
        params.value()["purgeRequested"] = purge_requested ? "true" : "false";
    }

    auto http_request = table(root_path(), ns)
                          .delete_(table_name, std::nullopt, std::move(params))
                          .with_bearer_auth(token.value());

    co_return (co_await perform_request(rtc, http_request)).map([](iobuf&&) {
        // we expect empty response, discard it
        return std::monostate{};
    });
}

ss::future<expected<commit_table_response>> catalog_client::commit_table_update(
  commit_table_request commit_request, retry_chain_node& rtc) {
    auto token = co_await ensure_token(rtc);
    if (!token.has_value()) {
        co_return tl::unexpected(token.error());
    }

    auto http_request = table(root_path(), commit_request.identifier.ns)
                          .update(commit_request.identifier.table)
                          .with_bearer_auth(token.value())
                          .with_content_type(json_content_type);

    co_return (co_await perform_request(
                 rtc, http_request, serialize_payload_as_json(commit_request)))
      .and_then(parse_json)
      .and_then(
        parse_as_expected("commit_table_update", parse_commit_table_response));
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
