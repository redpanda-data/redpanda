/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage_clients/abs_client.h"

#include "base/vlog.h"
#include "bytes/iostream.h"
#include "bytes/streambuf.h"
#include "cloud_storage_clients/abs_error.h"
#include "cloud_storage_clients/configuration.h"
#include "cloud_storage_clients/logger.h"
#include "cloud_storage_clients/util.h"
#include "cloud_storage_clients/xml_sax_parser.h"
#include "config/configuration.h"
#include "json/document.h"
#include "json/istreamwrapper.h"

#include <utility>

namespace {

// These are the HTTP codes on which Microsoft's SDKs retry requests.
// The mapping to ABS error codes is:
// [500 -> "InternalError", 500 ->, "OperationTimedOut", 503 -> "ServerBusy"].
// Note how some of these HTTP codes don't have a corresponding ABS error code,
// and, hence shouldn't be returned by the server. It's likely that these extra
// HTTP codes were used by older versions of the server, and since we can't
// verify that, we keep them here.
constexpr std::array<boost::beast::http::status, 6> retryable_http_codes = {
  boost::beast::http::status::request_timeout,       // 408
  boost::beast::http::status::too_many_requests,     // 429
  boost::beast::http::status::internal_server_error, // 500
  boost::beast::http::status::bad_gateway,           // 502
  boost::beast::http::status::service_unavailable,   // 503
  boost::beast::http::status::gateway_timeout,       // 504
};

constexpr boost::beast::string_view content_type_value = "text/plain";
constexpr boost::beast::string_view blob_type_value = "BlockBlob";
constexpr boost::beast::string_view blob_type_name = "x-ms-blob-type";
constexpr boost::beast::string_view delete_snapshot_name
  = "x-ms-delete-snapshots";
constexpr boost::beast::string_view is_hns_enabled_name = "x-ms-is-hns-enabled";
constexpr boost::beast::string_view delete_snapshot_value = "include";
constexpr boost::beast::string_view error_code_name = "x-ms-error-code";
constexpr boost::beast::string_view content_type_name = "Content-Type";
constexpr boost::beast::string_view expiry_option_name = "x-ms-expiry-option";
constexpr boost::beast::string_view expiry_option_value = "RelativeToNow";
constexpr boost::beast::string_view expiry_time_name = "x-ms-expiry-time";

constexpr boost::beast::string_view
  hierarchical_namespace_not_enabled_error_code
  = "HierarchicalNamespaceNotEnabled";

// filename for the set expiry test file
constexpr std::string_view set_expiry_test_file = "testsetexpiry";

bool is_error_retryable(
  const cloud_storage_clients::abs_rest_error_response& err) {
    return std::find(
             retryable_http_codes.begin(),
             retryable_http_codes.end(),
             err.http_code())
           != retryable_http_codes.end();
}
} // namespace

namespace cloud_storage_clients {

enum class response_content_type : int8_t { unknown, xml, json };

static response_content_type
get_response_content_type(const http::client::response_header& headers) {
    if (auto iter = headers.find(content_type_name); iter != headers.end()) {
        if (iter->value().find("json") != std::string_view::npos) {
            return response_content_type::json;
        }

        if (iter->value().find("xml") != std::string_view::npos) {
            return response_content_type::xml;
        }
    }

    return response_content_type::unknown;
}

static abs_rest_error_response
parse_xml_rest_error_response(boost::beast::http::status result, iobuf buf) {
    using namespace cloud_storage_clients;

    try {
        auto resp = util::iobuf_to_ptree(std::move(buf), abs_log);
        auto code = resp.get<ss::sstring>("Error.Code", "Unknown");
        auto msg = resp.get<ss::sstring>("Error.Message", "");
        return {std::move(code), std::move(msg), result};
    } catch (...) {
        vlog(
          cloud_storage_clients::abs_log.error,
          "Failed to parse ABS error response {}",
          std::current_exception());
        throw;
    }
}

static abs_rest_error_response
parse_json_rest_error_response(boost::beast::http::status result, iobuf buf) {
    using namespace cloud_storage_clients;

    iobuf_istreambuf strbuf{buf};
    std::istream stream{&strbuf};
    json::IStreamWrapper wrapper{stream};

    json::Document doc;
    if (doc.ParseStream(wrapper).HasParseError()) {
        vlog(
          cloud_storage_clients::abs_log.error,
          "Failed to parse ABS error response: {}",
          doc.GetParseError());

        throw std::runtime_error(ssx::sformat(
          "Failed to parse JSON ABS error response: {}", doc.GetParseError()));
    }

    std::optional<ss::sstring> code;
    std::optional<ss::sstring> member;
    if (auto error_it = doc.FindMember("error"); error_it != doc.MemberEnd()) {
        const auto& error = error_it->value;
        if (auto code_it = error.FindMember("code");
            code_it != error.MemberEnd()) {
            code = code_it->value.GetString();
        }

        if (auto member_it = error.FindMember("member");
            member_it != error.MemberEnd()) {
            member = member_it->value.GetString();
        }
    }

    return {code.value_or("Unknown"), member.value_or(""), result};
}

static abs_rest_error_response parse_rest_error_response(
  response_content_type type, boost::beast::http::status result, iobuf buf) {
    if (type == response_content_type::xml) {
        return parse_xml_rest_error_response(result, std::move(buf));
    }

    if (type == response_content_type::json) {
        return parse_json_rest_error_response(result, std::move(buf));
    }

    return abs_rest_error_response{"Unknown", "", result};
}

static abs_rest_error_response
parse_header_error_response(const http::http_response::header_type& hdr) {
    ss::sstring code{"Unknown"};
    if (auto it = hdr.find(error_code_name); it != hdr.end()) {
        code = ss::sstring{it->value().data(), it->value().size()};
    }

    ss::sstring message{hdr.reason().data(), hdr.reason().size()};

    return {code, message, hdr.result()};
}

abs_request_creator::abs_request_creator(
  const abs_configuration& conf,
  ss::lw_shared_ptr<const cloud_roles::apply_credentials> apply_credentials)
  : _ap{conf.uri}
  , _apply_credentials{std::move(apply_credentials)} {}

result<http::client::request_header> abs_request_creator::make_get_blob_request(
  const bucket_name& name,
  const object_key& key,
  std::optional<http_byte_range> byte_range) {
    // GET /{container-id}/{blob-id} HTTP/1.1
    // Host: {storage-account-id}.blob.core.windows.net
    // x-ms-date:{req-datetime in RFC9110} # added by 'add_auth'
    // x-ms-version:"2023-01-23"           # added by 'add_auth'
    // Authorization:{signature}           # added by 'add_auth'
    const auto target = fmt::format("/{}/{}", name(), key().string());
    const boost::beast::string_view host{_ap().data(), _ap().length()};

    http::client::request_header header{};
    header.method(boost::beast::http::verb::get);
    header.target(target);
    header.insert(boost::beast::http::field::host, host);
    if (byte_range.has_value()) {
        header.insert(
          boost::beast::http::field::range,
          fmt::format(
            "bytes={}-{}",
            byte_range.value().first,
            byte_range.value().second));
    }
    auto error_code = _apply_credentials->add_auth(header);
    if (error_code) {
        return error_code;
    }

    return header;
}

result<http::client::request_header> abs_request_creator::make_put_blob_request(
  const bucket_name& name, const object_key& key, size_t payload_size_bytes) {
    // PUT /{container-id}/{blob-id} HTTP/1.1
    // Host: {storage-account-id}.blob.core.windows.net
    // x-ms-date:{req-datetime in RFC9110} # added by 'add_auth'
    // x-ms-version:"2023-01-23"           # added by 'add_auth'
    // Authorization:{signature}           # added by 'add_auth'
    // Content-Length:{payload-size}
    // Content-Type: text/plain
    const auto target = fmt::format("/{}/{}", name(), key().string());
    const boost::beast::string_view host{_ap().data(), _ap().length()};

    http::client::request_header header{};
    header.method(boost::beast::http::verb::put);
    header.target(target);
    header.insert(boost::beast::http::field::host, host);
    header.insert(boost::beast::http::field::content_type, content_type_value);
    header.insert(
      boost::beast::http::field::content_length,
      std::to_string(payload_size_bytes));
    header.insert(blob_type_name, blob_type_value);

    auto error_code = _apply_credentials->add_auth(header);
    if (error_code) {
        return error_code;
    }

    return header;
}

result<http::client::request_header>
abs_request_creator::make_get_blob_metadata_request(
  const bucket_name& name, const object_key& key) {
    // HEAD /{container-id}/{blob-id}?comp=metadata HTTP/1.1
    // Host: {storage-account-id}.blob.core.windows.net
    // x-ms-date:{req-datetime in RFC9110} # added by 'add_auth'
    // x-ms-version:"2023-01-23"           # added by 'add_auth'
    // Authorization:{signature}           # added by 'add_auth'
    const auto target = fmt::format(
      "/{}/{}?comp=metadata", name(), key().string());
    const boost::beast::string_view host{_ap().data(), _ap().length()};

    http::client::request_header header{};
    header.method(boost::beast::http::verb::head);
    header.target(target);
    header.insert(boost::beast::http::field::host, host);

    auto error_code = _apply_credentials->add_auth(header);
    if (error_code) {
        return error_code;
    }

    return header;
}

result<http::client::request_header>
abs_request_creator::make_delete_blob_request(
  const bucket_name& name, const object_key& key) {
    // DELETE /{container-id}/{blob-id} HTTP/1.1
    // Host: {storage-account-id}.blob.core.windows.net
    // x-ms-date:{req-datetime in RFC9110} # added by 'add_auth'
    // x-ms-version:"2023-01-23"           # added by 'add_auth'
    // Authorization:{signature}           # added by 'add_auth'
    const auto target = fmt::format("/{}/{}", name(), key().string());

    const boost::beast::string_view host{_ap().data(), _ap().length()};

    http::client::request_header header{};
    header.method(boost::beast::http::verb::delete_);
    header.target(target);
    header.insert(boost::beast::http::field::host, host);
    header.insert(delete_snapshot_name, delete_snapshot_value);

    auto error_code = _apply_credentials->add_auth(header);
    if (error_code) {
        return error_code;
    }

    return header;
}

result<http::client::request_header>
abs_request_creator::make_list_blobs_request(
  const bucket_name& name,
  bool files_only,
  std::optional<object_key> prefix,
  std::optional<size_t> max_results,
  std::optional<ss::sstring> marker,
  std::optional<char> delimiter) {
    // GET /{container-id}?restype=container&comp=list&prefix={prefix}...
    // ...&maxresults{max_keys}
    // HTTP/1.1 Host: {storage-account-id}.blob.core.windows.net
    // x-ms-date:{req-datetime in RFC9110} # added by 'add_auth'
    // x-ms-version:"2023-01-23"           # added by 'add_auth'
    // Authorization:{signature}           # added by 'add_auth'
    auto target = fmt::format("/{}?restype=container&comp=list", name());
    if (prefix) {
        target += fmt::format("&prefix={}", prefix.value()().string());
    }

    if (max_results) {
        target += fmt::format("&maxresults={}", max_results.value());
    }

    if (delimiter) {
        target += fmt::format("&delimiter={}", delimiter.value());
    }

    if (marker.has_value()) {
        target += fmt::format("&marker={}", marker.value());
    }

    if (files_only) {
        target += fmt::format("&showonly=files");
    }

    const boost::beast::string_view host{_ap().data(), _ap().length()};

    http::client::request_header header{};
    header.method(boost::beast::http::verb::get);
    header.target(target);
    header.insert(boost::beast::http::field::host, host);

    auto error_code = _apply_credentials->add_auth(header);
    if (error_code) {
        return error_code;
    }

    return header;
}

result<http::client::request_header>
abs_request_creator::make_get_account_info_request() {
    const boost::beast::string_view target
      = "/?restype=account&comp=properties";
    const boost::beast::string_view host{_ap().data(), _ap().length()};

    http::client::request_header header{};
    header.method(boost::beast::http::verb::head);
    header.target(target);
    header.insert(boost::beast::http::field::host, host);

    auto error_code = _apply_credentials->add_auth(header);
    if (error_code) {
        return error_code;
    }

    return header;
}

result<http::client::request_header>
abs_request_creator::make_set_expiry_to_blob_request(
  const bucket_name& name,
  const object_key& key,
  ss::lowres_clock::duration expires_in) const {
    // https://learn.microsoft.com/en-us/rest/api/storageservices/set-blob-expiry?tabs=microsoft-entra-id
    // available only if HNS are enabled for the bucket
    // performs curl -v -X PUT -H "Authorization: Bearer ${AUTH_CODE}" -H
    // "x-ms-version: 2023-01-03" -H "x-ms-expiry-option: RelativeToNow" -H
    // "x-ms-expiry-time: 30000" -d {}
    // "https://testingimds2ab.blob.core.windows.net/testingcontainer/testHNS?comp=expiry"

    auto header = http::client::request_header{};

    header.method(boost::beast::http::verb::put);
    header.target(fmt::format("/{}/{}?comp=expiry", name(), key().string()));
    header.set(boost::beast::http::field::host, {_ap().data(), _ap().size()});
    header.set(expiry_option_name, expiry_option_value);
    header.set(
      expiry_time_name,
      fmt::format(
        "{}",
        std::chrono::duration_cast<std::chrono::milliseconds>(expires_in)
          .count()));
    header.set(boost::beast::http::field::content_length, "0");
    if (auto error_code = _apply_credentials->add_auth(header);
        error_code != std::error_code{}) {
        return error_code;
    }
    return header;
}

result<http::client::request_header>
abs_request_creator::make_delete_file_request(
  const access_point_uri& adls_ap,
  const bucket_name& name,
  const object_key& path) {
    // DELETE /{container-id}/{path} HTTP/1.1
    // Host: {storage-account-id}.dfs.core.windows.net
    // x-ms-date:{req-datetime in RFC9110} # added by 'add_auth'
    // x-ms-version:"2023-01-23"           # added by 'add_auth'
    // Authorization:{signature}           # added by 'add_auth'
    const auto target = fmt::format("/{}/{}", name(), path().string());

    const boost::beast::string_view host{adls_ap().data(), adls_ap().length()};

    http::client::request_header header{};
    header.method(boost::beast::http::verb::delete_);
    header.target(target);
    header.insert(boost::beast::http::field::host, host);

    auto error_code = _apply_credentials->add_auth(header);
    if (error_code) {
        return error_code;
    }

    return header;
}

abs_client::abs_client(
  const abs_configuration& conf,
  ss::lw_shared_ptr<const cloud_roles::apply_credentials> apply_credentials)
  : _data_lake_v2_client_config(
      conf.is_hns_enabled ? std::make_optional(conf.make_adls_configuration())
                          : std::nullopt)
  , _is_oauth(apply_credentials->is_oauth())
  , _requestor(conf, std::move(apply_credentials))
  , _client(conf)
  , _adls_client(
      conf.is_hns_enabled ? std::make_optional(*_data_lake_v2_client_config)
                          : std::nullopt)
  , _probe(conf._probe) {
    vlog(abs_log.trace, "Created client with config:{}", conf);
}

abs_client::abs_client(
  const abs_configuration& conf,
  const ss::abort_source& as,
  ss::lw_shared_ptr<const cloud_roles::apply_credentials> apply_credentials)
  : _data_lake_v2_client_config(
      conf.is_hns_enabled ? std::make_optional(conf.make_adls_configuration())
                          : std::nullopt)
  , _is_oauth(apply_credentials->is_oauth())
  , _requestor(conf, std::move(apply_credentials))
  , _client(conf, &as, conf._probe, conf.max_idle_time)
  , _adls_client(
      conf.is_hns_enabled ? std::make_optional(*_data_lake_v2_client_config)
                          : std::nullopt)
  , _probe(conf._probe) {
    vlog(abs_log.trace, "Created client with config:{}", conf);
}

ss::future<result<client_self_configuration_output, error_outcome>>
abs_client::self_configure() {
    auto& cfg = config::shard_local_cfg();
    auto hns_enabled
      = cfg.cloud_storage_azure_hierarchical_namespace_enabled.value();

    if (hns_enabled.has_value()) {
        // use override cluster property to skip check
        co_return abs_self_configuration_result{
          .is_hns_enabled = hns_enabled.value()};
    }

    auto result = co_await get_account_info(http::default_connect_timeout);
    if (!result) {
        co_return result.error();
    } else {
        co_return abs_self_configuration_result{
          .is_hns_enabled = result.value().is_hns_enabled};
    }
}

ss::future<> abs_client::stop() {
    vlog(abs_log.debug, "Stopping ABS client");

    co_await _client.stop();
    co_await _client.wait_input_shutdown();

    if (_adls_client) {
        co_await _adls_client->stop();
        co_await _adls_client->wait_input_shutdown();
    }

    vlog(abs_log.debug, "Stopped ABS client");
}

void abs_client::shutdown() { _client.shutdown(); }

template<typename T>
ss::future<result<T, error_outcome>> abs_client::send_request(
  ss::future<T> request_future,
  const object_key& key,
  std::optional<op_type_tag> op_type) {
    using namespace boost::beast::http;

    auto outcome = error_outcome::fail;

    try {
        co_return co_await std::move(request_future);
    } catch (const abs_rest_error_response& err) {
        if (is_error_retryable(err)) {
            vlog(
              abs_log.warn,
              "Received [{}] {} retryable error response from ABS: {}",
              err.http_code(),
              err.code_string(),
              err.message());
            outcome = error_outcome::retry;
            _probe->register_retryable_failure(op_type);
        } else if (
          err.http_code() == status::forbidden
          || err.http_code() == status::unauthorized) {
            vlog(
              abs_log.error,
              "Received [{}] {} error response from ABS. This indicates "
              "misconfiguration of ABS and/or Redpanda: {}",
              err.http_code(),
              err.code_string(),
              err.message());
            outcome = error_outcome::fail;
            _probe->register_failure(err.code());
        } else if (
          err.code() == abs_error_code::container_being_disabled
          || err.code() == abs_error_code::container_being_deleted
          || err.code() == abs_error_code::container_not_found) {
            vlog(
              abs_log.error,
              "Received [{}] {} error response from ABS. This indicates "
              "that your container is not available. Remediate the issue for "
              "uploads to resume: {}",
              err.http_code(),
              err.code_string(),
              err.message());
            outcome = error_outcome::fail;
            _probe->register_failure(err.code());
        } else if (err.code() == abs_error_code::blob_not_found) {
            // Unexpected 404s are logged by 'request_future' at warn
            // level, so only log at debug level here.
            vlog(abs_log.debug, "BlobNotFound response received {}", key);
            outcome = error_outcome::key_not_found;
            _probe->register_failure(err.code());
        } else if (
          err.code() == abs_error_code::operation_not_supported_on_directory) {
            vlog(
              abs_log.debug,
              "OperationNotSupportedOnDirectory response received {}",
              key);
            outcome = error_outcome::operation_not_supported;
            _probe->register_failure(err.code());
        } else {
            vlog(
              abs_log.error,
              "Received [{}] {} unexpected error response from ABS: {}",
              err.http_code(),
              err.code_string(),
              err.message());
            outcome = error_outcome::fail;
            _probe->register_failure(err.code());
        }
    } catch (...) {
        _probe->register_failure(abs_error_code::_unknown);

        outcome = util::handle_client_transport_error(
          std::current_exception(), abs_log);
    }

    co_return outcome;
}

ss::future<result<http::client::response_stream_ref, error_outcome>>
abs_client::get_object(
  const bucket_name& name,
  const object_key& key,
  ss::lowres_clock::duration timeout,
  bool expect_no_such_key,
  std::optional<http_byte_range> byte_range) {
    return send_request(
      do_get_object(
        name, key, timeout, expect_no_such_key, std::move(byte_range)),
      key,
      op_type_tag::download);
}

ss::future<http::client::response_stream_ref> abs_client::do_get_object(
  const bucket_name& name,
  const object_key& key,
  ss::lowres_clock::duration timeout,
  bool expect_no_such_key,
  std::optional<http_byte_range> byte_range) {
    bool is_byte_range_requested = byte_range.has_value();
    auto header = _requestor.make_get_blob_request(
      name, key, std::move(byte_range));
    if (!header) {
        vlog(
          abs_log.warn, "Failed to create request header: {}", header.error());
        throw std::system_error(header.error());
    }

    vlog(abs_log.trace, "send https request:\n{}", header.value());

    auto response_stream = co_await _client.request(
      std::move(header.value()), timeout);

    co_await response_stream->prefetch_headers();
    vassert(response_stream->is_header_done(), "Header is not received");

    const auto status = response_stream->get_headers().result();
    bool request_failed = status != boost::beast::http::status::ok;
    if (is_byte_range_requested) {
        request_failed &= status != boost::beast::http::status::partial_content;
    }
    if (request_failed) {
        if (
          expect_no_such_key
          && status == boost::beast::http::status::not_found) {
            vlog(
              abs_log.debug,
              "ABS replied with expected error: {:l}",
              response_stream->get_headers());
        } else {
            vlog(
              abs_log.warn,
              "ABS replied with error: {:l}",
              response_stream->get_headers());
        }

        const auto content_type = get_response_content_type(
          response_stream->get_headers());
        auto buf = co_await util::drain_response_stream(
          std::move(response_stream));
        throw parse_rest_error_response(content_type, status, std::move(buf));
    }

    co_return response_stream;
}

ss::future<result<abs_client::no_response, error_outcome>>
abs_client::put_object(
  const bucket_name& name,
  const object_key& key,
  size_t payload_size,
  ss::input_stream<char> body,
  ss::lowres_clock::duration timeout,
  bool accept_no_content) {
    return send_request(
      do_put_object(
        name, key, payload_size, std::move(body), timeout, accept_no_content)
        .then(
          []() { return ss::make_ready_future<no_response>(no_response{}); }),
      key,
      op_type_tag::upload);
}

ss::future<> abs_client::do_put_object(
  const bucket_name& name,
  const object_key& key,
  size_t payload_size,
  ss::input_stream<char> body,
  ss::lowres_clock::duration timeout,
  bool accept_no_content) {
    auto header = _requestor.make_put_blob_request(name, key, payload_size);
    if (!header) {
        co_await body.close();

        vlog(
          abs_log.warn, "Failed to create request header: {}", header.error());
        throw std::system_error(header.error());
    }

    vlog(abs_log.trace, "send https request:\n{}", header.value());

    auto response_stream = co_await _client
                             .request(std::move(header.value()), body, timeout)
                             .finally([&body] { return body.close(); });

    co_await response_stream->prefetch_headers();
    vassert(response_stream->is_header_done(), "Header is not received");

    const auto status = response_stream->get_headers().result();
    using enum boost::beast::http::status;

    if (const auto is_no_content_and_accepted = accept_no_content
                                                && status == no_content;
        status != created && !is_no_content_and_accepted) {
        const auto content_type = get_response_content_type(
          response_stream->get_headers());
        auto buf = co_await util::drain_response_stream(
          std::move(response_stream));
        throw parse_rest_error_response(content_type, status, std::move(buf));
    }
}

ss::future<result<abs_client::head_object_result, error_outcome>>
abs_client::head_object(
  const bucket_name& name,
  const object_key& key,
  ss::lowres_clock::duration timeout) {
    return send_request(do_head_object(name, key, timeout), key);
}

ss::future<abs_client::head_object_result> abs_client::do_head_object(
  const bucket_name& name,
  const object_key& key,
  ss::lowres_clock::duration timeout) {
    auto header = _requestor.make_get_blob_metadata_request(name, key);
    if (!header) {
        vlog(
          abs_log.warn, "Failed to create request header: {}", header.error());
        throw std::system_error(header.error());
    }

    vlog(abs_log.trace, "send https request:\n{}", header.value());

    auto response_stream = co_await _client.request(
      std::move(header.value()), timeout);

    co_await response_stream->prefetch_headers();
    vassert(response_stream->is_header_done(), "Header is not received");

    const auto status = response_stream->get_headers().result();
    if (status == boost::beast::http::status::ok) {
        const auto etag = response_stream->get_headers().at(
          boost::beast::http::field::etag);
        const auto size = boost::lexical_cast<uint64_t>(
          response_stream->get_headers().at(
            boost::beast::http::field::content_length));
        co_return head_object_result{
          .object_size = size, .etag = ss::sstring{etag.data(), etag.length()}};
    } else {
        throw parse_header_error_response(response_stream->get_headers());
    }
}

ss::future<result<abs_client::no_response, error_outcome>>
abs_client::delete_object(
  const bucket_name& name,
  const object_key& key,
  ss::lowres_clock::duration timeout) {
    using ret_t = result<no_response, error_outcome>;
    if (!_adls_client) {
        return send_request(
                 do_delete_object(name, key, timeout).then([]() {
                     return ss::make_ready_future<no_response>(no_response{});
                 }),
                 key)
          .then([&name, &key](const ret_t& result) {
              if (!result) {
                  if (result.error() == error_outcome::key_not_found) {
                      // ABS returns a 404 for attempts to delete a blob that
                      // doesn't exist. The remote doesn't expect this, so we
                      // map 404s to a successful response.
                      vlog(
                        abs_log.debug,
                        "Object to be deleted was not found in cloud storage: "
                        "object={}, bucket={}. Ignoring ...",
                        name,
                        key);
                      return ss::make_ready_future<ret_t>(no_response{});
                  } else if (
                    result.error() == error_outcome::operation_not_supported) {
                      // ABS does not allow for deletion of directories when HNS
                      // is disabled. The "folder" is "removed" when all blobs
                      // inside of it are deleted. Map this to a successful
                      // response.
                      vlog(
                        abs_log.warn,
                        "Cannot delete a directory in ABS cloud storage: "
                        "object={}, bucket={}. Ignoring ...",
                        name,
                        key);
                      return ss::make_ready_future<ret_t>(no_response{});
                  }
              }
              return ss::make_ready_future<ret_t>(result);
          });
    } else {
        return delete_path(name, key, timeout);
    }
}

ss::future<> abs_client::do_delete_object(
  const bucket_name& name,
  const object_key& key,
  ss::lowres_clock::duration timeout) {
    auto header = _requestor.make_delete_blob_request(name, key);
    if (!header) {
        vlog(
          abs_log.warn, "Failed to create request header: {}", header.error());
        throw std::system_error(header.error());
    }

    vlog(abs_log.trace, "send https request:\n{}", header.value());

    auto response_stream = co_await _client.request(
      std::move(header.value()), timeout);

    co_await response_stream->prefetch_headers();
    vassert(response_stream->is_header_done(), "Header is not received");

    const auto status = response_stream->get_headers().result();
    if (status != boost::beast::http::status::accepted) {
        const auto content_type = get_response_content_type(
          response_stream->get_headers());
        auto buf = co_await util::drain_response_stream(
          std::move(response_stream));
        throw parse_rest_error_response(content_type, status, std::move(buf));
    }
}

ss::future<result<abs_client::delete_objects_result, error_outcome>>
abs_client::delete_objects(
  const bucket_name& bucket,
  std::vector<object_key> keys,
  ss::lowres_clock::duration timeout) {
    abs_client::delete_objects_result delete_objects_result;
    for (const auto& key : keys) {
        try {
            auto res = co_await delete_object(bucket, key, timeout);
            if (res.has_error()) {
                delete_objects_result.undeleted_keys.push_back(
                  {key, fmt::format("{}", res.error())});
            }
        } catch (const std::exception& ex) {
            delete_objects_result.undeleted_keys.push_back({key, ex.what()});
        }
    }
    co_return delete_objects_result;
}

ss::future<result<abs_client::list_bucket_result, error_outcome>>
abs_client::list_objects(
  const bucket_name& name,
  std::optional<object_key> prefix,
  [[maybe_unused]] std::optional<object_key> start_after,
  std::optional<size_t> max_keys,
  std::optional<ss::sstring> continuation_token,
  ss::lowres_clock::duration timeout,
  std::optional<char> delimiter,
  std::optional<item_filter> collect_item_if) {
    return send_request(
      do_list_objects(
        name,
        std::move(prefix),
        max_keys,
        std::move(continuation_token),
        timeout,
        delimiter,
        std::move(collect_item_if)),
      object_key{""});
}

ss::future<abs_client::list_bucket_result> abs_client::do_list_objects(
  const bucket_name& name,
  std::optional<object_key> prefix,
  std::optional<size_t> max_results,
  std::optional<ss::sstring> marker,
  ss::lowres_clock::duration timeout,
  std::optional<char> delimiter,
  std::optional<item_filter>) {
    auto header = _requestor.make_list_blobs_request(
      name,
      _adls_client.has_value(),
      std::move(prefix),
      max_results,
      std::move(marker),
      delimiter);
    if (!header) {
        vlog(
          abs_log.warn, "Failed to create request header: {}", header.error());
        throw std::system_error(header.error());
    }

    vlog(abs_log.trace, "send https request:\n{}", header.value());

    auto response_stream = co_await _client.request(
      std::move(header.value()), timeout);

    co_await response_stream->prefetch_headers();
    vassert(response_stream->is_header_done(), "Header is not received");
    const auto status = response_stream->get_headers().result();

    if (status != boost::beast::http::status::ok) {
        const auto content_type = get_response_content_type(
          response_stream->get_headers());
        iobuf buf = co_await util::drain_response_stream(response_stream);
        throw parse_rest_error_response(content_type, status, std::move(buf));
    }

    co_return co_await ss::do_with(
      response_stream->as_input_stream(),
      xml_sax_parser{},
      [](ss::input_stream<char>& stream, xml_sax_parser& p) mutable {
          p.start_parse(std::make_unique<abs_parse_impl>());
          return ss::do_until(
                   [&stream] { return stream.eof(); },
                   [&stream, &p] {
                       return stream.read().then(
                         [&p](ss::temporary_buffer<char>&& chunk) {
                             p.parse_chunk(std::move(chunk));
                         });
                   })
            .then([&stream] { return stream.close(); })
            .then([&p] {
                p.end_parse();
                return p.result();
            });
      });
}

ss::future<result<abs_client::storage_account_info, error_outcome>>
abs_client::get_account_info(ss::lowres_clock::duration timeout) {
    if (_is_oauth) {
        return send_request(
          do_test_set_expiry_on_dummy_file(timeout), object_key{""});
    } else {
        return send_request(do_get_account_info(timeout), object_key{""});
    }
}

ss::future<abs_client::storage_account_info>
abs_client::do_test_set_expiry_on_dummy_file(
  ss::lowres_clock::duration timeout) {
    // since this is one-off operation at startup, it's easier to read directly
    // cloud_storage_azure_container than to wire it in. this is ok because if
    // we are in abs_client it means that the required properties, like
    // azure_container, are set
    auto container_name
      = config::shard_local_cfg().cloud_storage_azure_container.value();

    if (unlikely(!container_name.has_value())) {
        vlog(abs_log.error, "Failed to get azure container name from config");
        throw std::runtime_error("cloud_storage_azure_container is not set");
    }

    auto bucket = bucket_name{container_name.value()};
    auto test_file = object_key{set_expiry_test_file};

    // try set expiry
    auto set_expiry_header = _requestor.make_set_expiry_to_blob_request(
      bucket, test_file, std::chrono::seconds{30});
    if (!set_expiry_header) {
        vlog(
          abs_log.error,
          "Failed to create set_expiry header: {}",
          set_expiry_header.error());
        throw std::system_error(set_expiry_header.error());
    }

    vlog(abs_log.trace, "send https request:\n{}", set_expiry_header.value());

    auto response_stream = co_await _client.request(
      std::move(set_expiry_header.value()), timeout);

    co_await response_stream->prefetch_headers();
    vassert(response_stream->is_header_done(), "Header is not received");
    const auto& headers = response_stream->get_headers();

    if (headers.result() == boost::beast::http::status::bad_request) {
        if (auto error_code_it = headers.find(error_code_name);
            error_code_it != headers.end()
            && error_code_it->value()
                 == hierarchical_namespace_not_enabled_error_code) {
            // if there is a match of error code, we can proceed, otherwise
            // fallthrough
            co_return storage_account_info{.is_hns_enabled = false};
        }
    }

    if (
      headers.result() == boost::beast::http::status::ok
      || headers.result() == boost::beast::http::status::not_found) {
        // not found counts as hsn_enabled, otherwise it would fail as
        // bad_request
        co_return storage_account_info{.is_hns_enabled = true};
    }

    // unexpected header return code
    throw parse_header_error_response(headers);
}

ss::future<abs_client::storage_account_info>
abs_client::do_get_account_info(ss::lowres_clock::duration timeout) {
    auto header = _requestor.make_get_account_info_request();
    if (!header) {
        vlog(
          abs_log.warn, "Failed to create request header: {}", header.error());
        throw std::system_error(header.error());
    }

    vlog(abs_log.trace, "send https request:\n{}", header.value());

    auto response_stream = co_await _client.request(
      std::move(header.value()), timeout);

    co_await response_stream->prefetch_headers();
    vassert(response_stream->is_header_done(), "Header is not received");
    const auto& headers = response_stream->get_headers();

    if (headers.result() != boost::beast::http::status::ok) {
        throw parse_header_error_response(headers);
    }

    if (auto iter = headers.find(is_hns_enabled_name); iter != headers.end()) {
        co_return storage_account_info{
          .is_hns_enabled = iter->value() == "true"};
    } else {
        vlog(
          abs_log.warn,
          "x-ms-is-hns-enabled field not found in headers of account info "
          "response: {}",
          headers);

        co_return storage_account_info{};
    }
}

ss::future<> abs_client::do_delete_file(
  const bucket_name& name,
  object_key path,
  ss::lowres_clock::duration timeout) {
    vassert(
      _adls_client && _data_lake_v2_client_config,
      "Attempt to use ADLSv2 endpoint without having created a client");

    auto header = _requestor.make_delete_file_request(
      _data_lake_v2_client_config->uri, name, path);
    if (!header) {
        vlog(
          abs_log.warn, "Failed to create request header: {}", header.error());
        throw std::system_error(header.error());
    }

    vlog(abs_log.trace, "send https request:\n{}", header.value());

    auto response_stream = co_await _adls_client->request(
      std::move(header.value()), timeout);

    co_await response_stream->prefetch_headers();
    vassert(response_stream->is_header_done(), "Header is not received");

    const auto status = response_stream->get_headers().result();
    if (
      status != boost::beast::http::status::accepted
      && status != boost::beast::http::status::ok) {
        const auto content_type = get_response_content_type(
          response_stream->get_headers());
        auto buf = co_await util::drain_response_stream(
          std::move(response_stream));
        throw parse_rest_error_response(content_type, status, std::move(buf));
    }
}

ss::future<result<abs_client::no_response, error_outcome>>
abs_client::delete_path(
  const bucket_name& name,
  object_key path,
  ss::lowres_clock::duration timeout) {
    return send_request(
      do_delete_path(name, path, timeout).then([]() {
          return ss::make_ready_future<no_response>(no_response{});
      }),
      path);
}

ss::future<> abs_client::do_delete_path(
  const bucket_name& name,
  object_key path,
  ss::lowres_clock::duration timeout) {
    std::vector<object_key> blobs_to_delete = util::all_paths_to_file(path);
    for (auto iter = blobs_to_delete.rbegin(); iter != blobs_to_delete.rend();
         ++iter) {
        try {
            co_await do_delete_file(name, *iter, timeout);
        } catch (const abs_rest_error_response& abs_error) {
            if (
              abs_error.code() == abs_error_code::path_not_found
              || abs_error.code() == abs_error_code::blob_not_found) {
                vlog(
                  abs_log.debug,
                  "Object to be deleted was not found in cloud storage: "
                  "object={}, bucket={}. Ignoring ...",
                  *iter,
                  name);
                continue;
            }

            if (abs_error.code() == abs_error_code::directory_not_empty) {
                vlog(
                  abs_log.debug,
                  "Attempt to delete non-empty directory {} in bucket {}. "
                  "Ignoring ...",
                  *iter,
                  name);
                co_return;
            }

            throw;
        }
    }
}

} // namespace cloud_storage_clients
