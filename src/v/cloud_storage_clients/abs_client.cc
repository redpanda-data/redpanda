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

#include "bytes/iostream.h"
#include "bytes/streambuf.h"
#include "cloud_storage_clients/abs_error.h"
#include "cloud_storage_clients/client.h"
#include "cloud_storage_clients/configuration.h"
#include "cloud_storage_clients/logger.h"
#include "cloud_storage_clients/types.h"
#include "cloud_storage_clients/util.h"
#include "cloud_storage_clients/xml_sax_parser.h"
#include "config/configuration.h"
#include "http/client.h"
#include "http/mime.h"
#include "http/multipart.h"
#include "json/document.h"
#include "json/istreamwrapper.h"
#include "utils/memory_data_source.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/simple-stream.hh>
#include <seastar/core/temporary_buffer.hh>

#include <boost/beast/core/error.hpp>
#include <boost/beast/core/string_type.hpp>
#include <boost/beast/http/chunk_encode.hpp>
#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/field.hpp>
#include <boost/beast/http/fields.hpp>
#include <boost/beast/http/message.hpp>
#include <boost/beast/http/parser.hpp>
#include <boost/beast/http/verb.hpp>
#include <boost/beast/http/write.hpp>

#include <cstddef>
#include <iterator>
#include <memory>
#include <sstream>
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

constexpr boost::beast::string_view batch_uri = "/?comp=batch";
constexpr boost::beast::string_view batch_sub_content_type = "application/html";
constexpr boost::beast::string_view batch_sub_transfer_encoding = "binary";

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
  bucket_name const& name,
  object_key const& key,
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
  bucket_name const& name, object_key const& key, size_t payload_size_bytes) {
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
  bucket_name const& name, object_key const& key) {
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
  bucket_name const& name, object_key const& key) {
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
  [[maybe_unused]] std::optional<object_key> start_after,
  std::optional<size_t> max_keys,
  std::optional<char> delimiter) {
    // GET /{container-id}?restype=container&comp=list&prefix={prefix}...
    // ...&max_results{max_keys}
    // HTTP/1.1 Host: {storage-account-id}.blob.core.windows.net
    // x-ms-date:{req-datetime in RFC9110} # added by 'add_auth'
    // x-ms-version:"2023-01-23"           # added by 'add_auth'
    // Authorization:{signature}           # added by 'add_auth'
    auto target = fmt::format("/{}?restype=container&comp=list", name());
    if (prefix) {
        target += fmt::format("&prefix={}", prefix.value()().string());
    }

    if (max_keys) {
        target += fmt::format("&max_results={}", max_keys.value());
    }

    if (delimiter) {
        target += fmt::format("&delimiter={}", delimiter.value());
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
abs_request_creator::make_delete_file_request(
  const access_point_uri& adls_ap,
  bucket_name const& name,
  object_key const& path) {
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

result<http::client::request_header> abs_request_creator::make_batch_request(
  const std::string& boundary, size_t content_length) {
    http::client::request_header header{};
    header.target(batch_uri);
    header.method(boost::beast::http::verb::post);
    const boost::beast::string_view host{_ap().data(), _ap().length()};
    header.insert(boost::beast::http::field::host, host);
    header.insert(
      boost::beast::http::field::content_length,
      fmt::format("{}", content_length));
    header.insert(
      boost::beast::http::field::content_type,
      http::format_media_type(
        {.type = "multipart/mixed", .params = {{"boundary", boundary}}}));

    auto error_code = _apply_credentials->add_auth(header);
    if (error_code) {
        return error_code;
    }

    return header;
}

result<http::client::request_header>
abs_request_creator::make_batch_delete_blob_subrequest(
  bucket_name const& name, object_key const& key) {
    // DELETE /{container-id}/{blob-id} HTTP/1.1
    // x-ms-date:{req-datetime in RFC9110} # added by 'add_auth'
    // x-ms-version:"2023-01-23"           # added by 'add_auth'
    // Authorization:{signature}           # added by 'add_auth'
    const auto target = fmt::format("/{}/{}", name(), key().string());

    http::client::request_header header{};
    header.method(boost::beast::http::verb::delete_);
    header.target(target);
    header.insert(delete_snapshot_name, delete_snapshot_value);
    header.insert(boost::beast::http::field::content_length, "0");

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
  bucket_name const& name,
  object_key const& key,
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
  bucket_name const& name,
  object_key const& key,
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
  bucket_name const& name,
  object_key const& key,
  size_t payload_size,
  ss::input_stream<char> body,
  ss::lowres_clock::duration timeout) {
    return send_request(
      do_put_object(name, key, payload_size, std::move(body), timeout)
        .then(
          []() { return ss::make_ready_future<no_response>(no_response{}); }),
      key,
      op_type_tag::upload);
}

ss::future<> abs_client::do_put_object(
  bucket_name const& name,
  object_key const& key,
  size_t payload_size,
  ss::input_stream<char> body,
  ss::lowres_clock::duration timeout) {
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
    if (status != boost::beast::http::status::created) {
        const auto content_type = get_response_content_type(
          response_stream->get_headers());
        auto buf = co_await util::drain_response_stream(
          std::move(response_stream));
        throw parse_rest_error_response(content_type, status, std::move(buf));
    }
}

ss::future<result<abs_client::head_object_result, error_outcome>>
abs_client::head_object(
  bucket_name const& name,
  object_key const& key,
  ss::lowres_clock::duration timeout) {
    return send_request(do_head_object(name, key, timeout), key);
}

ss::future<abs_client::head_object_result> abs_client::do_head_object(
  bucket_name const& name,
  object_key const& key,
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
              // ABS returns a 404 for attempts to delete a blob that doesn't
              // exist. The remote doesn't expect this, so we map 404s to a
              // successful response.
              if (!result && result.error() == error_outcome::key_not_found) {
                  vlog(
                    abs_log.debug,
                    "Object to be deleted was not found in cloud storage: "
                    "object={}, bucket={}. Ignoring ...",
                    name,
                    key);
                  return ss::make_ready_future<ret_t>(no_response{});
              } else {
                  return ss::make_ready_future<ret_t>(result);
              }
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
    if (!_adls_client) {
        return do_delete_objects(bucket, std::move(keys), timeout);
    } else {
        return do_delete_paths(bucket, std::move(keys), timeout);
    }

    const std::string boundary = http::random_multipart_boundary();
}

ss::future<result<abs_client::delete_objects_result, error_outcome>>
abs_client::do_delete_objects(
  const bucket_name& bucket,
  std::vector<object_key> keys,
  ss::lowres_clock::duration timeout) {
    const auto boundary = http::random_multipart_boundary();
    auto request_body = co_await make_delete_objects_payload(
      bucket, boundary, keys);

    auto header = _requestor.make_batch_request(
      boundary, request_body.size_bytes());
    if (!header) {
        vlog(
          abs_log.warn, "Failed to create request header: {}", header.error());
        throw std::system_error(header.error());
    }

    auto istream = make_iobuf_input_stream(std::move(request_body));
    auto response_stream = co_await _client.request(
      std::move(header.value()), istream, timeout);

    co_await response_stream->prefetch_headers();
    vassert(response_stream->is_header_done(), "Header is not received");

    const auto status = response_stream->get_headers().result();
    if (status != boost::beast::http::status::accepted) {
        iobuf buf = co_await util::drain_response_stream(response_stream);
        const auto content_type = get_response_content_type(
          response_stream->get_headers());
        throw parse_rest_error_response(content_type, status, std::move(buf));
    }

    auto response_istream = response_stream->as_input_stream();

    co_return co_await parse_delete_objects_payload(
      std::move(response_istream), boundary, keys);
}

ss::future<iobuf> abs_client::make_delete_objects_payload(
  const bucket_name& bucket,
  std::string boundary,
  const std::vector<object_key>& keys) {
    iobuf buf;
    ss::output_stream<char> out = make_iobuf_ref_output_stream(buf);
    auto w = http::multipart_writer(out, std::move(boundary));

    size_t key_id = 0;
    for (const auto& key : keys) {
        http::multipart_fields part_header;
        part_header.insert(
          boost::beast::http::field::content_type, batch_sub_content_type);
        part_header.insert(
          boost::beast::http::field::content_transfer_encoding,
          batch_sub_transfer_encoding);
        part_header.insert(
          boost::beast::http::field::content_id, fmt::format("{}", ++key_id));

        auto key_header = _requestor.make_batch_delete_blob_subrequest(
          bucket, key);
        if (!key_header) {
            vlog(
              abs_log.warn,
              "Failed to create request header: {}",
              key_header.error());
            throw std::system_error(key_header.error());
        }

        iobuf part_body = http::request_header_to_iobuf(key_header.value());
        co_await w.write_part(std::move(part_header), std::move(part_body));
    }
    co_await w.close();

    co_return buf;
}

ss::future<result<abs_client::delete_objects_result, error_outcome>>
abs_client::parse_delete_objects_payload(
  ss::input_stream<char> response_body,
  std::string_view boundary,
  const std::vector<object_key>& keys) {
    auto mp = http::multipart_reader(response_body, boundary);

    abs_client::delete_objects_result delete_objects_result;

    for (;;) {
        auto maybe_part = co_await mp.next();
        if (!maybe_part.has_value()) {
            break;
        }

        // Convert part body to buffers compatible with boost::beast.
        static constexpr std::string_view last_crlf = "\r\n";
        auto part_parser = boost::beast::http::response_parser<
          boost::beast::http::empty_body>();
        std::vector<boost::asio::const_buffer> seq;
        for (auto const& fragm : maybe_part->body) {
            seq.emplace_back(fragm.get(), fragm.size());
        }
        seq.emplace_back(last_crlf.data(), last_crlf.size());

        boost::beast::error_code ec;
        part_parser.put(seq, ec);
        if (ec.failed()) {
            throw boost::system::system_error(ec);
        }

        part_parser.put_eof(ec);
        if (ec.failed()) {
            throw boost::system::system_error(ec);
        }

        const auto headers = part_parser.get();
        if (headers.result() == boost::beast::http::status::accepted) {
            // Ok.
        } else if (headers.result() == boost::beast::http::status::not_found) {
            // Ok.
        } else {
            delete_objects_result.undeleted_keys.emplace_back(
              keys.at(std::stoi(
                maybe_part->header.at(boost::beast::http::field::content_id))),
              "ERROR: TODO(nv) parse it");
        }
    }

    co_return delete_objects_result;
}

ss::future<result<abs_client::delete_objects_result, error_outcome>>
abs_client::do_delete_paths(
  const bucket_name& bucket,
  std::vector<object_key> keys,
  ss::lowres_clock::duration timeout) {
    abs_client::delete_objects_result delete_objects_result;
    for (const auto& key : keys) {
        try {
            auto res = co_await delete_path(bucket, key, timeout);
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
  std::optional<object_key> start_after,
  std::optional<size_t> max_keys,
  std::optional<ss::sstring> continuation_token,
  ss::lowres_clock::duration timeout,
  std::optional<char> delimiter,
  std::optional<item_filter> collect_item_if) {
    return send_request(
      do_list_objects(
        name,
        std::move(prefix),
        std::move(start_after),
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
  std::optional<object_key> start_after,
  std::optional<size_t> max_keys,
  [[maybe_unused]] std::optional<ss::sstring> continuation_token,
  ss::lowres_clock::duration timeout,
  std::optional<char> delimiter,
  std::optional<item_filter> gather_item_if) {
    auto header = _requestor.make_list_blobs_request(
      name,
      _adls_client.has_value(),
      std::move(prefix),
      std::move(start_after),
      max_keys,
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
    return send_request(do_get_account_info(timeout), object_key{""});
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
            if (abs_error.code() == abs_error_code::path_not_found) {
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
