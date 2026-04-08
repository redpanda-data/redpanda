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
#include "cloud_roles/types.h"
#include "cloud_storage_clients/abs_error.h"
#include "cloud_storage_clients/configuration.h"
#include "cloud_storage_clients/logger.h"
#include "cloud_storage_clients/multipart_upload.h"
#include "cloud_storage_clients/types.h"
#include "cloud_storage_clients/upstream.h"
#include "cloud_storage_clients/util.h"
#include "cloud_storage_clients/xml_sax_parser.h"
#include "config/configuration.h"
#include "container/chunked_hash_map.h"
#include "http/utils.h"
#include "json/document.h"
#include "json/istreamwrapper.h"
#include "utils/base64.h"
#include "utils/uuid.h"

#include <charconv>
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
constexpr boost::beast::string_view expiry_option_name = "x-ms-expiry-option";
constexpr boost::beast::string_view expiry_option_value = "RelativeToNow";
constexpr boost::beast::string_view expiry_time_name = "x-ms-expiry-time";

constexpr boost::beast::string_view content_type_multipart_val
  = "multipart/mixed";

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

net::base_transport::configuration make_adls_transport_configuration(
  const cloud_storage_clients::abs_configuration& conf,
  net::base_transport::configuration transport_conf) {
    constexpr uint16_t default_port = 443;

    const auto endpoint_uri = [&]() -> ss::sstring {
        auto adls_endpoint_override
          = config::shard_local_cfg().cloud_storage_azure_adls_endpoint.value();
        if (adls_endpoint_override.has_value()) {
            return adls_endpoint_override.value();
        }
        return ssx::sformat(
          "{}.dfs.core.windows.net", conf.storage_account_name());
    }();

    transport_conf.tls_sni_hostname = endpoint_uri;
    // conf.uri = access_point_uri{endpoint_uri};

    auto adls_port_override
      = config::shard_local_cfg().cloud_storage_azure_adls_port();
    transport_conf.server_addr = net::unresolved_address{
      endpoint_uri,
      adls_port_override.has_value() ? *adls_port_override : default_port};

    return transport_conf;
}

} // namespace

namespace cloud_storage_clients {

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

        throw std::runtime_error(
          ssx::sformat(
            "Failed to parse JSON ABS error response: {}",
            doc.GetParseError()));
    }

    std::optional<ss::sstring> code;
    std::optional<ss::sstring> member;
    if (auto error_it = doc.FindMember("error"); error_it != doc.MemberEnd()) {
        const auto& error = error_it->value;
        if (
          auto code_it = error.FindMember("code");
          code_it != error.MemberEnd()) {
            code = code_it->value.GetString();
        }

        if (
          auto member_it = error.FindMember("member");
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

/// Parse multipart/mixed batch delete response
/// The response format is:
/// --boundary
/// Content-Type: application/http
/// Content-ID: 0
///
/// HTTP/1.1 202 Accepted
/// x-ms-request-id: ...
/// ...
///
/// --boundary
/// ... (more responses)
/// --boundary--
static cloud_storage_clients::client::delete_objects_result
parse_batch_delete_response(
  iobuf buf,
  std::string_view boundary,
  const chunked_vector<object_key>& keys) {
    cloud_storage_clients::client::delete_objects_result result;

    // Simple multipart parser - split by boundary
    auto boundary_delim = ssx::sformat("--{}", boundary);
    util::multipart_response_parser parts{std::move(buf), boundary_delim};

    constexpr auto convert_content_id =
      [](std::string_view raw) -> std::optional<size_t> {
        size_t v{};
        auto res = std::from_chars(raw.data(), raw.data() + raw.size(), v);
        return res.ec == std::errc{} ? std::make_optional(v) : std::nullopt;
    };

    chunked_hash_set<size_t> content_ids_seen;
    content_ids_seen.reserve(keys.size());
    std::optional<iobuf> part;
    while ((part = parts.get_part()).has_value()) {
        iobuf_parser part_parser{std::move(part).value()};
        auto mime = util::mime_header::from(part_parser);
        auto maybe_content_id = mime.content_id<size_t>(convert_content_id);
        if (!maybe_content_id.has_value()) {
            vlog(
              abs_log.debug,
              "batch_delete_response: MIME header missing 'Content-ID' from "
              "batch response, skipping part");
            continue;
        }
        content_ids_seen.insert(maybe_content_id.value());
        // having stripped off the leading MIME headers, we should have a
        // complete HTTP response at the front of the parser
        auto subrequest = util::multipart_subresponse::from(part_parser);

        if (maybe_content_id.value() >= keys.size()) {
            vlog(
              abs_log.warn,
              "batch_delete_response: Content-ID in response part {} out of "
              "range, expected [{},{}): Error message: '{}'",
              maybe_content_id.value(),
              0,
              keys.size(),
              subrequest.error(error_code_name));
            continue;
        }

        if (
          auto maybe_error_message = subrequest.error(error_code_name);
          maybe_error_message.has_value()) {
            result.undeleted_keys.push_back({
              .key = keys[maybe_content_id.value()],
              .reason = std::move(maybe_error_message).value(),
            });
        }
    }

    for (auto id : std::views::iota(0ul, keys.size())) {
        if (!content_ids_seen.contains(id)) {
            result.undeleted_keys.push_back({
              .key = keys[id],
              .reason = "Object missing from batch response",
            });
        }
    }

    return result;
}

abs_request_creator::abs_request_creator(
  const abs_configuration& conf,
  ss::lw_shared_ptr<const cloud_roles::apply_credentials> apply_credentials)
  : _ap{conf.uri}
  , _apply_credentials{std::move(apply_credentials)} {}

result<http::client::request_header> abs_request_creator::make_get_blob_request(
  const plain_bucket_name& name,
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
    auto error_code = add_auth(header);
    if (error_code) {
        return error_code;
    }
    util::url_encode_target(header);
    return header;
}

result<http::client::request_header> abs_request_creator::make_put_blob_request(
  const plain_bucket_name& name,
  const object_key& key,
  size_t payload_size_bytes) {
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

    auto error_code = add_auth(header);
    if (error_code) {
        return error_code;
    }
    util::url_encode_target(header);
    return header;
}

result<http::client::request_header>
abs_request_creator::make_get_blob_metadata_request(
  const plain_bucket_name& name, const object_key& key) {
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

    auto error_code = add_auth(header);
    if (error_code) {
        return error_code;
    }
    util::url_encode_target(header);
    return header;
}

result<http::client::request_header>
abs_request_creator::make_delete_blob_request(
  const plain_bucket_name& name, const object_key& key) {
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

    auto error_code = add_auth(header);
    if (error_code) {
        return error_code;
    }
    util::url_encode_target(header);
    return header;
}

result<std::pair<http::client::request_header, ss::input_stream<char>>>
abs_request_creator::make_batch_delete_request(
  const plain_bucket_name& name, const chunked_vector<object_key>& keys) {
    // Azure Blob Storage Batch API
    // https://learn.microsoft.com/en-us/rest/api/storageservices/blob-batch
    //
    // POST /?comp=batch HTTP/1.1
    // Host: {storage-account-id}.blob.core.windows.net
    // Content-Type: multipart/mixed; boundary=batch_<unique-id>
    // Content-Length: <...>
    // x-ms-date: {req-datetime in RFC9110} # added by 'add_auth'
    // x-ms-version: 2023-01-23             # added by 'add_auth'
    // Authorization:{signature}            # added by 'add_auth'
    //
    // Body structure:
    // --batch_<unique-id>
    // Content-ID: 0
    //
    // DELETE /{container-id}/{blob-id} HTTP/1.1
    // Content-Type: application/http
    // Content-Transfer-Encoding: binary
    // x-ms-delete-snapshots: include
    // x-ms-date: {req-datetime in RFC9110}
    // Authorization:{signature}
    //
    // --batch_<unique-id>
    // ... (repeat for each blob)
    // --batch_<unique-id>--

    // Generate unique boundary using uuid
    auto boundary = fmt::format("batch_{}", uuid_t::create());

    // Build the multipart body using iobuf and stream
    iobuf body;
    iobuf_ostreambuf obuf(body);
    std::ostream out(&obuf);

    for (size_t i = 0; i < keys.size(); ++i) {
        const auto& key = keys[i];

        // Boundary line
        fmt::print(out, "--{}\r\n", boundary);

        http::client::request_header part_header{};
        part_header.insert(
          boost::beast::http::field::content_type, "application/http");
        part_header.insert(
          boost::beast::http::field::content_transfer_encoding, "binary");
        part_header.insert(
          boost::beast::http::field::content_id, fmt::to_string(i));

        // Create individual delete request for this blob

        // Create a temporary header to get auth headers for this subrequest
        // NOTE: Per
        // https://learn.microsoft.com/en-us/rest/api/storageservices/blob-batch?tabs=microsoft-entra-id#request-body
        // The subrequests should
        // - Not have the x-ms-version header.
        // - Include the path of the URL
        // - Omit the host
        // - Each subrequest is authorized separately, with the provided
        //   auth headers in the subrequest.
        http::client::request_header subrequest_header{};
        subrequest_header.method(boost::beast::http::verb::delete_);
        subrequest_header.target(fmt::format("/{}/{}", name(), key().string()));
        subrequest_header.insert(delete_snapshot_name, delete_snapshot_value);
        util::url_encode_target(subrequest_header);
        if (
          auto ec = add_auth(subrequest_header, true /* omit_version */); ec) {
            return ec;
        }

        // Content-Length for DELETE is always 0
        subrequest_header.insert(
          boost::beast::http::field::content_length, fmt::to_string(0));

        for (const auto& f : part_header) {
            fmt::print(out, "{}: {}\r\n", f.name_string(), f.value());
        }
        fmt::print(out, "\r\n");

        fmt::print(
          out,
          "{} {} HTTP/1.1\r\n",
          subrequest_header.method_string(),
          subrequest_header.target());

        for (const auto& field : subrequest_header) {
            fmt::print(out, "{}: {}\r\n", field.name_string(), field.value());
        }

        fmt::print(out, "\r\n");
    }

    // Final boundary
    fmt::print(out, "--{}--\r\n", boundary);

    if (!out.good()) {
        throw std::runtime_error(
          fmt::format(
            "failed to create batch delete request, state: {}", out.rdstate()));
    }

    // Main request header
    http::client::request_header header{};
    header.method(boost::beast::http::verb::post);
    header.target("/?comp=batch");
    header.insert(
      boost::beast::http::field::host,
      boost::beast::string_view{_ap().data(), _ap().length()});
    header.insert(
      boost::beast::http::field::content_type,
      fmt::format("{}; boundary={}", content_type_multipart_val, boundary));
    header.insert(
      boost::beast::http::field::content_length,
      std::to_string(body.size_bytes()));
    if (auto ec = add_auth(header); ec) {
        return ec;
    }
    util::url_encode_target(header);

    auto stream = make_iobuf_input_stream(std::move(body));
    return std::make_tuple(std::move(header), std::move(stream));
}

result<http::client::request_header>
abs_request_creator::make_list_blobs_request(
  const plain_bucket_name& name,
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
        target += fmt::format(
          "&prefix={}",
          http::uri_encode(
            prefix.value()().string(), http::uri_encode_slash::yes));
    }

    if (max_results) {
        target += fmt::format("&maxresults={}", max_results.value());
    }

    if (delimiter) {
        target += fmt::format(
          "&delimiter={}",
          http::uri_encode(
            std::string_view{&*delimiter, 1}, http::uri_encode_slash::yes));
    }

    if (marker.has_value()) {
        target += fmt::format(
          "&marker={}",
          http::uri_encode(marker.value(), http::uri_encode_slash::yes));
    }

    if (files_only) {
        target += fmt::format("&showonly=files");
    }

    const boost::beast::string_view host{_ap().data(), _ap().length()};

    http::client::request_header header{};
    header.method(boost::beast::http::verb::get);
    header.target(target);
    header.insert(boost::beast::http::field::host, host);

    auto error_code = add_auth(header);
    if (error_code) {
        return error_code;
    }
    util::url_encode_target(header);
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

    auto error_code = add_auth(header);
    if (error_code) {
        return error_code;
    }
    util::url_encode_target(header);

    return header;
}

result<http::client::request_header>
abs_request_creator::make_set_expiry_to_blob_request(
  const plain_bucket_name& name,
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
    if (auto error_code = add_auth(header); error_code != std::error_code{}) {
        return error_code;
    }
    util::url_encode_target(header);
    return header;
}

result<http::client::request_header>
abs_request_creator::make_delete_file_request(
  const access_point_uri& adls_ap,
  const plain_bucket_name& name,
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

    auto error_code = add_auth(header);
    if (error_code) {
        return error_code;
    }
    util::url_encode_target(header);
    return header;
}

result<http::client::request_header>
abs_request_creator::make_put_block_request(
  const plain_bucket_name& name,
  const object_key& key,
  const ss::sstring& block_id,
  size_t payload_size_bytes) {
    // PUT /{container-id}/{blob-id}?comp=block&blockid={BASE64_ID} HTTP/1.1
    // Host: {storage-account-id}.blob.core.windows.net
    // x-ms-date:{req-datetime in RFC9110} # added by 'add_auth'
    // x-ms-version:"2023-01-23"           # added by 'add_auth'
    // Authorization:{signature}           # added by 'add_auth'
    // Content-Length:{payload-size}
    const auto target = fmt::format(
      "/{}/{}?comp=block&blockid={}", name(), key().string(), block_id);
    const boost::beast::string_view host{_ap().data(), _ap().length()};

    http::client::request_header header{};
    header.method(boost::beast::http::verb::put);
    header.target(target);
    header.insert(boost::beast::http::field::host, host);
    header.insert(
      boost::beast::http::field::content_length,
      std::to_string(payload_size_bytes));

    auto error_code = add_auth(header);
    if (error_code) {
        return error_code;
    }
    util::url_encode_target(header);
    return header;
}

result<std::pair<http::client::request_header, ss::input_stream<char>>>
abs_request_creator::make_put_block_list_request(
  const plain_bucket_name& name,
  const object_key& key,
  const std::vector<ss::sstring>& block_ids) {
    // PUT /{container-id}/{blob-id}?comp=blocklist HTTP/1.1
    // Host: {storage-account-id}.blob.core.windows.net
    // x-ms-date:{req-datetime in RFC9110} # added by 'add_auth'
    // x-ms-version:"2023-01-23"           # added by 'add_auth'
    // Authorization:{signature}           # added by 'add_auth'
    // Content-Length:{xml-body-size}
    // Content-Type: application/xml

    // Generate XML body
    auto xml_body = iobuf::from(R"xml(<?xml version="1.0" encoding="utf-8"?>
<BlockList>
)xml");
    for (const auto& block_id : block_ids) {
        xml_body.append_str(fmt::format("<Latest>{}</Latest>\n", block_id));
    }
    xml_body.append_str("</BlockList>\n");

    const auto target = fmt::format(
      "/{}/{}?comp=blocklist", name(), key().string());
    const boost::beast::string_view host{_ap().data(), _ap().length()};

    http::client::request_header header{};
    header.method(boost::beast::http::verb::put);
    header.target(target);
    header.insert(boost::beast::http::field::host, host);
    header.insert(boost::beast::http::field::content_type, "application/xml");
    header.insert(
      boost::beast::http::field::content_length,
      std::to_string(xml_body.size_bytes()));

    auto error_code = add_auth(header);
    if (error_code) {
        return error_code;
    }
    util::url_encode_target(header);

    // Convert XML body to input stream
    auto body_stream = make_iobuf_input_stream(std::move(xml_body));
    return std::make_pair(std::move(header), std::move(body_stream));
}

std::error_code abs_request_creator::add_auth(
  http::client::request_header& header, bool omit_version) const {
    if (!omit_version) {
        header.set("x-ms-version", cloud_roles::azure_storage_api_version);
    }
    return _apply_credentials->add_auth(header);
}

// Helper function to generate Base64-encoded block IDs for ABS multipart upload
// Block IDs must all be the same pre-encoded length, so we use 10-digit
// zero-padded format
static ss::sstring generate_block_id(size_t part_number) {
    auto id = fmt::format("{:010d}", part_number);
    // Convert to bytes_view for Base64 encoding
    bytes_view bv{reinterpret_cast<const uint8_t*>(id.data()), id.size()};
    return bytes_to_base64(bv);
}

// abs_multipart_state implementation

abs_multipart_state::abs_multipart_state(
  abs_client* client,
  plain_bucket_name container,
  object_key key,
  ss::lowres_clock::duration timeout)
  : _client(client)
  , _container(std::move(container))
  , _key(std::move(key))
  , _timeout(timeout) {}

ss::future<> abs_multipart_state::initialize_multipart() {
    // ABS Block Blobs don't require initialization - blocks can be uploaded
    // directly
    vlog(abs_log.debug, "ABS multipart upload initialized (no-op)");
    _initialized = true;
    _client->_probe->register_multipart_create();
    co_return;
}

ss::future<> abs_multipart_state::upload_part(size_t part_num, iobuf data) {
    // Generate Base64-encoded block ID
    auto block_id = generate_block_id(part_num);

    vlog(
      abs_log.debug,
      "Uploading ABS block {} (block_id: {}, size: {})",
      part_num,
      block_id,
      data.size_bytes());

    // Create Put Block request
    auto header = _client->_requestor.make_put_block_request(
      _container, _key, block_id, data.size_bytes());
    if (!header) {
        vlog(
          abs_log.error,
          "Failed to create Put Block request: {}",
          header.error());
        throw std::system_error(header.error());
    }

    // Upload the block
    auto body = make_iobuf_input_stream(std::move(data));
    auto response_stream = co_await _client->_client
                             .request(std::move(header.value()), body, _timeout)
                             .finally([&body] { return body.close(); });

    co_await response_stream->prefetch_headers();
    vassert(response_stream->is_header_done(), "Header is not received");

    const auto status = response_stream->get_headers().result();
    if (status != boost::beast::http::status::created) {
        const auto content_type = util::get_response_content_type(
          response_stream->get_headers());
        auto buf = co_await http::drain(std::move(response_stream));
        throw parse_rest_error_response(content_type, status, std::move(buf));
    }

    co_await http::drain(std::move(response_stream));

    _client->_probe->register_multipart_upload();

    _block_ids.push_back(block_id);
}

ss::future<> abs_multipart_state::complete_multipart_upload() {
    vlog(
      abs_log.debug,
      "Completing ABS multipart upload ({} blocks)",
      _block_ids.size());

    // Create Put Block List request
    auto put_block_list_req = _client->_requestor.make_put_block_list_request(
      _container, _key, _block_ids);
    if (!put_block_list_req) {
        throw std::system_error(put_block_list_req.error());
    }
    auto [header, body] = std::move(put_block_list_req.value());

    // Commit the blocks
    auto response_stream = co_await _client->_client
                             .request(std::move(header), body, _timeout)
                             .finally([&body] { return body.close(); });

    co_await response_stream->prefetch_headers();
    vassert(response_stream->is_header_done(), "Header is not received");

    const auto status = response_stream->get_headers().result();
    if (status != boost::beast::http::status::created) {
        const auto content_type = util::get_response_content_type(
          response_stream->get_headers());
        auto buf = co_await http::drain(std::move(response_stream));
        throw parse_rest_error_response(content_type, status, std::move(buf));
    }

    co_await http::drain(std::move(response_stream));

    _client->_probe->register_multipart_complete();
}

ss::future<> abs_multipart_state::abort_multipart_upload() {
    // ABS uncommitted blocks expire after 7 days - no explicit abort needed
    vlog(abs_log.debug, "ABS multipart upload aborted (no-op)");
    _client->_probe->register_multipart_abort();
    co_return;
}

ss::future<> abs_multipart_state::upload_as_single_object(iobuf data) {
    auto size = data.size_bytes();
    vlog(
      abs_log.debug,
      "ABS small file optimization: using Put Blob (size: {})",
      size);

    // Use the regular put_object method for small files
    auto body = make_iobuf_input_stream(std::move(data));
    co_await _client->do_put_object(
      _container, _key, size, std::move(body), _timeout);
}

abs_client::abs_client(
  ss::weak_ptr<upstream> upstream_ptr,
  const abs_configuration& conf,
  const net::base_transport::configuration& transport_conf,
  ss::shared_ptr<client_probe> probe,
  ss::lw_shared_ptr<const cloud_roles::apply_credentials> apply_credentials)
  : client(std::move(upstream_ptr))
  , _data_lake_v2_client_config(
      conf.is_hns_enabled
        ? std::make_optional(
            make_adls_transport_configuration(conf, transport_conf))
        : std::nullopt)
  , _is_oauth(apply_credentials->is_oauth())
  , _requestor(conf, std::move(apply_credentials))
  , _client(transport_conf, nullptr, probe)
  , _adls_client(
      conf.is_hns_enabled ? std::make_optional(*_data_lake_v2_client_config)
                          : std::nullopt)
  , _probe(std::move(probe)) {
    vlog(abs_log.trace, "Created client with config:{}", conf);
}

abs_client::abs_client(
  ss::weak_ptr<upstream> upstream_ptr,
  const abs_configuration& conf,
  const net::base_transport::configuration& transport_conf,
  ss::shared_ptr<client_probe> probe,
  const ss::abort_source& as,
  ss::lw_shared_ptr<const cloud_roles::apply_credentials> apply_credentials)
  : client(std::move(upstream_ptr))
  , _data_lake_v2_client_config(
      conf.is_hns_enabled
        ? std::make_optional(
            make_adls_transport_configuration(conf, transport_conf))
        : std::nullopt)
  , _is_oauth(apply_credentials->is_oauth())
  , _requestor(conf, std::move(apply_credentials))
  , _client(transport_conf, &as, probe, conf.max_idle_time)
  , _adls_client(
      conf.is_hns_enabled ? std::make_optional(*_data_lake_v2_client_config)
                          : std::nullopt)
  , _probe(std::move(probe)) {
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

void abs_client::shutdown() { _client.shutdown_now(); }

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
              "Received [{}] {} error response from ABS. This could indicate "
              "either misconfiguration (e.g. unauthorized IP address) or "
              "invalidation of the SAS token: {}.",
              err.http_code(),
              err.code_string(),
              err.message());
            if (err.code() == abs_error_code::authentication_failed) {
                // Unfortunately, we can only get a common REST API error code
                // here:
                // https://learn.microsoft.com/en-us/rest/api/storageservices/common-rest-api-error-codes
                // According to this page:
                // https://learn.microsoft.com/en-us/troubleshoot/azure/azure-storage/blobs/authentication/storage-troubleshoot-403-errors
                // the expired token will trigger generic AuthenticationFailed
                // error.
                outcome = error_outcome::authentication_failed;
                if (auto p = _upstream_ptr.get()) {
                    p->maybe_refresh_credentials();
                }
            } else {
                outcome = error_outcome::fail;
            }
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
  const plain_bucket_name& name,
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
  const plain_bucket_name& name,
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

        const auto content_type = util::get_response_content_type(
          response_stream->get_headers());
        auto buf = co_await http::drain(std::move(response_stream));
        throw parse_rest_error_response(content_type, status, std::move(buf));
    }

    co_return response_stream;
}

ss::future<result<abs_client::no_response, error_outcome>>
abs_client::put_object(
  const plain_bucket_name& name,
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
  const plain_bucket_name& name,
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

    if (
      const auto is_no_content_and_accepted = accept_no_content
                                              && status == no_content;
      status != created && !is_no_content_and_accepted) {
        const auto content_type = util::get_response_content_type(
          response_stream->get_headers());
        auto buf = co_await http::drain(std::move(response_stream));
        throw parse_rest_error_response(content_type, status, std::move(buf));
    }
}

ss::future<result<abs_client::head_object_result, error_outcome>>
abs_client::head_object(
  const plain_bucket_name& name,
  const object_key& key,
  ss::lowres_clock::duration timeout) {
    return send_request(do_head_object(name, key, timeout), key);
}

ss::future<abs_client::head_object_result> abs_client::do_head_object(
  const plain_bucket_name& name,
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
  const plain_bucket_name& name,
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
  const plain_bucket_name& name,
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
        const auto content_type = util::get_response_content_type(
          response_stream->get_headers());
        auto buf = co_await http::drain(std::move(response_stream));
        throw parse_rest_error_response(content_type, status, std::move(buf));
    }
}

ss::future<abs_client::delete_objects_result>
abs_client::do_batch_delete_objects(
  const plain_bucket_name& bucket,
  const chunked_vector<object_key>& keys,
  ss::lowres_clock::duration timeout) {
    auto request = _requestor.make_batch_delete_request(bucket, keys);
    if (!request) {
        throw std::system_error(request.error());
    }
    auto& [header, body] = request.value();
    vlog(abs_log.trace, "send batch delete request:\n{}", header);

    std::exception_ptr ex;
    std::optional<delete_objects_result> result;
    try {
        auto response_stream = co_await _client.request(
          std::move(header), body, timeout);

        co_await response_stream->prefetch_headers();
        vassert(response_stream->is_header_done(), "Header is not received");

        const auto status = response_stream->get_headers().result();
        if (status != boost::beast::http::status::accepted) {
            // If the top level request fails, we expect a regular old XML or
            // JSON REST error response, like any other endpoint.
            const auto content_type = util::get_response_content_type(
              response_stream->get_headers());
            auto buf = co_await http::drain(std::move(response_stream));
            throw parse_rest_error_response(
              content_type, status, std::move(buf));
        }

        const auto& headers = response_stream->get_headers();
        auto boundary = util::find_multipart_boundary(headers);
        auto response_buf = co_await http::drain(std::move(response_stream));
        if (!boundary.has_value()) {
            throw std::runtime_error(boundary.error());
        }
        if (abs_log.is_enabled(ss::log_level::trace)) {
            // Log up to 1024 bytes of the response to aid debugging
            // without dumping the entire (potentially large) multipart
            // body.
            auto preview_bytes
              = iobuf_const_parser(response_buf)
                  .peek_bytes(
                    std::min(response_buf.size_bytes(), size_t{1024}));
            vlog(
              abs_log.trace,
              "batch delete response: boundary='{}', body_size={}, "
              "first_bytes='{}'",
              boundary.value(),
              response_buf.size_bytes(),
              std::string_view(
                reinterpret_cast<const char*>(preview_bytes.data()),
                preview_bytes.size()));
        }

        result = parse_batch_delete_response(
          std::move(response_buf), boundary.value(), keys);
    } catch (...) {
        ex = std::current_exception();
    }

    co_await body.close();

    if (ex) {
        std::rethrow_exception(ex);
    }
    co_return std::move(result).value();
}

ss::future<result<abs_client::delete_objects_result, error_outcome>>
abs_client::delete_objects(
  const plain_bucket_name& bucket,
  const chunked_vector<object_key>& keys,
  ss::lowres_clock::duration timeout) {
    const object_key dummy{""};
    co_return co_await send_request(
      do_batch_delete_objects(bucket, keys, timeout), dummy);
}

bool abs_client::is_valid() const noexcept {
    // If the upstream is gone (evicted) credentials may be stale so we consider
    // the client no longer valid. maybe_refresh_credentials() would be a
    // no-op.
    return _upstream_ptr.get() != nullptr;
}

fmt::iterator abs_client::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "ABSClient{{{}}}", _client.server_address());
}

ss::future<result<abs_client::list_bucket_result, error_outcome>>
abs_client::list_objects(
  const plain_bucket_name& name,
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
  const plain_bucket_name& name,
  std::optional<object_key> prefix,
  std::optional<size_t> max_results,
  std::optional<ss::sstring> marker,
  ss::lowres_clock::duration timeout,
  std::optional<char> delimiter,
  std::optional<item_filter> collect_item_if) {
    // Don't use files_only (showonly=files) when a delimiter is specified.
    // The showonly=files parameter excludes BlobPrefix entries from the
    // response, but BlobPrefix entries are exactly what we want when using
    // a delimiter to discover virtual directories.
    const bool files_only = _adls_client.has_value() && !delimiter.has_value();
    auto header = _requestor.make_list_blobs_request(
      name,
      files_only,
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
        const auto content_type = util::get_response_content_type(
          response_stream->get_headers());
        iobuf buf = co_await http::drain(response_stream);
        throw parse_rest_error_response(content_type, status, std::move(buf));
    }

    co_return co_await parse_from_stream<abs_parse_impl>(
      response_stream->as_input_stream(), std::move(collect_item_if));
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
    // TODO: Review this code. It is likely buggy when Remote Read Replicas are
    // used. We are testing HNS on default storage account, but in RRR setup, we
    // actually use a different storage account for reads.
    // A similar issue exists in S3 client.

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

    auto bucket = plain_bucket_name{container_name.value()};
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
        if (
          auto error_code_it = headers.find(error_code_name);
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
  const plain_bucket_name& name,
  object_key path,
  ss::lowres_clock::duration timeout) {
    vassert(
      _adls_client && _data_lake_v2_client_config,
      "Attempt to use ADLSv2 endpoint without having created a client");

    auto header = _requestor.make_delete_file_request(
      access_point_uri{_data_lake_v2_client_config->server_addr.host()},
      name,
      path);
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
        const auto content_type = util::get_response_content_type(
          response_stream->get_headers());
        auto buf = co_await http::drain(std::move(response_stream));
        throw parse_rest_error_response(content_type, status, std::move(buf));
    }
}

ss::future<result<abs_client::no_response, error_outcome>>
abs_client::delete_path(
  const plain_bucket_name& name,
  object_key path,
  ss::lowres_clock::duration timeout) {
    return send_request(
      do_delete_path(name, path, timeout).then([]() {
          return ss::make_ready_future<no_response>(no_response{});
      }),
      path);
}

ss::future<> abs_client::do_delete_path(
  const plain_bucket_name& name,
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

ss::future<result<ss::shared_ptr<multipart_upload_state>, error_outcome>>
abs_client::initiate_multipart_upload(
  const plain_bucket_name& bucket,
  const object_key& key,
  size_t part_size,
  ss::lowres_clock::duration timeout) {
    // Validate part size
    constexpr size_t max_abs_block_size = multipart_limits::max_abs_block_size;
    if (part_size > max_abs_block_size) {
        vlog(
          abs_log.error,
          "ABS block_size {} exceeds maximum {}",
          part_size,
          max_abs_block_size);
        co_return error_outcome::fail;
    }

    vlog(
      abs_log.debug,
      "Initiating ABS multipart upload: container={}, key={}, part_size={}",
      bucket,
      key,
      part_size);

    // Create and return ABS multipart state
    // Caller will wrap this in a multipart_upload
    auto state = ss::make_shared<abs_multipart_state>(
      this, bucket, key, timeout);
    co_return state;
}

} // namespace cloud_storage_clients
