/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage_clients/s3_client.h"

#include "base/units.h"
#include "base/vlog.h"
#include "bytes/bytes.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "bytes/iostream.h"
#include "bytes/streambuf.h"
#include "cloud_storage_clients/logger.h"
#include "cloud_storage_clients/multipart_upload.h"
#include "cloud_storage_clients/s3_error.h"
#include "cloud_storage_clients/upstream.h"
#include "cloud_storage_clients/util.h"
#include "cloud_storage_clients/xml_sax_parser.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "config/types.h"
#include "container/chunked_hash_map.h"
#include "hashing/secure.h"
#include "http/client.h"
#include "http/utils.h"
#include "json/istreamwrapper.h"
#include "json/reader.h"
#include "utils/base64.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/util/log.hh>

#include <boost/beast/core/error.hpp>
#include <boost/beast/http/field.hpp>
#include <boost/beast/http/status.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>

#include <charconv>
#include <exception>
#include <utility>
#include <variant>

namespace cloud_storage_clients {

struct aws_header_names {
    static constexpr boost::beast::string_view prefix = "prefix";
    static constexpr boost::beast::string_view start_after = "start-after";
    static constexpr boost::beast::string_view max_keys = "max-keys";
    static constexpr boost::beast::string_view continuation_token
      = "continuation-token";
    static constexpr boost::beast::string_view x_amz_tagging = "x-amz-tagging";
    static constexpr boost::beast::string_view x_amz_request_id
      = "x-amz-request-id";
    // https://cloud.google.com/storage/docs/xml-api/reference-headers#xguploaderuploadid
    static constexpr boost::beast::string_view x_guploader_uploadid
      = "x-guploader-uploadid";
    static constexpr boost::beast::string_view delimiter = "delimiter";
};

struct aws_header_values {
    static constexpr boost::beast::string_view user_agent
      = "redpanda.vectorized.io";
    static constexpr boost::beast::string_view text_plain = "text/plain";
};

// request_creator //

request_creator::request_creator(
  const s3_configuration& conf,
  ss::lw_shared_ptr<const cloud_roles::apply_credentials> apply_credentials)
  : _ap(conf.uri)
  , _ap_style(conf.url_style)
  , _apply_credentials{std::move(apply_credentials)} {}

result<http::client::request_header> request_creator::make_get_object_request(
  const plain_bucket_name& name,
  const object_key& key,
  std::optional<http_byte_range> byte_range) {
    http::client::request_header header{};
    // Virtual Style:
    // GET /{object-id} HTTP/1.1
    // Host: {bucket-name}.s3.{region}.amazonaws.com
    // Path Style:
    // GET /{bucket-name}/{object-id} HTTP/1.1
    // Host: s3.{region}.amazonaws.com
    //
    // x-amz-date:{req-datetime}
    // Authorization:{signature}
    // x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
    auto host = make_host(name);
    auto target = make_target(name, key);
    header.method(boost::beast::http::verb::get);
    header.target(target);
    header.insert(
      boost::beast::http::field::user_agent, aws_header_values::user_agent);
    header.insert(boost::beast::http::field::host, host);
    header.insert(boost::beast::http::field::content_length, "0");
    if (byte_range.has_value()) {
        header.insert(
          boost::beast::http::field::range,
          fmt::format(
            "bytes={}-{}",
            byte_range.value().first,
            byte_range.value().second));
    }
    auto ec = _apply_credentials->add_auth(header);
    if (ec) {
        return ec;
    }
    util::url_encode_target(header);
    return header;
}

result<http::client::request_header> request_creator::make_head_object_request(
  const plain_bucket_name& name, const object_key& key) {
    http::client::request_header header{};
    // Virtual Style:
    // HEAD /{object-id} HTTP/1.1
    // Host: {bucket-name}.s3.{region}.amazonaws.com
    // Path Style:
    // HEAD /{bucket-name}/{object-id} HTTP/1.1
    // Host: s3.{region}.amazonaws.com
    //
    // x-amz-date:{req-datetime}
    // Authorization:{signature}
    // x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
    auto host = make_host(name);
    auto target = make_target(name, key);
    header.method(boost::beast::http::verb::head);
    header.target(target);
    header.insert(
      boost::beast::http::field::user_agent, aws_header_values::user_agent);
    header.insert(boost::beast::http::field::host, host);
    header.insert(boost::beast::http::field::content_length, "0");
    auto ec = _apply_credentials->add_auth(header);
    if (ec) {
        return ec;
    }
    util::url_encode_target(header);
    return header;
}

result<http::client::request_header>
request_creator::make_unsigned_put_object_request(
  const plain_bucket_name& name,
  const object_key& key,
  size_t payload_size_bytes) {
    // Virtual Style:
    // PUT /my-image.jpg HTTP/1.1
    // Host: {bucket-name}.s3.{region}.amazonaws.com
    // Path Style:
    // PUT /{bucket-name}/{object-id} HTTP/1.1
    // Host: s3.{region}.amazonaws.com
    //
    // Date: Wed, 12 Oct 2009 17:50:00 GMT
    // Authorization: authorization string
    // Content-Type: text/plain
    // Content-Length: 11434
    // x-amz-meta-author: Janet
    // Expect: 100-continue
    // [11434 bytes of object data]
    http::client::request_header header{};
    auto host = make_host(name);
    auto target = make_target(name, key);
    header.method(boost::beast::http::verb::put);
    header.target(target);
    header.insert(
      boost::beast::http::field::user_agent, aws_header_values::user_agent);
    header.insert(boost::beast::http::field::host, host);
    header.insert(
      boost::beast::http::field::content_type, aws_header_values::text_plain);
    header.insert(
      boost::beast::http::field::content_length,
      std::to_string(payload_size_bytes));

    auto ec = _apply_credentials->add_auth(header);
    if (ec) {
        return ec;
    }
    util::url_encode_target(header);
    return header;
}

result<http::client::request_header>
request_creator::make_list_objects_v2_request(
  const plain_bucket_name& name,
  std::optional<object_key> prefix,
  std::optional<object_key> start_after,
  std::optional<size_t> max_keys,
  std::optional<ss::sstring> continuation_token,
  std::optional<char> delimiter) {
    // Virtual Style:
    // GET /?list-type=2&prefix=photos/2006/&delimiter=/ HTTP/1.1
    // Host: {bucket-name}.s3.{region}.amazonaws.com
    // Path Style:
    // GET /{bucket-name}/?list-type=2&prefix=photos/2006/&delimiter=/ HTTP/1.1
    // Host: s3.{region}.amazonaws.com
    //
    // x-amz-date: 20160501T000433Z
    // Authorization: authorization string
    http::client::request_header header{};
    auto host = make_host(name);
    auto key = fmt::format("?list-type=2");
    if (prefix.has_value()) {
        key = fmt::format(
          "{}&prefix={}",
          key,
          http::uri_encode((*prefix)().string(), http::uri_encode_slash::yes));
    }
    if (start_after.has_value()) {
        key = fmt::format(
          "{}&start-after={}",
          key,
          http::uri_encode(
            (*start_after)().string(), http::uri_encode_slash::yes));
    }
    if (max_keys.has_value()) {
        key = fmt::format("{}&max-keys={}", key, *max_keys);
    }
    if (continuation_token.has_value()) {
        key = fmt::format(
          "{}&continuation-token={}",
          key,
          http::uri_encode(
            std::string_view(*continuation_token),
            http::uri_encode_slash::yes));
    }
    if (delimiter.has_value()) {
        key = fmt::format(
          "{}&delimiter={}",
          key,
          http::uri_encode(
            std::string_view{&*delimiter, 1}, http::uri_encode_slash::yes));
    }
    auto target = make_target(name, object_key{key});
    header.method(boost::beast::http::verb::get);
    header.target(target);
    header.insert(
      boost::beast::http::field::user_agent, aws_header_values::user_agent);
    header.insert(boost::beast::http::field::host, host);
    header.insert(boost::beast::http::field::content_length, "0");

    auto ec = _apply_credentials->add_auth(header);
    vlog(s3_log.trace, "ListObjectsV2:\n {}", header);
    if (ec) {
        return ec;
    }
    util::url_encode_target(header);
    return header;
}

result<http::client::request_header>
request_creator::make_delete_object_request(
  const plain_bucket_name& name, const object_key& key) {
    http::client::request_header header{};
    // Virtual Style:
    // DELETE /{object-id} HTTP/1.1
    // Host: {bucket-name}.s3.amazonaws.com
    // Path Style:
    // DELETE /{bucket-name}/{object-id} HTTP/1.1
    // Host: s3.{region}.amazonaws.com
    //
    // x-amz-date:{req-datetime}
    // Authorization:{signature}
    // x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
    //
    // NOTE: x-amz-mfa, x-amz-bypass-governance-retention are not used for now
    auto host = make_host(name);
    auto target = make_target(name, key);
    header.method(boost::beast::http::verb::delete_);
    header.target(target);
    header.insert(
      boost::beast::http::field::user_agent, aws_header_values::user_agent);
    header.insert(boost::beast::http::field::host, host);
    header.insert(boost::beast::http::field::content_length, "0");
    auto ec = _apply_credentials->add_auth(header);
    if (ec) {
        return ec;
    }
    util::url_encode_target(header);
    return header;
}

result<std::tuple<http::client::request_header, ss::input_stream<char>>>
request_creator::make_delete_objects_request(
  const plain_bucket_name& name, const chunked_vector<object_key>& keys) {
    // https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
    // will generate this request:
    //
    // Virtual Style:
    // POST /?delete HTTP/1.1
    // Host: {bucket-name}.s3.{region}.amazonaws.com
    // Path Style:
    // POST /{bucket-name}/?delete HTTP/1.1
    // Host: s3.{region}.amazonaws.com
    //
    // Content-MD5: <Computer from body>
    // Authorization: <applied by _requestor>
    // Content-Length: <...>
    //
    // <?xml version="1.0" encoding="UTF-8"?>
    // <Delete>
    //     <Object>
    //         <Key>object_key</Key>
    //     </Object>
    //     <Object>
    //         <Key>object_key</Key>
    //     </Object>
    //      ...
    //     <Quiet>true</Quiet>
    // </Delete>
    //
    // note:
    //  - Delete.Quiet true will generate a response that reports only failures
    //  to delete or errors
    //  - the actual xml might not be pretty-printed
    //  - with clang15 and ranges, xml generation could be a one-liner + a
    //  custom formatter for xml escaping

    auto body = [&] {
        auto delete_tree = boost::property_tree::ptree{};
        // request a quiet response
        delete_tree.put("Delete.Quiet", true);
        // add an array of Object.Key=key to the Delete root
        for (auto key_tree = boost::property_tree::ptree{};
             const auto& k : keys) {
            key_tree.put("Key", k().c_str());
            delete_tree.add_child("Delete.Object", key_tree);
        }

        iobuf buf;
        iobuf_ostreambuf obuf(buf);
        std::ostream out(&obuf);
        boost::property_tree::write_xml(out, delete_tree);
        if (!out.good()) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "failed to create delete request, state: {}",
              out.rdstate()));
        }
        return buf;
    }();

    auto body_md5 = [&] {
        // compute md5 and produce a base64 encoded signature for body
        auto hash = hash_md5{};
        hash.update(body);
        auto bin_digest = hash.reset();
        return bytes_to_base64(to_bytes_view(bin_digest));
    }();

    http::client::request_header header{};
    header.method(boost::beast::http::verb::post);
    auto host = make_host(name);
    auto target = make_target(name, object_key{"?delete"});
    header.target(target);
    header.insert(boost::beast::http::field::host, host);
    // from experiments, minio is sloppy in checking this field. It will check
    // that it's valid base64, but seems not to actually check the value
    header.insert(
      boost::beast::http::field::content_md5,
      {body_md5.data(), body_md5.size()});

    header.insert(
      boost::beast::http::field::content_length,
      fmt::format("{}", body.size_bytes()));

    auto ec = _apply_credentials->add_auth(header);
    if (ec) {
        return ec;
    }
    util::url_encode_target(header);
    return {std::move(header), make_iobuf_input_stream(std::move(body))};
}

result<std::tuple<http::client::request_header, ss::input_stream<char>>>
request_creator::make_gcs_batch_delete_request(
  const plain_bucket_name& name, const chunked_vector<object_key>& keys) {
    // Google Cloud Storage Batch API
    // https://cloud.google.com/storage/docs/batch
    //
    // POST /batch/storage/v1 HTTP/1.1
    // Host: storage.googleapis.com
    // Content-Type: multipart/mixed; boundary=<boundary>
    // Authorization: Bearer <token>  # added by 'add_auth'
    // Content-Length: <...>
    //
    // Body structure:
    // --<boundary>
    // Content-Type: application/http
    // Content-ID: <id>
    //
    // DELETE /storage/v1/b/<bucket>/o/<object> HTTP/1.1
    //
    // --<boundary>
    // ... (repeat for each object)
    // --<boundary>--
    //
    // Note: GCS batch API requires path-only URLs in subrequests
    // Max 100 requests per batch, max 10MB payload

    // Generate unique boundary
    auto boundary = fmt::format("batch_{}", uuid_t::create());

    // Build the multipart body
    iobuf body;
    iobuf_ostreambuf obuf(body);
    std::ostream out(&obuf);

    auto encoded_bucket = http::uri_encode(name(), http::uri_encode_slash::yes);

    for (size_t i = 0; i < keys.size(); ++i) {
        const auto& key = keys[i];

        auto encoded_key = http::uri_encode(
          key().string(), http::uri_encode_slash::yes);

        // Boundary line
        fmt::print(out, "--{}\r\n", boundary);

        http::client::request_header part_header{};
        part_header.insert(
          boost::beast::http::field::content_type, "application/http");
        part_header.insert(
          boost::beast::http::field::content_transfer_encoding, "binary");
        part_header.insert(
          boost::beast::http::field::content_id, fmt::to_string(i));

        http::client::request_header subrequest_header{};
        subrequest_header.method(boost::beast::http::verb::delete_);
        subrequest_header.target(
          fmt::format("/storage/v1/b/{}/o/{}", encoded_bucket, encoded_key));

        // NOTE: Per docs.cloud.google.com/storage/docs/batch#http:
        // if you provide an [Auth] header for a specific nested request, then
        // that header applies only to the request that specified it. If you
        // provide an [Auth] header for the outer request, then that header
        // applies to all of the nested requests unless they override it with an
        // [Auth] header of their own.

        // part header

        for (const auto& f : part_header) {
            fmt::print(out, "{}: {}\r\n", f.name_string(), f.value());
        }
        fmt::print(out, "\r\n");

        // subrequest header

        fmt::print(
          out,
          "{} {} HTTP/1.1\r\n",
          subrequest_header.method_string(),
          subrequest_header.target());

        for (const auto& f : subrequest_header) {
            fmt::print(out, "{}: {}\r\n", f.name_string(), f.value());
        }
        fmt::print(out, "\r\n");
    }

    // Final boundary marker
    fmt::print(out, "--{}--\r\n", boundary);

    if (!out.good()) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "failed to create GCS batch delete request, state: {}",
          out.rdstate()));
    }

    // Create the main request header
    http::client::request_header header{};
    header.method(boost::beast::http::verb::post);
    // GCS batch endpoint uses a fixed path, not bucket-specific
    header.target("/batch/storage/v1");
    header.insert(boost::beast::http::field::host, "storage.googleapis.com");
    header.insert(
      boost::beast::http::field::content_type,
      fmt::format("multipart/mixed; boundary={}", boundary));
    header.insert(
      boost::beast::http::field::content_length,
      fmt::format("{}", body.size_bytes()));

    auto ec = _apply_credentials->add_auth(header);
    if (ec) {
        return ec;
    }

    // Convert iobuf to input_stream
    auto stream = make_iobuf_input_stream(std::move(body));

    return {std::move(header), std::move(stream)};
}

std::string request_creator::make_host(const plain_bucket_name& name) const {
    switch (_ap_style.value()) {
    case s3_url_style::virtual_host:
        // Host: bucket-name.s3.region-code.amazonaws.com
        return fmt::format("{}.{}", name(), _ap());
    case s3_url_style::path:
        // Host: s3.region-code.amazonaws.com
        return fmt::format("{}", _ap());
    }
}

std::string request_creator::make_target(
  const plain_bucket_name& name, const object_key& key) const {
    switch (_ap_style.value()) {
    case s3_url_style::virtual_host:
        // Target: /homepage.html
        return fmt::format("/{}", key().string());
    case s3_url_style::path:
        // Target: /example.com/homepage.html
        return fmt::format("/{}/{}", name(), key().string());
    }
}

// client //

namespace {
inline cloud_storage_clients::s3_error_code
status_to_error_code(boost::beast::http::status s) {
    // According to this https://repost.aws/knowledge-center/http-5xx-errors-s3
    // the 500 and 503 errors are used in case of throttling
    if (
      s == boost::beast::http::status::service_unavailable
      || s == boost::beast::http::status::internal_server_error) {
        return cloud_storage_clients::s3_error_code::slow_down;
    }
    return cloud_storage_clients::s3_error_code::_unknown;
}

rest_error_response parse_xml_rest_error_response(iobuf&& buf) {
    try {
        auto resp = util::iobuf_to_ptree(std::move(buf), s3_log);
        constexpr const char* empty = "";
        auto code = resp.get<ss::sstring>("Error.Code", empty);
        auto msg = resp.get<ss::sstring>("Error.Message", empty);
        auto rid = resp.get<ss::sstring>("Error.RequestId", empty);
        auto res = resp.get<ss::sstring>("Error.Resource", empty);
        rest_error_response err(code, msg, rid, res);
        return err;
    } catch (...) {
        vlog(s3_log.error, "!!error parse error {}", std::current_exception());
        throw;
    }
}

rest_error_response parse_unknown_format_error_response(
  boost::beast::http::status status, iobuf buf) {
    static constexpr auto max_body_size = static_cast<size_t>(4_KiB);
    static constexpr auto truncation_warning_segment = " [truncated~4096]";

    auto maybe_truncation_warning = "";
    auto read_size = buf.size_bytes();

    // if the error message exceeds reasonable size (4k), truncate it and
    // indicate a truncation to the recipient
    if (read_size > max_body_size) {
        maybe_truncation_warning = truncation_warning_segment;
        read_size = max_body_size;
    }

    ss::sstring error_body = "";
    iobuf_parser parser{std::move(buf)};

    // this is the unhappy unhappy path, handle potentially invalid utf8
    try {
        error_body = parser.read_string_safe(read_size);
    } catch (const invalid_utf8_exception& e) {
        vlog(s3_log.info, "response body contains invalid utf8 {}", e);
    } catch (...) {
        vlog(s3_log.error, "error parse error {}", std::current_exception());
        throw;
    }

    return rest_error_response{
      fmt::format("{}", status_to_error_code(status)),
      fmt::format(
        "http status: {}, error body{}: {}",
        status,
        maybe_truncation_warning,
        error_body),
      "",
      ""};
}

template<typename ResultT = void>
ss::future<ResultT> parse_rest_error_response(
  response_content_type type, boost::beast::http::status result, iobuf buf) {
    // AWS errors occasionally come with an empty body
    // (See https://github.com/redpanda-data/redpanda/issues/6061)
    // Without a proper code, we treat it as a hint to gracefully retry
    // (synthesize the slow_down code).
    if (!buf.empty()) {
        if (type == response_content_type::xml) {
            // Error responses from S3 _should_ have the Content-Type header set
            // with `application/xml`- however, certain responses (such as `503
            // Service Unavailable`) may not be of this form.
            // https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
            return ss::make_exception_future<ResultT>(
              parse_xml_rest_error_response(std::move(buf)));
        } else {
            // render out up to 4k of plaintext or json errors
            return ss::make_exception_future<ResultT>(
              parse_unknown_format_error_response(result, std::move(buf)));
        }
    }

    return ss::make_exception_future<ResultT>(rest_error_response(
      fmt::format("{}", status_to_error_code(result)),
      fmt::format("Empty error response, status code {}", result),
      "",
      ""));
}

namespace {
struct gcs_error_handler
  : public rapidjson::BaseReaderHandler<rapidjson::UTF8<>, gcs_error_handler> {
    enum class state : uint8_t { init, in_error, in_message, found };
    state st = state::init;
    std::optional<ss::sstring> message;
    bool Key(const char* str, rapidjson::SizeType len, bool) {
        std::string_view k{str, len};
        if (st == state::init && k == "error") {
            st = state::in_error;
        } else if (st == state::in_error && k == "message") {
            st = state::in_message;
        }
        return true;
    }

    bool String(const char* str, rapidjson::SizeType len, bool) {
        if (st == state::in_message) {
            message.emplace(str, len);
            st = state::found;
        }
        return true;
    }

    bool EndObject(rapidjson::SizeType) {
        if (st == state::in_error) {
            st = state::init;
        }
        return true;
    }
};

ss::sstring parse_gcs_error_reason(iobuf body) {
    iobuf_istreambuf ibuf(body);
    std::istream stream(&ibuf);
    json::IStreamWrapper wrapper(stream);
    json::Reader reader;
    gcs_error_handler handler;
    auto res = reader.Parse(wrapper, handler);
    if (res && handler.message.has_value()) {
        return std::move(handler.message).value();
    } else {
        return "Unknown";
    }
};
} // namespace

/// Parse GCS batch delete response using multipart parsing utilities
/// GCS batch responses follow the multipart/mixed format similar to ABS
static cloud_storage_clients::client::delete_objects_result
parse_gcs_batch_delete_response(
  iobuf buf,
  std::string_view boundary,
  const chunked_vector<object_key>& keys) {
    cloud_storage_clients::client::delete_objects_result result;

    // Parse multipart response - split by boundary
    auto boundary_delim = ssx::sformat("--{}", boundary);
    util::multipart_response_parser parts{std::move(buf), boundary_delim};

    constexpr auto convert_content_id =
      [](std::string_view raw) -> std::optional<size_t> {
        constexpr std::string_view pfx = "response-";
        std::optional<size_t> result{};
        if (auto pos = raw.find(pfx); pos != raw.npos) {
            raw = raw.substr(pos + pfx.size());
            size_t v{};
            auto res = std::from_chars(raw.data(), raw.data() + raw.size(), v);
            // reject if from_chars found any junk on the end of the raw ID
            if (res.ec == std::errc{} && res.ptr == raw.data() + raw.size()) {
                result = v;
            }
        }
        return result;
    };

    chunked_hash_set<size_t> content_ids_seen;
    std::optional<iobuf> part;
    while ((part = parts.get_part()).has_value()) {
        iobuf_parser part_parser{std::move(part).value()};
        auto mime = util::mime_header::from(part_parser);
        auto maybe_content_id = mime.content_id<size_t>(convert_content_id);
        if (!maybe_content_id.has_value()) {
            vlog(
              s3_log.debug,
              "MIME header missing 'Content-ID' from batch response, skipping "
              "part");
            continue;
        }
        content_ids_seen.insert(maybe_content_id.value());
        // having stripped off the leading MIME headers, we should have a
        // complete HTTP response at the front of the parser
        auto subrequest = util::multipart_subresponse::from(part_parser);

        if (maybe_content_id.value() >= keys.size()) {
            vlog(
              s3_log.warn,
              "batch_delete_response: Content-ID in response part {} out of "
              "range, expected [{},{}): Error message: '{}'",
              maybe_content_id.value(),
              0,
              keys.size(),
              subrequest.error(parse_gcs_error_reason));
            continue;
        }

        vlog(
          s3_log.trace,
          "batch_delete_response: Processing Content-ID {} (key: {})",
          maybe_content_id,
          keys[maybe_content_id.value()]);

        if (
          auto maybe_error_message = subrequest.error(parse_gcs_error_reason);
          maybe_error_message.has_value()) {
            // Extract error message from response if available
            // GCS error responses may contain error details in the body
            result.undeleted_keys.push_back({
              .key = keys[maybe_content_id.value()],
              .reason = std::move(maybe_error_message).value(),
            });
        }
    }

    // Check for any keys that were not in the response
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

/// Head response doesn't give us an XML encoded error object in
/// the body. This method uses headers to generate an error object.
template<class ResultT = void>
ss::future<ResultT> parse_head_error_response(
  const http::http_response::header_type& hdr, const object_key& key) {
    try {
        ss::sstring code;
        ss::sstring msg;
        if (hdr.result() == boost::beast::http::status::not_found) {
            code = "NoSuchKey";
            msg = "Not found";
        } else {
            code = fmt::format("{}", status_to_error_code(hdr.result()));
            msg = ss::sstring(hdr.reason().data(), hdr.reason().size());
        }
        boost::string_view rid;
        if (hdr.find(aws_header_names::x_amz_request_id) != hdr.end()) {
            rid = hdr.at(aws_header_names::x_amz_request_id);
        } else if (
          hdr.find(aws_header_names::x_guploader_uploadid) != hdr.end()) {
            rid = hdr.at(aws_header_names::x_guploader_uploadid);
        }
        rest_error_response err(
          code, msg, ss::sstring(rid.data(), rid.size()), key().native());
        return ss::make_exception_future<ResultT>(err);
    } catch (...) {
        vlog(
          s3_log.error,
          "!!error parse error {}, header: {}",
          std::current_exception(),
          hdr);
        throw;
    }
}
} // namespace

template<typename T>
ss::future<result<T, error_outcome>> s3_client::send_request(
  ss::future<T> request_future,
  const plain_bucket_name& bucket,
  const object_key& key) {
    auto outcome = error_outcome::retry;

    try {
        co_return co_await std::move(request_future);
    } catch (const rest_error_response& err) {
        if (
          err.code() == s3_error_code::no_such_key
          || err.code() == s3_error_code::no_such_configuration) {
            // Unexpected 404s are logged by 'request_future' at warn
            // level, so only log at debug level here.
            vlog(s3_log.debug, "NoSuchKey response received {}", key);
            outcome = error_outcome::key_not_found;
        } else if (err.code() == s3_error_code::no_such_bucket) {
            vlog(
              s3_log.error,
              "The specified S3 bucket, {}, could not be found. Ensure that "
              "your bucket exists and that the cloud_storage_bucket and "
              "cloud_storage_region cluster configs are correct.",
              bucket());

            outcome = error_outcome::fail;
        } else if (
          err.code() == s3_error_code::slow_down
          || err.code() == s3_error_code::internal_error
          || err.code() == s3_error_code::request_timeout) {
            // This can happen when we're dealing with high request rate to
            // the manifest's prefix. Backoff algorithm should be applied.
            // In principle only slow_down should occur, but in practice
            // AWS S3 does return internal_error as well sometimes.
            vlog(
              s3_log.warn,
              "{} response received {} in {}",
              err.code(),
              key,
              bucket);
            outcome = error_outcome::retry;
        } else if (
          err.code() == s3_error_code::expired_token
          || err.code() == s3_error_code::authentication_required) {
            // Unexpected REST API error, we can't recover from this
            vlog(
              s3_log.error,
              "{} auth error response received {} in {}",
              err.code(),
              key,
              bucket);
            outcome = error_outcome::authentication_failed;
            if (auto p = _upstream_ptr.get()) {
                p->maybe_refresh_credentials();
            }
        } else {
            // Unexpected REST API error, we can't recover from this
            // because the issue is not temporary (e.g. bucket doesn't
            // exist)
            vlog(
              s3_log.error,
              "Accessing {} in {}, unexpected REST API error \"{}\" detected, "
              "code: "
              "{}, request_id: {}, resource: {}",
              key,
              bucket,
              err.message(),
              err.code_string(),
              err.request_id(),
              err.resource());
            outcome = error_outcome::fail;
        }
    } catch (...) {
        outcome = util::handle_client_transport_error(
          std::current_exception(), s3_log);
    }

    co_return outcome;
}

s3_client::s3_client(
  ss::weak_ptr<upstream> upstream_ptr,
  const s3_configuration& conf,
  const net::base_transport::configuration& transport_conf,
  ss::shared_ptr<client_probe> probe,
  ss::lw_shared_ptr<const cloud_roles::apply_credentials> apply_credentials)
  : client(std::move(upstream_ptr))
  , _requestor(conf, std::move(apply_credentials))
  , _client(transport_conf, nullptr, probe)
  , _probe(std::move(probe)) {}

s3_client::s3_client(
  ss::weak_ptr<upstream> upstream_ptr,
  const s3_configuration& conf,
  const net::base_transport::configuration& transport_conf,
  ss::shared_ptr<client_probe> probe,
  const ss::abort_source& as,
  ss::lw_shared_ptr<const cloud_roles::apply_credentials> apply_credentials)
  : client(std::move(upstream_ptr))
  , _requestor(conf, std::move(apply_credentials))
  , _client(transport_conf, &as, probe, conf.max_idle_time)
  , _probe(std::move(probe)) {}

ss::future<result<client_self_configuration_output, error_outcome>>
s3_client::self_configure() {
    // Oracle cloud storage only supports path-style requests
    // (https://www.oracle.com/ca-en/cloud/storage/object-storage/faq/#category-amazon),
    // but self-configuration will misconfigure to virtual-host style due to a
    // ListObjects request that happens to succeed. Override for this
    // specific case.
    //
    // Similarily, MinIO supports `virtual_host` only iff the property
    // MINIO_DOMAIN is set, see:
    // (https://min.io/docs/minio/linux/reference/minio-server/settings/core.html#envvar.MINIO_DOMAIN).
    // If it is not, API calls with `virtual_host` will fail, though the
    // ListObjects request used for self configuration still manages to succeed,
    // leading to a similarily bad state as Oracle. Override for this case as
    // well.
    auto backend = config::shard_local_cfg().cloud_storage_backend();
    if (
      backend == model::cloud_storage_backend::oracle_s3_compat
      || backend == model::cloud_storage_backend::minio) {
        co_return s3_self_configuration_result{.url_style = s3_url_style::path};
    }

    // Also handle possibly inferred backend.
    auto inferred_backend = infer_backend_from_uri(_requestor._ap);
    if (
      inferred_backend == model::cloud_storage_backend::oracle_s3_compat
      || inferred_backend == model::cloud_storage_backend::minio) {
        co_return s3_self_configuration_result{.url_style = s3_url_style::path};
    }

    // Test virtual host style addressing, fall back to path if necessary.
    // If any configuration options prevent testing, addressing style will
    // default to virtual_host.
    // If both addressing methods fail, return an error.
    const auto& bucket_config = config::shard_local_cfg().cloud_storage_bucket;

    if (!bucket_config.value().has_value()) {
        vlog(
          s3_log.warn,
          "Could not self-configure S3 Client, {} is not set. Defaulting to "
          "virtual_host",
          bucket_config.name());
        co_return s3_self_configuration_result{
          .url_style = s3_url_style::virtual_host};
    }

    // TODO: Review this code. It is likely buggy when Remote Read Replicas are
    // used. We are testing HNS on default storage account, but in RRR setup, we
    // actually use a different storage account for reads.
    // A similar issue exists in ABS client.
    const auto bucket = cloud_storage_clients::plain_bucket_name{
      bucket_config.value().value()};

    // Test virtual_host style.
    vassert(
      !_requestor._ap_style.has_value()
        || _requestor._ap_style == s3_url_style::virtual_host,
      "_ap_style should be unset or virtual host before self configuration "
      "begins");
    _requestor._ap_style = s3_url_style::virtual_host;
    if (co_await self_configure_test(bucket)) {
        // Virtual host style request succeeded.
        co_return s3_self_configuration_result{
          .url_style = s3_url_style::virtual_host};
    }

    // fips mode can only work in virtual_host mode, so if the above test failed
    // the TS service is likely misconfigured
    vassert(
      !config::fips_mode_enabled(config::node().fips_mode.value()),
      "fips_mode requires the bucket to configured in virtual_host mode, but "
      "the connectivity test failed");

    // Test path style.
    _requestor._ap_style = s3_url_style::path;
    if (co_await self_configure_test(bucket)) {
        // Path style request succeeded.
        co_return s3_self_configuration_result{.url_style = s3_url_style::path};
    }

    // Both addressing styles failed.
    vlog(
      s3_log.error,
      "Couldn't reach S3 storage with either path style or virtual_host style "
      "requests.",
      bucket_config.name());
    co_return error_outcome::fail;
}

ss::future<bool>
s3_client::self_configure_test(const plain_bucket_name& bucket) {
    // Check that the current addressing-style works by issuing a ListObjects
    // request.
    auto list_objects_result = co_await list_objects(
      bucket, std::nullopt, std::nullopt, 1);
    co_return list_objects_result;
}

ss::future<> s3_client::stop() { return _client.stop(); }

void s3_client::shutdown() { _client.shutdown_now(); }

ss::future<result<http::client::response_stream_ref, error_outcome>>
s3_client::get_object(
  const plain_bucket_name& name,
  const object_key& key,
  ss::lowres_clock::duration timeout,
  bool expect_no_such_key,
  std::optional<http_byte_range> byte_range) {
    return send_request(
      do_get_object(
        name, key, timeout, expect_no_such_key, std::move(byte_range)),
      name,
      key);
}

ss::future<http::client::response_stream_ref> s3_client::do_get_object(
  const plain_bucket_name& name,
  const object_key& key,
  ss::lowres_clock::duration timeout,
  bool expect_no_such_key,
  std::optional<http_byte_range> byte_range) {
    bool is_byte_range_requested = byte_range.has_value();
    auto header = _requestor.make_get_object_request(
      name, key, std::move(byte_range));
    if (!header) {
        return ss::make_exception_future<http::client::response_stream_ref>(
          std::system_error(header.error()));
    }
    vlog(s3_log.trace, "send https request:\n{}", header.value());
    return _client.request(std::move(header.value()), timeout)
      .then([expect_no_such_key, is_byte_range_requested, key](
              http::client::response_stream_ref&& ref) {
          // here we didn't receive any bytes from the socket and
          // ref->is_header_done() is 'false', we need to prefetch
          // the header first
          // clang-tidy 16.0.4 is reporting an erroneous 'use-after-move' error
          // when calling `then` after `prefetch_headers`.
          auto f = ref->prefetch_headers();
          return f.then([ref = std::move(ref),
                         expect_no_such_key,
                         is_byte_range_requested,
                         key]() mutable {
              vassert(ref->is_header_done(), "Header is not received");
              const auto result = ref->get_headers().result();
              bool request_failed = result != boost::beast::http::status::ok;
              if (is_byte_range_requested) {
                  request_failed
                    &= result != boost::beast::http::status::partial_content;
              }
              if (request_failed) {
                  // Got error response, consume the response body and produce
                  // rest api error
                  if (
                    expect_no_such_key
                    && result == boost::beast::http::status::not_found) {
                      vlog(
                        s3_log.debug,
                        "S3 GET request with expected error for key {}: {} "
                        "{:l}",
                        key,
                        ref->get_headers().result(),
                        ref->get_headers());
                  } else {
                      vlog(
                        s3_log.warn,
                        "S3 GET request failed for key {}: {} {:l}",
                        key,
                        ref->get_headers().result(),
                        ref->get_headers());
                  }
                  const auto content_type = util::get_response_content_type(
                    ref->get_headers());
                  return http::drain(std::move(ref))
                    .then([content_type, result](iobuf&& res) {
                        return parse_rest_error_response<
                          http::client::response_stream_ref>(
                          content_type, result, std::move(res));
                    });
              }
              return ss::make_ready_future<http::client::response_stream_ref>(
                std::move(ref));
          });
      });
}

ss::future<result<s3_client::head_object_result, error_outcome>>
s3_client::head_object(
  const plain_bucket_name& name,
  const object_key& key,
  ss::lowres_clock::duration timeout) {
    return send_request(do_head_object(name, key, timeout), name, key);
}

ss::future<s3_client::head_object_result> s3_client::do_head_object(
  const plain_bucket_name& name,
  const object_key& key,
  ss::lowres_clock::duration timeout) {
    auto header = _requestor.make_head_object_request(name, key);
    if (!header) {
        return ss::make_exception_future<s3_client::head_object_result>(
          std::system_error(header.error()));
    }
    vlog(s3_log.trace, "send https request:\n{}", header.value());
    return _client.request(std::move(header.value()), timeout)
      .then(
        [key](const http::client::response_stream_ref& ref)
          -> ss::future<head_object_result> {
            return http::drain<void>(ref).then(
              [ref, key]() -> ss::future<head_object_result> {
                  auto status = ref->get_headers().result();
                  if (status == boost::beast::http::status::not_found) {
                      vlog(
                        s3_log.debug,
                        "Object {} not available, error: {:l}",
                        key,
                        ref->get_headers());
                      return parse_head_error_response<head_object_result>(
                        ref->get_headers(), key);
                  } else if (status != boost::beast::http::status::ok) {
                      vlog(
                        s3_log.warn,
                        "S3 HEAD request failed for key {}: {} {:l}",
                        key,
                        status,
                        ref->get_headers());
                      return parse_head_error_response<head_object_result>(
                        ref->get_headers(), key);
                  }
                  auto len = boost::lexical_cast<uint64_t>(
                    ref->get_headers().at(
                      boost::beast::http::field::content_length));
                  auto etag = ref->get_headers().at(
                    boost::beast::http::field::etag);
                  head_object_result results{
                    .object_size = len,
                    .etag = ss::sstring(etag.data(), etag.length()),
                  };
                  return ss::make_ready_future<head_object_result>(
                    std::move(results));
              });
        })
      .handle_exception_type([this](const rest_error_response& err) {
          _probe->register_failure(err.code(), op_type_tag::download);
          return ss::make_exception_future<head_object_result>(err);
      });
}

ss::future<result<s3_client::no_response, error_outcome>> s3_client::put_object(
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
      name,
      key);
}

ss::future<> s3_client::do_put_object(
  const plain_bucket_name& name,
  const object_key& id,
  size_t payload_size,
  ss::input_stream<char> body,
  ss::lowres_clock::duration timeout,
  bool accept_no_content) {
    auto header = _requestor.make_unsigned_put_object_request(
      name, id, payload_size);
    if (!header) {
        return body.close().then([header] {
            return ss::make_exception_future<>(
              std::system_error(header.error()));
        });
    }
    vlog(s3_log.trace, "send https request:\n{}", header.value());
    return ss::do_with(
      std::move(body),
      [this, timeout, header = std::move(header), id, accept_no_content](
        ss::input_stream<char>& body) mutable {
          auto make_request = [this, &header, &body, &timeout]() {
              return _client.request(std::move(header.value()), body, timeout);
          };

          return ss::futurize_invoke(make_request)
            .then([id, accept_no_content](
                    const http::client::response_stream_ref& ref) {
                return http::drain(ref).then([ref, id, accept_no_content](
                                               iobuf&& res) {
                    auto status = ref->get_headers().result();
                    using enum boost::beast::http::status;
                    if (
                      const auto is_no_content_and_accepted
                      = status == no_content && accept_no_content;
                      status != ok && !is_no_content_and_accepted) {
                        vlog(
                          s3_log.warn,
                          "S3 PUT request failed for key {}: {} {:l}",
                          id,
                          status,
                          ref->get_headers());
                        const auto content_type
                          = util::get_response_content_type(ref->get_headers());
                        return parse_rest_error_response<>(
                          content_type, status, std::move(res));
                    }
                    return ss::now();
                });
            })
            .handle_exception_type(
              [](const ss::abort_requested_exception& err) {
                  return ss::make_exception_future<>(err);
              })
            .handle_exception_type([this, id](const rest_error_response& err) {
                _probe->register_failure(err.code(), op_type_tag::upload);
                if (err.code() == s3_error_code::_unknown) {
                    vlog(
                      s3_log.error,
                      "S3 PUT request failed with error for key {}: {}",
                      id,
                      err);
                }
                return ss::make_exception_future<>(err);
            })
            .handle_exception([id](std::exception_ptr eptr) {
                vlog(
                  s3_log.warn,
                  "S3 PUT request failed with error for key {}: {}",
                  id,
                  eptr);
                return ss::make_exception_future<>(eptr);
            })
            .finally([&body]() { return body.close(); });
      });
}

ss::future<result<s3_client::list_bucket_result, error_outcome>>
s3_client::list_objects(
  const plain_bucket_name& name,
  std::optional<object_key> prefix,
  std::optional<object_key> start_after,
  std::optional<size_t> max_keys,
  std::optional<ss::sstring> continuation_token,
  ss::lowres_clock::duration timeout,
  std::optional<char> delimiter,
  std::optional<item_filter> collect_item_if) {
    const object_key dummy{""};
    co_return co_await send_request(
      do_list_objects_v2(
        name,
        prefix,
        start_after,
        max_keys,
        continuation_token,
        timeout,
        delimiter,
        std::move(collect_item_if)),
      name,
      dummy);
}

ss::future<s3_client::list_bucket_result> s3_client::do_list_objects_v2(
  const plain_bucket_name& name,
  std::optional<object_key> prefix,
  std::optional<object_key> start_after,
  std::optional<size_t> max_keys,
  std::optional<ss::sstring> continuation_token,
  ss::lowres_clock::duration timeout,
  std::optional<char> delimiter,
  std::optional<item_filter> collect_item_if) {
    auto header = _requestor.make_list_objects_v2_request(
      name,
      std::move(prefix),
      std::move(start_after),
      std::move(max_keys),
      std::move(continuation_token),
      delimiter);
    if (!header) {
        return ss::make_exception_future<list_bucket_result>(
          std::system_error(header.error()));
    }
    vlog(s3_log.trace, "send https request:\n{}", header.value());
    return _client.request(std::move(header.value()), timeout)
      .then([pred = std::move(collect_item_if)](
              const http::client::response_stream_ref& resp) mutable {
          return resp->prefetch_headers().then(
            [resp, gather_item_if = std::move(pred)]() mutable {
                const auto& header = resp->get_headers();
                if (header.result() != boost::beast::http::status::ok) {
                    vlog(
                      s3_log.warn,
                      "S3 ListObjectsv2 request failed: {} {:l}",
                      header.result(),
                      header);

                    const auto content_type = util::get_response_content_type(
                      header);
                    // In the error path we drain the response stream fully, the
                    // error response should not be very large.
                    return http::drain(resp).then(
                      [result = header.result(), content_type](iobuf buf) {
                          return parse_rest_error_response<list_bucket_result>(
                            content_type, result, std::move(buf));
                      });
                }

                return parse_from_stream<aws_parse_impl>(
                  resp->as_input_stream(), std::move(gather_item_if));
            });
      });
}

ss::future<result<s3_client::no_response, error_outcome>>
s3_client::delete_object(
  const plain_bucket_name& bucket,
  const object_key& key,
  ss::lowres_clock::duration timeout) {
    using ret_t = result<s3_client::no_response, error_outcome>;

    return send_request(
             do_delete_object(bucket, key, timeout).then([] {
                 return ss::make_ready_future<no_response>(no_response{});
             }),
             bucket,
             key)
      .then([&bucket, &key](const ret_t& result) {
          // Google's implementation of S3 returns a NoSuchKey error
          // when the object to be deleted does not exist. We return
          // no_response{} in this case in order to get the same behaviour of
          // AWS S3.
          //
          // TODO: Subclass cloud_storage_clients::client with a GCS client
          // implementation where this edge case is handled.
          if (!result && result.error() == error_outcome::key_not_found) {
              vlog(
                s3_log.debug,
                "Object to be deleted was not found in cloud storage: "
                "object={}, bucket={}. Ignoring ...",
                key,
                bucket);
              return ss::make_ready_future<ret_t>(no_response{});
          } else {
              return ss::make_ready_future<ret_t>(result);
          }
      });
}

ss::future<> s3_client::do_delete_object(
  const plain_bucket_name& bucket,
  const object_key& key,
  ss::lowres_clock::duration timeout) {
    auto header = _requestor.make_delete_object_request(bucket, key);
    if (!header) {
        return ss::make_exception_future<>(std::system_error(header.error()));
    }
    vlog(s3_log.trace, "send https request:\n{}", header.value());
    return _client.request(std::move(header.value()), timeout)
      .then([key](const http::client::response_stream_ref& ref) {
          return http::drain(ref).then([ref, key](iobuf&& res) {
              auto status = ref->get_headers().result();
              if (
                status != boost::beast::http::status::ok
                && status
                     != boost::beast::http::status::no_content) { // expect 204
                  vlog(
                    s3_log.warn,
                    "S3 DeleteObject request failed for key {}: {} {:l}",
                    key,
                    status,
                    ref->get_headers());
                  const auto content_type = util::get_response_content_type(
                    ref->get_headers());
                  return parse_rest_error_response<>(
                    content_type, status, std::move(res));
              }
              return ss::now();
          });
      });
}

std::variant<client::delete_objects_result, rest_error_response>
iobuf_to_delete_objects_result(iobuf&& buf) {
    auto root = util::iobuf_to_ptree(std::move(buf), s3_log);
    auto result = client::delete_objects_result{};
    try {
        if (
          auto error_code = root.get_optional<ss::sstring>("Error.Code");
          error_code) {
            // This is an error response. S3 can reply with 200 error code and
            // error response in the body.
            constexpr const char* empty = "";
            auto code = root.get<ss::sstring>("Error.Code", empty);
            auto msg = root.get<ss::sstring>("Error.Message", empty);
            auto rid = root.get<ss::sstring>("Error.RequestId", empty);
            auto res = root.get<ss::sstring>("Error.Resource", empty);
            rest_error_response err(code, msg, rid, res);
            return err;
        }
        for (const auto& [tag, value] : root.get_child("DeleteResult")) {
            if (tag != "Error") {
                continue;
            }
            auto code = value.get_optional<ss::sstring>("Code");
            auto key = value.get_optional<ss::sstring>("Key");
            auto message = value.get_optional<ss::sstring>("Message");
            auto version_id = value.get_optional<ss::sstring>("VersionId");
            vlog(
              s3_log.trace,
              R"(delete_objects_result::undeleted_keys Key:"{}" Code: "{}" Message:"{}" VersionId:"{}")",
              key.value_or("[no key present]"),
              code.value_or("[no error code present]"),
              message.value_or("[no error message present]"),
              version_id.value_or("[no version id present]"));
            if (key.has_value()) {
                result.undeleted_keys.push_back({
                  object_key{key.value()},
                  code.value_or("[no error code present]"),
                });
            } else {
                vlog(
                  s3_log.warn,
                  "a DeleteResult.Error does not contain the Key tag");
            }
        }
    } catch (...) {
        vlog(
          s3_log.error,
          "DeleteObjects response parse failed: {}",
          std::current_exception());
        if (s3_log.is_enabled(ss::log_level::trace)) {
            std::stringstream outs;
            boost::property_tree::write_xml(outs, root);
            vlog(s3_log.trace, "Response XML: {}", outs.str());
        }
        throw;
    }
    return result;
}

auto s3_client::do_delete_objects(
  const plain_bucket_name& bucket,
  const chunked_vector<object_key>& keys,
  ss::lowres_clock::duration timeout)
  -> ss::future<client::delete_objects_result> {
    auto request = _requestor.make_delete_objects_request(bucket, keys);
    if (!request) {
        return ss::make_exception_future<delete_objects_result>(
          std::system_error(request.error()));
    }
    auto& [header, body] = request.value();
    vlog(s3_log.trace, "send DeleteObjects request:\n{}", header);

    return ss::do_with(
             std::move(body),
             [&_client = _client, header = std::move(header), timeout](
               auto& to_delete) mutable {
                 return _client.request(std::move(header), to_delete, timeout)
                   .finally([&] { return to_delete.close(); });
             })
      .then([](const http::client::response_stream_ref& response) {
          return http::drain(response).then([response](iobuf&& res) {
              auto status = response->get_headers().result();
              if (status != boost::beast::http::status::ok) {
                  const auto content_type = util::get_response_content_type(
                    response->get_headers());
                  return parse_rest_error_response<delete_objects_result>(
                    content_type, status, std::move(res));
              }
              auto parse_result = iobuf_to_delete_objects_result(
                std::move(res));
              if (
                std::holds_alternative<client::delete_objects_result>(
                  parse_result)) {
                  return ss::make_ready_future<delete_objects_result>(
                    std::get<client::delete_objects_result>(parse_result));
              }
              return ss::make_exception_future<delete_objects_result>(
                std::get<rest_error_response>(parse_result));
          });
      });
}

auto s3_client::delete_objects(
  const plain_bucket_name& bucket,
  const chunked_vector<object_key>& keys,
  ss::lowres_clock::duration timeout)
  -> ss::future<result<delete_objects_result, error_outcome>> {
    const object_key dummy{""};
    co_return co_await send_request(
      do_delete_objects(bucket, keys, timeout), bucket, dummy);
}

bool s3_client::is_valid() const noexcept {
    // If the upstream is gone (evicted) credentials may be stale so we consider
    // the client no longer valid. maybe_refresh_credentials() would be a
    // no-op.
    return _upstream_ptr.get() != nullptr;
}

fmt::iterator s3_client::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "S3Client{{{}}}", _client.server_address());
}

gcs_client::gcs_client(
  ss::weak_ptr<upstream> upstream_ptr,
  const s3_configuration& conf,
  const net::base_transport::configuration& transport_conf,
  ss::shared_ptr<client_probe> probe,
  ss::lw_shared_ptr<const cloud_roles::apply_credentials> apply_credentials)
  : s3_client(
      std::move(upstream_ptr),
      conf,
      transport_conf,
      std::move(probe),
      std::move(apply_credentials)) {}

gcs_client::gcs_client(
  ss::weak_ptr<upstream> upstream_ptr,
  const s3_configuration& conf,
  const net::base_transport::configuration& transport_conf,
  ss::shared_ptr<client_probe> probe,
  const ss::abort_source& as,
  ss::lw_shared_ptr<const cloud_roles::apply_credentials> apply_credentials)
  : s3_client(
      std::move(upstream_ptr),
      conf,
      transport_conf,
      std::move(probe),
      as,
      std::move(apply_credentials)) {}

auto gcs_client::delete_objects(
  const plain_bucket_name& bucket,
  const chunked_vector<object_key>& keys,
  ss::lowres_clock::duration timeout)
  -> ss::future<result<delete_objects_result, error_outcome>> {
    const object_key dummy{""};
    co_return co_await send_request(
      do_delete_objects(bucket, keys, timeout), bucket, dummy);
}

auto gcs_client::do_delete_objects(
  const plain_bucket_name& bucket,
  const chunked_vector<object_key>& keys,
  ss::lowres_clock::duration timeout)
  -> ss::future<client::delete_objects_result> {
    auto request = _requestor.make_gcs_batch_delete_request(bucket, keys);
    if (!request) {
        co_return ss::coroutine::exception(
          std::make_exception_ptr(std::system_error(request.error())));
    }
    auto& [header, body] = request.value();
    vlog(s3_log.trace, "send GCS batch delete request:\n{}", header);

    std::exception_ptr ex;
    std::optional<delete_objects_result> result;
    try {
        auto response_stream = co_await _client.request(
          std::move(header), body, timeout);

        co_await response_stream->prefetch_headers();
        vassert(response_stream->is_header_done(), "Header is not received");

        const auto status = response_stream->get_headers().result();
        // GCS batch API returns 200 OK for successful batch requests
        // Individual subrequest failures are encoded in the multipart response
        if (status != boost::beast::http::status::ok) {
            const auto content_type = util::get_response_content_type(
              response_stream->get_headers());
            auto buf = co_await http::drain(std::move(response_stream));
            co_await body.close();
            co_return co_await parse_rest_error_response<delete_objects_result>(
              content_type, status, std::move(buf));
        }

        // Extract boundary from Content-Type header
        const auto& headers = response_stream->get_headers();
        auto boundary = util::find_multipart_boundary(headers);
        auto response_buf = co_await http::drain(std::move(response_stream));
        if (!boundary.has_value()) {
            throw std::runtime_error(boundary.error());
        }
        if (s3_log.is_enabled(ss::log_level::trace)) {
            auto preview_bytes
              = iobuf_const_parser(response_buf)
                  .peek_bytes(
                    std::min(response_buf.size_bytes(), size_t{1024}));
            vlog(
              s3_log.trace,
              "GCS batch delete response: boundary='{}', body_size={}, "
              "first_bytes='{}'",
              boundary.value(),
              response_buf.size_bytes(),
              std::string_view(
                reinterpret_cast<const char*>(preview_bytes.data()),
                preview_bytes.size()));
        }
        result = parse_gcs_batch_delete_response(
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

fmt::iterator gcs_client::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "GCSClient{{{}}}", _client.server_address());
}

// Multipart upload implementations //

result<http::client::request_header>
request_creator::make_create_multipart_upload_request(
  const plain_bucket_name& name, const object_key& key) {
    // POST /{bucket}/{key}?uploads HTTP/1.1
    // Host: s3.{region}.amazonaws.com
    http::client::request_header header{};
    auto host = make_host(name);
    auto target = fmt::format("{}?uploads", make_target(name, key));
    header.method(boost::beast::http::verb::post);
    header.target(target);
    header.insert(
      boost::beast::http::field::user_agent, aws_header_values::user_agent);
    header.insert(boost::beast::http::field::host, host);
    header.insert(boost::beast::http::field::content_length, "0");
    auto ec = _apply_credentials->add_auth(header);
    if (ec) {
        return ec;
    }
    util::url_encode_target(header);
    return header;
}

result<http::client::request_header> request_creator::make_upload_part_request(
  const plain_bucket_name& name,
  const object_key& key,
  size_t part_number,
  const ss::sstring& upload_id,
  size_t payload_size_bytes) {
    // PUT /{bucket}/{key}?partNumber={part_number}&uploadId={upload_id}
    // HTTP/1.1
    http::client::request_header header{};
    auto host = make_host(name);
    auto target = fmt::format(
      "{}?partNumber={}&uploadId={}",
      make_target(name, key),
      part_number,
      upload_id);
    header.method(boost::beast::http::verb::put);
    header.target(target);
    header.insert(
      boost::beast::http::field::user_agent, aws_header_values::user_agent);
    header.insert(boost::beast::http::field::host, host);
    header.insert(
      boost::beast::http::field::content_type, aws_header_values::text_plain);
    header.insert(
      boost::beast::http::field::content_length,
      std::to_string(payload_size_bytes));
    auto ec = _apply_credentials->add_auth(header);
    if (ec) {
        return ec;
    }
    util::url_encode_target(header);
    return header;
}

result<std::tuple<http::client::request_header, ss::input_stream<char>>>
request_creator::make_complete_multipart_upload_request(
  const plain_bucket_name& name,
  const object_key& key,
  const ss::sstring& upload_id,
  const std::vector<ss::sstring>& etags) {
    // POST /{bucket}/{key}?uploadId={upload_id} HTTP/1.1
    // Body: XML with list of parts
    http::client::request_header header{};
    auto host = make_host(name);
    auto target = fmt::format(
      "{}?uploadId={}", make_target(name, key), upload_id);

    // Generate XML body
    // <CompleteMultipartUpload>
    //   <Part><PartNumber>1</PartNumber><ETag>"etag1"</ETag></Part>
    //   <Part><PartNumber>2</PartNumber><ETag>"etag2"</ETag></Part>
    // </CompleteMultipartUpload>
    auto xml_body = iobuf::from(R"xml(<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUpload>
)xml");
    for (size_t i = 0; i < etags.size(); ++i) {
        xml_body.append_str(
          fmt::format(
            "  <Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>\n",
            i + 1,
            etags[i]));
    }
    xml_body.append_str("</CompleteMultipartUpload>\n");

    header.method(boost::beast::http::verb::post);
    header.target(target);
    header.insert(
      boost::beast::http::field::user_agent, aws_header_values::user_agent);
    header.insert(boost::beast::http::field::host, host);
    header.insert(boost::beast::http::field::content_type, "application/xml");
    header.insert(
      boost::beast::http::field::content_length,
      std::to_string(xml_body.size_bytes()));
    auto ec = _apply_credentials->add_auth(header);
    if (ec) {
        return ec;
    }
    util::url_encode_target(header);

    // Create input stream from XML body
    auto body = make_iobuf_input_stream(std::move(xml_body));

    return std::make_tuple(std::move(header), std::move(body));
}

result<http::client::request_header>
request_creator::make_abort_multipart_upload_request(
  const plain_bucket_name& name,
  const object_key& key,
  const ss::sstring& upload_id) {
    // DELETE /{bucket}/{key}?uploadId={upload_id} HTTP/1.1
    http::client::request_header header{};
    auto host = make_host(name);
    auto target = fmt::format(
      "{}?uploadId={}", make_target(name, key), upload_id);
    header.method(boost::beast::http::verb::delete_);
    header.target(target);
    header.insert(
      boost::beast::http::field::user_agent, aws_header_values::user_agent);
    header.insert(boost::beast::http::field::host, host);
    header.insert(boost::beast::http::field::content_length, "0");
    auto ec = _apply_credentials->add_auth(header);
    if (ec) {
        return ec;
    }
    util::url_encode_target(header);
    return header;
}

ss::future<result<ss::shared_ptr<multipart_upload_state>, error_outcome>>
s3_client::initiate_multipart_upload(
  const plain_bucket_name& bucket,
  const object_key& key,
  size_t part_size,
  ss::lowres_clock::duration timeout) {
    // Validate part size
    if (part_size < multipart_limits::min_s3_part_size) {
        vlog(
          s3_log.error,
          "S3 part_size {} < minimum {}",
          part_size,
          multipart_limits::min_s3_part_size);
        co_return error_outcome::fail;
    }

    if (part_size > multipart_limits::max_s3_part_size) {
        vlog(
          s3_log.warn,
          "S3 part_size {} > maximum {} (continuing)",
          part_size,
          multipart_limits::max_s3_part_size);
    }

    // Create and return multipart state
    // Caller will wrap this in a multipart_upload
    auto state = ss::make_shared<s3_multipart_state>(
      this, bucket, key, timeout);
    co_return state;
}

// s3_multipart_state implementations //

s3_multipart_state::s3_multipart_state(
  s3_client* client,
  plain_bucket_name bucket,
  object_key key,
  ss::lowres_clock::duration timeout)
  : _client(client)
  , _bucket(std::move(bucket))
  , _key(std::move(key))
  , _timeout(timeout) {}

ss::future<> s3_multipart_state::initialize_multipart() {
    vlog(
      s3_log.debug,
      "Initializing S3 multipart upload for {}/{}",
      _bucket,
      _key);

    auto header = _client->_requestor.make_create_multipart_upload_request(
      _bucket, _key);
    if (!header) {
        throw std::system_error(header.error());
    }

    vlog(
      s3_log.trace, "send CreateMultipartUpload request:\n{}", header.value());

    auto response_stream = co_await _client->_client.request(
      std::move(header.value()), _timeout);

    co_await response_stream->prefetch_headers();
    vassert(response_stream->is_header_done(), "Header is not received");

    const auto status = response_stream->get_headers().result();
    if (status != boost::beast::http::status::ok) {
        const auto content_type = util::get_response_content_type(
          response_stream->get_headers());
        auto buf = co_await http::drain(std::move(response_stream));
        co_return co_await parse_rest_error_response(
          content_type, status, std::move(buf));
    }

    // Parse XML response to extract UploadId
    auto response_buf = co_await http::drain(std::move(response_stream));

    try {
        auto response_tree = util::iobuf_to_ptree(
          std::move(response_buf), s3_log);
        _upload_id = response_tree.get<ss::sstring>(
          "InitiateMultipartUploadResult.UploadId");

        _client->_probe->register_multipart_create();

        vlog(
          s3_log.debug,
          "Initialized S3 multipart upload with upload_id: {}",
          _upload_id);
    } catch (const std::exception& ex) {
        vlog(
          s3_log.error,
          "Failed to parse CreateMultipartUpload response: {}",
          ex);
        throw std::runtime_error(
          fmt::format(
            "Failed to parse UploadId from CreateMultipartUpload response: {}",
            ex.what()));
    }
}

ss::future<> s3_multipart_state::upload_part(size_t part_num, iobuf data) {
    vassert(!_upload_id.empty(), "Multipart upload not initialized");

    const size_t data_size = data.size_bytes();
    vlog(
      s3_log.debug,
      "Uploading part {} (size: {}) for upload_id: {}",
      part_num,
      data_size,
      _upload_id);

    auto header = _client->_requestor.make_upload_part_request(
      _bucket, _key, part_num, _upload_id, data_size);
    if (!header) {
        throw std::system_error(header.error());
    }

    vlog(s3_log.trace, "send UploadPart request:\n{}", header.value());

    // Convert iobuf to input_stream
    auto body = make_iobuf_input_stream(std::move(data));

    auto response_stream = co_await _client->_client
                             .request(std::move(header.value()), body, _timeout)
                             .finally([&body] { return body.close(); });

    co_await response_stream->prefetch_headers();
    vassert(response_stream->is_header_done(), "Header is not received");

    const auto status = response_stream->get_headers().result();
    if (status != boost::beast::http::status::ok) {
        const auto content_type = util::get_response_content_type(
          response_stream->get_headers());
        auto buf = co_await http::drain(std::move(response_stream));
        co_return co_await parse_rest_error_response(
          content_type, status, std::move(buf));
    }

    // Extract ETag from response headers
    const auto& headers = response_stream->get_headers();
    auto etag_it = headers.find(boost::beast::http::field::etag);
    if (etag_it == headers.end()) {
        co_await http::drain(std::move(response_stream));
        throw std::runtime_error("UploadPart response missing ETag header");
    }

    ss::sstring etag(etag_it->value().data(), etag_it->value().size());
    _etags.push_back(std::move(etag));

    // Drain response
    co_await http::drain(std::move(response_stream));

    _client->_probe->register_multipart_upload();

    vlog(
      s3_log.debug, "Uploaded part {} with ETag: {}", part_num, _etags.back());
}

ss::future<> s3_multipart_state::complete_multipart_upload() {
    vassert(!_upload_id.empty(), "Multipart upload not initialized");

    vlog(
      s3_log.debug,
      "Completing S3 multipart upload {} with {} parts",
      _upload_id,
      _etags.size());

    auto request = _client->_requestor.make_complete_multipart_upload_request(
      _bucket, _key, _upload_id, _etags);
    if (!request) {
        throw std::system_error(request.error());
    }

    auto [header, body] = std::move(request.value());
    vlog(s3_log.trace, "send CompleteMultipartUpload request:\n{}", header);

    auto response_stream = co_await _client->_client
                             .request(std::move(header), body, _timeout)
                             .finally([&body] { return body.close(); });

    co_await response_stream->prefetch_headers();
    vassert(response_stream->is_header_done(), "Header is not received");

    const auto status = response_stream->get_headers().result();
    if (status != boost::beast::http::status::ok) {
        const auto content_type = util::get_response_content_type(
          response_stream->get_headers());
        auto buf = co_await http::drain(std::move(response_stream));
        co_return co_await parse_rest_error_response(
          content_type, status, std::move(buf));
    }

    // AWS S3 can return errors embedded in a 200 OK response body for
    // CompleteMultipartUpload. The response must be parsed to detect these.
    // See:
    // https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
    auto response_buf = co_await http::drain(std::move(response_stream));
    auto response_tree = util::iobuf_to_ptree(std::move(response_buf), s3_log);
    if (
      auto error_code = response_tree.get_optional<std::string>("Error.Code");
      error_code) {
        // Use std::string for ptree extraction since ss::sstring's stream
        // extraction operator reads only until whitespace, which truncates
        // multi-word error messages.
        throw rest_error_response(
          *error_code,
          response_tree.get<std::string>("Error.Message", ""),
          response_tree.get<std::string>("Error.RequestId", ""),
          response_tree.get<std::string>("Error.Resource", ""));
    }

    _client->_probe->register_multipart_complete();

    vlog(s3_log.debug, "Completed multipart upload {}", _upload_id);
}

ss::future<> s3_multipart_state::abort_multipart_upload() {
    if (_upload_id.empty()) {
        // Nothing to abort
        vlog(s3_log.debug, "Abort called but multipart not initialized");
        co_return;
    }

    vlog(s3_log.debug, "Aborting S3 multipart upload {}", _upload_id);

    auto header = _client->_requestor.make_abort_multipart_upload_request(
      _bucket, _key, _upload_id);
    if (!header) {
        // Log error but don't throw - abort should be best effort
        vlog(
          s3_log.warn,
          "Failed to create abort request: {}",
          header.error().message());
        co_return;
    }

    vlog(
      s3_log.trace, "send AbortMultipartUpload request:\n{}", header.value());

    try {
        auto response_stream = co_await _client->_client.request(
          std::move(header.value()), _timeout);

        co_await response_stream->prefetch_headers();
        vassert(response_stream->is_header_done(), "Header is not received");

        const auto status = response_stream->get_headers().result();
        if (
          status != boost::beast::http::status::no_content
          && status != boost::beast::http::status::ok) {
            vlog(
              s3_log.warn,
              "AbortMultipartUpload returned unexpected status: {}",
              status);
        }

        // Drain response
        co_await http::drain(std::move(response_stream));

        _client->_probe->register_multipart_abort();

        vlog(s3_log.debug, "Aborted multipart upload {}", _upload_id);
    } catch (const std::exception& ex) {
        // Log but don't throw - abort failures are non-fatal
        vlog(
          s3_log.warn,
          "Failed to abort multipart upload {}: {}",
          _upload_id,
          ex);
    }
}

ss::future<> s3_multipart_state::upload_as_single_object(iobuf data) {
    vlog(
      s3_log.debug,
      "Using single put_object for small file (size: {})",
      data.size_bytes());

    const size_t data_size = data.size_bytes();
    auto body = make_iobuf_input_stream(std::move(data));

    // Use existing put_object implementation
    co_await _client->do_put_object(
      _bucket, _key, data_size, std::move(body), _timeout);
}

} // namespace cloud_storage_clients
