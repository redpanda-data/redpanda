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

#include "cloud_storage_clients/abs_error.h"
#include "cloud_storage_clients/configuration.h"
#include "cloud_storage_clients/logger.h"
#include "cloud_storage_clients/util.h"
#include "cloud_storage_clients/xml_sax_parser.h"
#include "vlog.h"

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
constexpr boost::beast::string_view tags_name = "x-ms-tags";
constexpr boost::beast::string_view delete_snapshot_name
  = "x-ms-delete-snapshots";
constexpr boost::beast::string_view delete_snapshot_value = "include";

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

static abs_rest_error_response
parse_rest_error_response(boost::beast::http::status result, iobuf buf) {
    using namespace cloud_storage_clients;

    try {
        auto resp = util::iobuf_to_ptree(std::move(buf), abs_log);
        auto code = resp.get<ss::sstring>("Error.Code", "");
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
parse_header_error_response(const http::http_response::header_type& hdr) {
    ss::sstring code;
    ss::sstring msg;
    if (hdr.result() == boost::beast::http::status::not_found) {
        code = "BlobNotFound";
        msg = "Not found";
    } else {
        code = "Unknown";
        msg = ss::sstring(hdr.reason().data(), hdr.reason().size());
    }

    return {std::move(code), std::move(msg), hdr.result()};
}

abs_request_creator::abs_request_creator(
  const abs_configuration& conf,
  ss::lw_shared_ptr<const cloud_roles::apply_credentials> apply_credentials)
  : _ap{conf.uri}
  , _apply_credentials{std::move(apply_credentials)} {}

result<http::client::request_header> abs_request_creator::make_get_blob_request(
  bucket_name const& name, object_key const& key) {
    // GET /{container-id}/{blob-id} HTTP/1.1
    // Host: {storage-account-id}.blob.core.windows.net
    // x-ms-date:{req-datetime in RFC9110} # added by 'add_auth'
    // x-ms-version:"2021-08-06"           # added by 'add_auth'
    // Authorization:{signature}           # added by 'add_auth'
    const auto target = fmt::format("/{}/{}", name(), key().string());
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

result<http::client::request_header> abs_request_creator::make_put_blob_request(
  bucket_name const& name,
  object_key const& key,
  size_t payload_size_bytes,
  const object_tag_formatter& tags) {
    // PUT /{container-id}/{blob-id} HTTP/1.1
    // Host: {storage-account-id}.blob.core.windows.net
    // x-ms-date:{req-datetime in RFC9110} # added by 'add_auth'
    // x-ms-version:"2021-08-06"           # added by 'add_auth'
    // x-ms-tags:{object-formatter-tags}
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

    if (!tags.empty()) {
        header.insert(tags_name, tags.str());
    }

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
    // x-ms-version:"2021-08-06"           # added by 'add_auth'
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
    // x-ms-version:"2021-08-06"           # added by 'add_auth'
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
  std::optional<object_key> prefix,
  [[maybe_unused]] std::optional<object_key> start_after,
  std::optional<size_t> max_keys,
  std::optional<char> delimiter) {
    // GET /{container-id}?restype=container&comp=list&prefix={prefix}...
    // ...&max_results{max_keys}
    // HTTP/1.1 Host: {storage-account-id}.blob.core.windows.net
    // x-ms-date:{req-datetime in RFC9110} # added by 'add_auth'
    // x-ms-version:"2021-08-06"           # added by 'add_auth'
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

abs_client::abs_client(
  const abs_configuration& conf,
  ss::lw_shared_ptr<const cloud_roles::apply_credentials> apply_credentials)
  : _requestor(conf, std::move(apply_credentials))
  , _client(conf)
  , _probe(conf._probe) {}

abs_client::abs_client(
  const abs_configuration& conf,
  const ss::abort_source& as,
  ss::lw_shared_ptr<const cloud_roles::apply_credentials> apply_credentials)
  : _requestor(conf, std::move(apply_credentials))
  , _client(conf, &as, conf._probe, conf.max_idle_time)
  , _probe(conf._probe) {}

ss::future<> abs_client::stop() {
    vlog(abs_log.debug, "Stopping ABS client");

    co_await _client.stop();
    co_await _client.wait_input_shutdown();

    vlog(abs_log.debug, "Stopped ABS client");
}

void abs_client::shutdown() { _client.shutdown(); }

template<typename T>
ss::future<result<T, error_outcome>> abs_client::send_request(
  ss::future<T> request_future,
  const bucket_name& bucket,
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
            outcome = error_outcome::retry_slowdown;
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
              "Received [{}] {} unexpected error response for storage account "
              "{} from ABS: {}",
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
  bool expect_no_such_key) {
    return send_request(
      do_get_object(name, key, timeout, expect_no_such_key),
      name,
      key,
      op_type_tag::download);
}

ss::future<http::client::response_stream_ref> abs_client::do_get_object(
  bucket_name const& name,
  object_key const& key,
  ss::lowres_clock::duration timeout,
  bool expect_no_such_key) {
    auto header = _requestor.make_get_blob_request(name, key);
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

        auto buf = co_await util::drain_response_stream(
          std::move(response_stream));
        throw parse_rest_error_response(status, std::move(buf));
    }

    co_return response_stream;
}

ss::future<result<abs_client::no_response, error_outcome>>
abs_client::put_object(
  bucket_name const& name,
  object_key const& key,
  size_t payload_size,
  ss::input_stream<char> body,
  const object_tag_formatter& tags,
  ss::lowres_clock::duration timeout) {
    return send_request(
      do_put_object(name, key, payload_size, std::move(body), tags, timeout)
        .then(
          []() { return ss::make_ready_future<no_response>(no_response{}); }),
      name,
      key,
      op_type_tag::upload);
}

ss::future<> abs_client::do_put_object(
  bucket_name const& name,
  object_key const& key,
  size_t payload_size,
  ss::input_stream<char> body,
  const object_tag_formatter& tags,
  ss::lowres_clock::duration timeout) {
    auto header = _requestor.make_put_blob_request(
      name, key, payload_size, tags);
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
        auto buf = co_await util::drain_response_stream(
          std::move(response_stream));
        throw parse_rest_error_response(status, std::move(buf));
    }
}

ss::future<result<abs_client::head_object_result, error_outcome>>
abs_client::head_object(
  bucket_name const& name,
  object_key const& key,
  ss::lowres_clock::duration timeout) {
    return send_request(do_head_object(name, key, timeout), name, key);
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
    return send_request(
             do_delete_object(name, key, timeout).then([]() {
                 return ss::make_ready_future<no_response>(no_response{});
             }),
             name,
             key)
      .then([&name, &key](const ret_t& result) {
          // ABS returns a 404 for attempts to delete a blob that doesn't exist.
          // The remote doesn't expect this, so we map 404s to a successful
          // response.
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
        auto buf = co_await util::drain_response_stream(
          std::move(response_stream));
        throw parse_rest_error_response(status, std::move(buf));
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
      name,
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
      name, std::move(prefix), std::move(start_after), max_keys, delimiter);
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
        iobuf buf = co_await util::drain_response_stream(response_stream);
        throw parse_rest_error_response(status, std::move(buf));
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

} // namespace cloud_storage_clients
