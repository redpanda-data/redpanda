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
#include "vlog.h"

#include <utility>

namespace {
constexpr boost::beast::string_view text_plain = "text/plain";
constexpr boost::beast::string_view block_blob = "BlockBlob";
} // namespace

namespace cloud_storage_clients {

static abs_rest_error_response
parse_rest_error_response(boost::beast::http::status result, iobuf&& buf) {
    using namespace cloud_storage_clients;

    try {
        auto resp = util::iobuf_to_ptree(std::move(buf), abs_log);
        auto code = resp.get<ss::sstring>("Error.Code", "");
        auto msg = resp.get<ss::sstring>("Error.Message", "");
        return {std::move(code), std::move(msg), result};
    } catch (...) {
        vlog(
          cloud_storage_clients::abs_log.error,
          "!!error parse error {}",
          std::current_exception());
        throw;
    }
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
    // x-ms-date:{req-datetime in RFC9110}
    // x-ms-version:"2021-08-06"
    // Authorization:{signature}
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
    // x-ms-date:{req-datetime in RFC9110}
    // x-ms-version:"2021-08-06"
    // x-ms-tags:{object-formatter-tags}
    // Authorization:{signature}
    // Content-Length:{payload-size}
    // Content-Type: text/plain
    const auto target = fmt::format("/{}/{}", name(), key().string());
    const boost::beast::string_view host{_ap().data(), _ap().length()};

    http::client::request_header header{};
    header.method(boost::beast::http::verb::put);
    header.target(target);
    header.insert(boost::beast::http::field::host, host);
    header.insert(boost::beast::http::field::content_type, text_plain);
    header.insert(
      boost::beast::http::field::content_length,
      std::to_string(payload_size_bytes));
    header.insert("x-ms-blob-type", block_blob);

    if (!tags.empty()) {
        header.insert("x-ms-tags", tags.str());
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
    // GET /{container-id}/{blob-id}?comp=metadata HTTP/1.1
    // Host: {storage-account-id}.blob.core.windows.net
    // x-ms-date:{req-datetime in RFC9110}
    // x-ms-version:"2021-08-06"
    // Authorization:{signature}
    const auto target = fmt::format(
      "/{}/{}?comp=metadata", name(), key().string());
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
abs_request_creator::make_delete_blob_request(
  bucket_name const& name, object_key const& key) {
    // DELETE /{container-id}/{blob-id} HTTP/1.1
    // Host: {storage-account-id}.blob.core.windows.net
    // x-ms-date:{req-datetime in RFC9110}
    // x-ms-version:"2021-08-06"
    // Authorization:{signature}
    const auto target = fmt::format("/{}/{}", name(), key().string());
    const boost::beast::string_view host{_ap().data(), _ap().length()};

    http::client::request_header header{};
    header.method(boost::beast::http::verb::delete_);
    header.target(target);
    header.insert(boost::beast::http::field::host, host);
    header.insert("x-ms-delete-snapshots", "include");

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
  std::optional<size_t> max_keys) {
    // GET /{container-id}?restype=container&comp=list&prefix={prefix}...
    // ...&max_results{max_keys}
    // HTTP/1.1 Host: {storage-account-id}.blob.core.windows.net
    // x-ms-date:{req-datetime in RFC9110}
    // x-ms-version:"2021-08-06"
    // Authorization:{signature}
    auto target = fmt::format("/{}?restype=container&comp=list", name());
    if (prefix) {
        target += fmt::format("&prefix={}", prefix.value()());
    }

    if (max_keys) {
        target += fmt::format("&max_results={}", max_keys.value());
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

ss::future<> abs_client::stop() { return _client.stop(); }

void abs_client::shutdown() { _client.shutdown(); }

template<typename T>
ss::future<result<T, error_outcome>> abs_client::send_request(
  ss::future<T> request_future,
  const bucket_name& bucket,
  const object_key& key) {
    auto outcome = error_outcome::retry;

    try {
        co_return co_await std::move(request_future);
    } catch (const abs_rest_error_response& err) {
        _probe->register_failure(err.code());

        if (err.code() == abs_error_code::blob_not_found) {
            // Unexpected 404s are logged by 'request_future' at warn
            // level, so only log at debug level here.
            vlog(s3_log.debug, "BlobNotFound response received {}", key);
            outcome = error_outcome::key_not_found;
        } else {
            vlog(
              abs_log.error,
              "Accessing storage account {}, unexpected REST API error "
              "detected: code={}, http_code={}\n{}",
              bucket,
              err.code_string(),
              err.http_code(),
              err.message());
            outcome = error_outcome::fail;
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
  const ss::lowres_clock::duration& timeout,
  bool expect_no_such_key) {
    return send_request(
      do_get_object(name, key, timeout, expect_no_such_key), name, key);
}

ss::future<http::client::response_stream_ref> abs_client::do_get_object(
  bucket_name const& name,
  object_key const& key,
  const ss::lowres_clock::duration& timeout,
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
              "ABS replied with expected error: {}",
              response_stream->get_headers());
        } else {
            vlog(
              abs_log.warn,
              "ABS replied with error: {}",
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
  ss::input_stream<char>&& body,
  const object_tag_formatter& tags,
  const ss::lowres_clock::duration& timeout) {
    return send_request(
      do_put_object(name, key, payload_size, std::move(body), tags, timeout)
        .then(
          []() { return ss::make_ready_future<no_response>(no_response{}); }),
      name,
      key);
}

ss::future<> abs_client::do_put_object(
  bucket_name const& name,
  object_key const& key,
  size_t payload_size,
  ss::input_stream<char>&& body,
  const object_tag_formatter& tags,
  const ss::lowres_clock::duration& timeout) {
    auto header = _requestor.make_put_blob_request(
      name, key, payload_size, tags);
    if (!header) {
        vlog(
          abs_log.warn, "Failed to create request header: {}", header.error());
        throw std::system_error(header.error());
    }

    auto response_stream = co_await _client.request(
      std::move(header.value()), body, timeout);

    co_await body.close();

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
  const ss::lowres_clock::duration& timeout) {
    return send_request(do_head_object(name, key, timeout), name, key);
}

ss::future<abs_client::head_object_result> abs_client::do_head_object(
  bucket_name const& name,
  object_key const& key,
  const ss::lowres_clock::duration& timeout) {
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
        co_return head_object_result{
          .object_size = 0, .etag = ss::sstring{etag.data(), etag.length()}};
    } else {
        auto buf = co_await util::drain_response_stream(
          std::move(response_stream));
        throw parse_rest_error_response(status, std::move(buf));
    }
}

ss::future<result<abs_client::no_response, error_outcome>>
abs_client::delete_object(
  const bucket_name& name,
  const object_key& key,
  const ss::lowres_clock::duration& timeout) {
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
  const ss::lowres_clock::duration& timeout) {
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

// ss::future<result<abs_client::list_bucket_result, error_outcome>>
// abs_client::list_objects(
//   const bucket_name& name,
//   std::optional<object_key> prefix,
//   std::optional<object_key> start_after,
//   std::optional<size_t> max_keys,
//   const ss::lowres_clock::duration& timeout) {
//     return send_request(
//       do_list_objects(
//         name, std::move(prefix), std::move(start_after), max_keys, timeout),
//       name,
//       object_key{""});
// }
//
// ss::future<abs_client::list_bucket_result> abs_client::do_list_objects(
//   const bucket_name& name,
//   std::optional<object_key> prefix,
//   std::optional<object_key> start_after,
//   std::optional<size_t> max_keys,
//   const ss::lowres_clock::duration& timeout) {
//     auto header = _requestor.make_list_blobs_request(
//       name, std::move(prefix), std::move(start_after), max_keys);
//     if (!header) {
//         vlog(
//           abs_log.warn, "Failed to create request header: {}",
//           header.error());
//         throw std::system_error(header.error());
//     }
//
//     vlog(abs_log.trace, "send https request:\n{}", header.value());
//
//     auto response_stream = co_await _client.request(
//       std::move(header.value()), timeout);
//
//     co_await response_stream->prefetch_headers();
//     vassert(response_stream->is_header_done(), "Header is not received");
// }

} // namespace cloud_storage_clients
