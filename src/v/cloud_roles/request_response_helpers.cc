/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_roles/request_response_helpers.h"

#include "bytes/iobuf_istreambuf.h"
#include "json/istreamwrapper.h"

namespace cloud_roles {

static ss::future<iobuf>
drain_response_stream(http::client::response_stream_ref resp) {
    return ss::do_with(
      iobuf(), [resp = std::move(resp)](iobuf& outbuf) mutable {
          return ss::do_until(
                   [resp] { return resp->is_done(); },
                   [resp, &outbuf] {
                       return resp->recv_some().then([&outbuf](iobuf&& chunk) {
                           outbuf.append(std::move(chunk));
                       });
                   })
            .then([&outbuf] {
                return ss::make_ready_future<iobuf>(std::move(outbuf));
            });
      });
}

ss::future<boost::beast::http::status>
get_status(http::client::response_stream_ref& resp) {
    co_await resp->prefetch_headers();
    co_return resp->get_headers().result();
}

ss::future<api_response> make_request(
  http::client client,
  http::client::request_header req,
  std::chrono::milliseconds timeout) {
    try {
        auto response_stream = co_await client.request(std::move(req), timeout);
        auto status = co_await get_status(response_stream);
        if (
          std::find(
            retryable_http_status.begin(), retryable_http_status.end(), status)
          != retryable_http_status.end()) {
            co_return api_request_error{
              .reason = fmt::format("http request failed:{}", status),
              .error_kind = api_request_error_kind::failed_retryable};
        }

        if (status != boost::beast::http::status::ok) {
            co_return api_request_error{
              .reason = fmt::format("http request failed:{}", status),
              .error_kind = api_request_error_kind::failed_abort};
        }

        co_return co_await drain_response_stream(std::move(response_stream));
    } catch (const std::system_error& ec) {
        if (auto code = ec.code(); std::find(
                                     retryable_system_error_codes.begin(),
                                     retryable_system_error_codes.end(),
                                     code.value())
                                   == retryable_system_error_codes.end()) {
            co_return api_request_error{
              .reason = ec.what(),
              .error_kind = api_request_error_kind::failed_abort};
        }
        co_return api_request_error{
          .reason = ec.what(),
          .error_kind = api_request_error_kind::failed_retryable};
    } catch (const std::exception& e) {
        co_return api_request_error{
          .reason = e.what(),
          .error_kind = api_request_error_kind::failed_abort};
    }
}

json::Document parse_json_response(iobuf resp) {
    iobuf_istreambuf ibuf{resp};
    std::istream stream{&ibuf};
    json::Document doc;
    json::IStreamWrapper wrapper(stream);
    doc.ParseStream(wrapper);
    return doc;
}

ss::future<api_response> post_request(
  http::client client,
  http::client::request_header req,
  iobuf content,
  std::chrono::milliseconds timeout) {
    try {
        req.set(
          boost::beast::http::field::content_length,
          boost::beast::to_static_string(content.size_bytes()));

        auto [req_str, resp] = co_await client.make_request(
          std::move(req), timeout);
        co_await req_str->send_some(std::move(content));
        co_await req_str->send_eof();

        auto status = co_await get_status(resp);
        if (
          std::find(
            retryable_http_status.begin(), retryable_http_status.end(), status)
          != retryable_http_status.end()) {
            co_return api_request_error{
              .reason = fmt::format("http request failed:{}", status),
              .error_kind = api_request_error_kind::failed_retryable};
        }

        if (status != boost::beast::http::status::ok) {
            co_return api_request_error{
              .reason = fmt::format("http request failed:{}", status),
              .error_kind = api_request_error_kind::failed_abort};
        }

        co_return co_await drain_response_stream(std::move(resp));
    } catch (const std::system_error& ec) {
        if (auto code = ec.code(); std::find(
                                     retryable_system_error_codes.begin(),
                                     retryable_system_error_codes.end(),
                                     code.value())
                                   == retryable_system_error_codes.end()) {
            co_return api_request_error{
              .reason = ec.what(),
              .error_kind = api_request_error_kind::failed_abort};
        }
        co_return api_request_error{
          .reason = ec.what(),
          .error_kind = api_request_error_kind::failed_retryable};
    } catch (const std::exception& e) {
        co_return api_request_error{
          .reason = e.what(),
          .error_kind = api_request_error_kind::failed_abort};
    }
}

ss::future<api_response> post_request(
  http::client client,
  http::client::request_header req,
  seastar::sstring content,
  std::chrono::milliseconds timeout) {
    iobuf b;
    b.append(content.data(), content.size());
    return post_request(
      std::move(client), std::move(req), std::move(b), timeout);
}

std::chrono::system_clock::time_point parse_timestamp(std::string_view sv) {
    std::tm tm = {};
    std::stringstream ss({sv.data(), sv.size()});
    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S.Z%Z");
    return std::chrono::system_clock::from_time_t(timegm(&tm));
}

} // namespace cloud_roles
