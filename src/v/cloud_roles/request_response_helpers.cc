/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "request_response_helpers.h"

#include "bytes/iostream.h"
#include "bytes/streambuf.h"
#include "cloud_roles/logger.h"
#include "config/configuration.h"
#include "json/istreamwrapper.h"
#include "json/ostreamwrapper.h"
#include "json/schema.h"

#include <rapidjson/error/en.h>

namespace cloud_roles {

ss::future<boost::beast::http::status>
get_status(http::client::response_stream_ref& resp) {
    co_await resp->prefetch_headers();
    co_return resp->get_headers().result();
}

using http_call = ss::noncopyable_function<ss::future<api_response>(
  http::client::request_header&)>;

namespace {

ss::future<iobuf>
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

/// Helper function to catch and log common HTTP errors around user supplied
/// operation.
ss::future<api_response>
do_request(http::client::request_header req, http_call func) {
    try {
        co_return co_await func(req);
    } catch (const std::system_error& ec) {
        if (!is_retryable(ec)) {
            vlog(
              clrl_log.warn,
              "abort system error {} while making request {}",
              ec,
              req);
            co_return api_response(make_abort_error(ec));
        } else {
            vlog(
              clrl_log.warn,
              "retryable system error {} while making request {}",
              ec,
              req);
            co_return api_response(make_retryable_error(ec));
        }
    } catch (const std::exception& e) {
        vlog(
          clrl_log.warn,
          "abort exception {} while making request {}",
          e.what(),
          req);
        co_return api_response(make_abort_error(e));
    }
}

ss::future<> log_error_response(http::client::response_stream_ref stream) {
    auto buf = co_await drain_response_stream(stream);
    iobuf_parser p{std::move(buf)};
    auto response_string = p.read_string(p.bytes_left());
    vlog(
      clrl_log.error,
      "failed during IAM credentials refresh: {}",
      response_string);
}

ss::future<api_response> make_request_without_payload(
  http::client& client,
  http::client::request_header req,
  std::optional<std::chrono::milliseconds> timeout) {
    auto tout = timeout.value_or(
      config::shard_local_cfg().cloud_storage_roles_operation_timeout_ms);
    auto response_stream = co_await client.request(std::move(req), tout);
    auto status = co_await get_status(response_stream);
    if (is_retryable(status)) {
        co_await log_error_response(response_stream);
        co_return make_retryable_error(
          fmt::format("http request failed:{}", status), status);
    }

    if (status != boost::beast::http::status::ok) {
        co_await log_error_response(response_stream);
        co_return make_abort_error(
          fmt::format("http request failed:{}", status), status);
    }
    co_return co_await drain_response_stream(std::move(response_stream));
}

ss::future<api_response> make_request_with_payload(
  http::client& client,
  http::client::request_header& req,
  iobuf content,
  std::optional<std::chrono::milliseconds> timeout) {
    req.set(
      boost::beast::http::field::content_length,
      fmt::format("{}", content.size_bytes()));

    auto stream = make_iobuf_input_stream(std::move(content));
    auto tout = timeout.value_or(
      config::shard_local_cfg().cloud_storage_roles_operation_timeout_ms);

    auto response = co_await client.request(std::move(req), stream, tout);
    auto status = co_await get_status(response);
    if (is_retryable(status)) {
        co_await log_error_response(response);
        co_return make_retryable_error(
          fmt::format("http request failed:{}", status), status);
    }

    if (status != boost::beast::http::status::ok) {
        co_await log_error_response(response);
        co_return make_abort_error(
          fmt::format("http request failed:{}", status), status);
    }
    auto data = co_await drain_response_stream(std::move(response));
    co_await stream.close();
    co_return data;
}

} // namespace

ss::future<api_response> make_request(
  http::client client,
  http::client::request_header req,
  std::optional<std::chrono::milliseconds> timeout) {
    auto tout = timeout.value_or(
      config::shard_local_cfg().cloud_storage_roles_operation_timeout_ms);
    return http::with_client(
      std::move(client), [req = std::move(req), tout](auto& client) mutable {
          return do_request(req, [&client, tout](auto& req) mutable {
              return make_request_without_payload(client, req, tout);
          });
      });
}

json::Document parse_json_response(iobuf resp) {
    iobuf_istreambuf ibuf{resp};
    std::istream stream{&ibuf};
    json::Document doc;
    json::IStreamWrapper wrapper(stream);
    doc.ParseStream(wrapper);
    return doc;
}

validate_and_parse_res
parse_json_response_and_validate(std::string_view schema, iobuf resp) {
    auto schema_doc = json::Document{};
    schema_doc.Parse(schema.data(), schema.size());
    if (schema_doc.HasParseError()) {
        return api_response_parse_error{ssx::sformat(
          "schema generated parsing errors: {} @{}",
          rapidjson::GetParseError_En(schema_doc.GetParseError()),
          schema_doc.GetErrorOffset())};
    }

    auto success_schema = json::SchemaDocument{schema_doc};
    auto jresp = parse_json_response(std::move(resp));

    auto validator = json::SchemaValidator{success_schema};
    jresp.Accept(validator);

    if (validator.IsValid()) {
        // successful validation
        return jresp;
    }

    auto& err_obj = validator.GetError();
    if (auto m_it = err_obj.FindMember("required");
        m_it != err_obj.MemberEnd()) {
        // some missing fields, return a list (note, this might hide other
        // problems. but missing fields is likely more important)
        auto missing_fields_array = m_it->value["missing"].GetArray();
        auto err_resp = malformed_api_response_error{};
        err_resp.missing_fields.reserve(missing_fields_array.Size());
        for (auto& jf : missing_fields_array) {
            err_resp.missing_fields.emplace_back(
              jf.GetString(), jf.GetStringLength());
        }
        return err_resp;
    }

    // generic validation error. render error to json and return it
    auto oss = std::ostringstream{};
    auto joss = json::OStreamWrapper{oss};
    auto writer = json::Writer<json::OStreamWrapper>{joss};
    err_obj.Accept(writer);
    return api_response_parse_error{oss.str()};
}

ss::future<api_response> request_with_payload(
  http::client client,
  http::client::request_header req,
  iobuf content,
  std::optional<std::chrono::milliseconds> timeout) {
    auto tout = timeout.value_or(
      config::shard_local_cfg().cloud_storage_roles_operation_timeout_ms);

    return http::with_client(
      std::move(client),
      [req = std::move(req), content = std::move(content), tout](
        auto& client) mutable -> ss::future<api_response> {
          return do_request(
            req,
            [&client, content = std::move(content), tout](auto& req) mutable {
                return make_request_with_payload(
                  client, req, std::move(content), tout);
            });
      });
}

ss::future<api_response> request_with_payload(
  http::client client,
  http::client::request_header req,
  seastar::sstring content,
  std::optional<std::chrono::milliseconds> timeout) {
    iobuf b;
    b.append(content.data(), content.size());
    auto tout = timeout.value_or(
      config::shard_local_cfg().cloud_storage_roles_operation_timeout_ms);
    return request_with_payload(
      std::move(client), std::move(req), std::move(b), tout);
}

std::chrono::system_clock::time_point parse_timestamp(std::string_view sv) {
    std::tm tm = {};
    std::stringstream ss({sv.data(), sv.size()});
    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S.Z%Z");
    return std::chrono::system_clock::from_time_t(timegm(&tm));
}

} // namespace cloud_roles
