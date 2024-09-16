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

#include "base/type_traits.h"
#include "debug_bundle/debug_bundle_service.h"
#include "debug_bundle/error.h"
#include "debug_bundle/json.h"
#include "debug_bundle/types.h"
#include "json/document.h"
#include "json/types.h"
#include "redpanda/admin/api-doc/debug_bundle.json.hh"
#include "redpanda/admin/server.h"
#include "reflection/type_traits.h"
#include "ssx/sformat.h"
#include "utils/functional.h"

#include <seastar/core/sstring.hh>
#include <seastar/http/reply.hh>
#include <seastar/json/json_elements.hh>
#include <seastar/util/short_streams.hh>

#include <boost/lexical_cast/try_lexical_convert.hpp>
#include <fmt/core.h>
#include <rapidjson/error/en.h>

#include <charconv>
#include <chrono>
#include <sstream>

namespace {

ss::future<debug_bundle::result<json::Document>>
as_json_doc(ss::http::request* req) {
    json::Document doc;
    auto content = co_await ss::util::read_entire_stream_contiguous(
      *req->content_stream);
    doc.Parse(content);
    if (doc.HasParseError()) {
        co_return debug_bundle::error_info{
          debug_bundle::error_code::invalid_parameters,
          fmt::format(
            "JSON parse error: {} at offset {}",
            rapidjson::GetParseError_En(doc.GetParseError()),
            doc.GetErrorOffset())};
    } else {
        co_return std::move(doc);
    }
}

template<typename T>
std::unique_ptr<ss::http::reply> make_json_body(
  ss::http::reply::status_type status,
  const T& t,
  std::unique_ptr<ss::http::reply> rep) {
    rep->set_status(status);
    rep->write_body("json", ss::json::stream_object(t));
    return rep;
}

std::unique_ptr<ss::http::reply> make_error_body(
  debug_bundle::error_code ec,
  const ss::sstring& msg,
  std::unique_ptr<ss::http::reply> rep) {
    ss::httpd::debug_bundle_json::error_body res;
    res.code = static_cast<int>(ec);
    res.message = msg;

    ss::http::reply::status_type status{ss::http::reply::status_type::ok};
    switch (ec) {
    case debug_bundle::error_code::success:
        status = ss::http::reply::status_type::ok;
        break;
    case debug_bundle::error_code::debug_bundle_process_running:
    case debug_bundle::error_code::debug_bundle_process_never_started:
    case debug_bundle::error_code::debug_bundle_process_not_running:
        status = ss::http::reply::status_type::conflict;
        break;
    case debug_bundle::error_code::invalid_parameters:
        status = ss::http::reply::status_type::unprocessable_entity;
        break;
    case debug_bundle::error_code::process_failed:
    case debug_bundle::error_code::internal_error:
    case debug_bundle::error_code::rpk_binary_not_present:
        status = ss::http::reply::status_type::internal_server_error;
        break;
    case debug_bundle::error_code::job_id_not_recognized:
        status = ss::http::reply::status_type::not_found;
        break;
    }
    return make_json_body(status, res, std::move(rep));
}

std::unique_ptr<ss::http::reply> make_error_body(
  const debug_bundle::error_info& err, std::unique_ptr<ss::http::reply> rep) {
    return make_error_body(err.code(), err.message(), std::move(rep));
}

} // namespace

void admin_server::register_debug_bundle_routes() {
    register_route_raw_async<superuser>(
      ss::httpd::debug_bundle_json::post_debug_bundle,
      [this](
        std::unique_ptr<ss::http::request> req,
        std::unique_ptr<ss::http::reply> rep) {
          return post_debug_bundle(std::move(req), std::move(rep));
      });
}

ss::future<std::unique_ptr<ss::http::reply>> admin_server::post_debug_bundle(
  std::unique_ptr<ss::http::request> req,
  std::unique_ptr<ss::http::reply> rep) {
    using debug_bundle::from_json;

    const auto json_doc = co_await as_json_doc(req.get());
    if (json_doc.has_error()) {
        co_return make_error_body(
          std::move(json_doc).assume_error(), std::move(rep));
    }
    if (!json_doc.assume_value().IsObject()) {
        co_return make_error_body(
          debug_bundle::error_code::invalid_parameters,
          "Request body is not a JSON object",
          std::move(rep));
    }
    const auto& obj = json_doc.assume_value().GetObject();

    auto job_id = from_json<debug_bundle::job_id_t>(obj, "job_id", true);
    if (job_id.has_error()) {
        co_return make_error_body(
          std::move(job_id).assume_error(), std::move(rep));
    }

    auto params = from_json<debug_bundle::debug_bundle_parameters>(
      obj, "config", false);
    if (params.has_error()) {
        co_return make_error_body(
          std::move(params).assume_error(), std::move(rep));
    }

    auto res = co_await _debug_bundle_service.local()
                 .initiate_rpk_debug_bundle_collection(
                   job_id.assume_value(), std::move(params).assume_value());
    if (res.has_error()) {
        co_return make_error_body(res.assume_error(), std::move(rep));
    }

    ss::httpd::debug_bundle_json::bundle_start_response body;
    body.job_id = ssx::sformat("{}", job_id.assume_value());
    co_return make_json_body(
      ss::http::reply::status_type::ok, body, std::move(rep));
}
