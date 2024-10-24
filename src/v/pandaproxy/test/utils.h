/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "pandaproxy/json/types.h"

#include <boost/beast/core/string_type.hpp>
#include <fmt/core.h>
#include <http/client.h>

using serialization_format = pandaproxy::json::serialization_format;

struct consumed_response {
    http::client::response_header headers;
    ss::sstring body;
};

inline ss::sstring consume_body(http::client::response_stream& res) {
    ss::sstring result;
    while (!res.is_done()) {
        iobuf buf = res.recv_some().get();
        for (auto& fragm : buf) {
            result.append(fragm.get(), fragm.size());
        }
    }
    return result;
}

inline boost::beast::string_view to_header_value(serialization_format fmt) {
    auto sv = name(fmt);
    return {sv.data(), sv.size()};
}

inline http::client::request_header make_header(
  boost::beast::http::verb method,
  boost::beast::string_view target,
  const iobuf& body,
  serialization_format content,
  serialization_format accept) {
    http::client::request_header hdr;
    hdr.method(method);
    hdr.target(target);
    hdr.insert(
      boost::beast::http::field::content_length,
      fmt::format("{}", body.size_bytes()));
    if (content != serialization_format::none) {
        hdr.insert(
          boost::beast::http::field::content_type, to_header_value(content));
    }
    if (accept != serialization_format::none) {
        hdr.insert(boost::beast::http::field::accept, to_header_value(accept));
    }
    return hdr;
}

inline consumed_response do_request(
  http::client& client,
  boost::beast::http::verb method,
  boost::beast::string_view target,
  iobuf&& body,
  serialization_format content,
  serialization_format accept) {
    auto hdr = make_header(method, target, body, content, accept);
    auto res = client.request(std::move(hdr), std::move(body)).get();
    auto bdy = consume_body(*res);
    vassert(res->is_header_done(), "Header isn't done");
    return {res->get_headers(), std::move(bdy)};
}

inline consumed_response http_request(
  http::client& client,
  boost::beast::string_view target,
  boost::beast::http::verb method = boost::beast::http::verb::get,
  serialization_format content = serialization_format::none,
  serialization_format accept = serialization_format::none) {
    return do_request(client, method, target, iobuf(), content, accept);
}

inline consumed_response http_request(
  http::client& client,
  boost::beast::string_view target,
  iobuf&& body,
  boost::beast::http::verb method = boost::beast::http::verb::post,
  serialization_format content = serialization_format::none,
  serialization_format accept = serialization_format::none) {
    return do_request(client, method, target, std::move(body), content, accept);
}
