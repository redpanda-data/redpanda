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

#include "net/unresolved_address.h"
#include "pandaproxy/json/types.h"

#include <boost/beast/core/string_type.hpp>
#include <http/client.h>
#include <fmt/format.h>

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
  boost::beast::string_view host,
  boost::beast::http::verb method,
  boost::beast::string_view target,
  iobuf const& body,
  serialization_format content,
  serialization_format accept) {
    http::client::request_header hdr;
    hdr.set(boost::beast::http::field::host, host);
    hdr.method(method);
    hdr.target(target);
    hdr.insert(
      boost::beast::http::field::content_length,
      boost::beast::to_static_string(body.size_bytes()));
    if (content != serialization_format::none) {
        hdr.insert(
          boost::beast::http::field::content_type, to_header_value(content));
    }
    if (accept != serialization_format::none) {
        hdr.insert(boost::beast::http::field::accept, to_header_value(accept));
    }
    return hdr;
}

// TODO(@NyaliaLui): Update boost::beast because
// this to_string_view is defined in latest version
// https://github.com/boostorg/beast/blob/master/include/boost/beast/core/string_type.hpp
inline boost::beast::string_view to_string_view(const ss::sstring& s)
{
    return boost::beast::string_view(s.data(), s.size());
}

inline consumed_response do_request(
  http::client& client,
  net::unresolved_address& host,
  boost::beast::http::verb method,
  boost::beast::string_view target,
  iobuf&& body,
  serialization_format content,
  serialization_format accept) {
    ss::sstring _host{fmt::format("{}:{}", host.host(), host.port())};
    auto hdr = make_header(to_string_view(_host), method, target, body, content, accept);
    auto [req, res] = client.make_request(std::move(hdr)).get();
    req->send_some(std::move(body)).get();
    req->send_eof().get();
    auto bdy = consume_body(*res);
    vassert(res->is_header_done(), "Header isn't done");
    return {res->get_headers(), std::move(bdy)};
}

inline consumed_response http_request(
  http::client& client,
  net::unresolved_address& host,
  boost::beast::string_view target,
  boost::beast::http::verb method = boost::beast::http::verb::get,
  serialization_format content = serialization_format::none,
  serialization_format accept = serialization_format::none) {
    return do_request(client, host, method, target, iobuf(), content, accept);
}

inline consumed_response http_request(
  http::client& client,
  net::unresolved_address& host,
  boost::beast::string_view target,
  iobuf&& body,
  boost::beast::http::verb method = boost::beast::http::verb::post,
  serialization_format content = serialization_format::none,
  serialization_format accept = serialization_format::none) {
    return do_request(client, host, method, target, std::move(body), content, accept);
}
