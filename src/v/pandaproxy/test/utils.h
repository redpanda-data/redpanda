/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include <http/client.h>

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

inline http::client::request_header make_header(
  boost::beast::http::verb method,
  boost::beast::string_view target,
  iobuf const& body) {
    http::client::request_header hdr;
    hdr.method(method);
    hdr.target(target);
    hdr.insert(
      boost::beast::http::field::content_length,
      boost::beast::to_static_string(body.size_bytes()));
    hdr.insert(
      boost::beast::http::field::content_type,
      "application/vnd.kafka.binary.v2+json");
    return hdr;
}

inline consumed_response do_request(
  http::client& client,
  boost::beast::http::verb method,
  boost::beast::string_view target,
  iobuf&& body) {
    auto hdr = make_header(method, target, body);
    auto [req, res] = client.make_request(std::move(hdr)).get();
    req->send_some(std::move(body)).get();
    req->send_eof().get();
    auto bdy = consume_body(*res);
    vassert(res->is_header_done(), "Header isn't done");
    return {res->get_headers(), std::move(bdy)};
}

inline consumed_response http_request(
  http::client& client,
  boost::beast::string_view target,
  boost::beast::http::verb method = boost::beast::http::verb::get) {
    return do_request(client, method, target, iobuf());
}

inline consumed_response http_request(
  http::client& client,
  boost::beast::string_view target,
  iobuf&& body,
  boost::beast::http::verb method = boost::beast::http::verb::post) {
    return do_request(client, method, target, std::move(body));
}
