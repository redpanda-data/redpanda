/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "test_utils/http_imposter.h"

#include "config/node_config.h"
#include "vlog.h"

#include <seastar/http/function_handlers.hh>

#include <utility>

static ss::logger http_imposter_log("http_imposter"); // NOLINT

http_imposter_fixture::http_imposter_fixture()
  : _server_addr{ss::ipv4_addr{httpd_host_name.data(), httpd_port_number}} {
    _server.start().get();
}

http_imposter_fixture::~http_imposter_fixture() { _server.stop().get(); }

const std::vector<ss::httpd::request>&
http_imposter_fixture::get_requests() const {
    return _requests;
}

const std::multimap<ss::sstring, ss::httpd::request>&
http_imposter_fixture::get_targets() const {
    return _targets;
}

void http_imposter_fixture::listen() {
    _server.set_routes([this](ss::httpd::routes& r) { set_routes(r); }).get();
    _server.listen(_server_addr).get();
}

static ss::sstring remove_query_params(std::string_view url) {
    auto q_pos = url.find('?');
    return q_pos == std::string_view::npos ? ss::sstring{url.data(), url.size()}
                                           : ss::sstring{url.substr(0, q_pos)};
}

struct content_handler {
    explicit content_handler(http_imposter_fixture& imp)
      : fixture(imp) {}

    ss::sstring
    handle(ss::httpd::const_req request, ss::httpd::reply& repl) const {
        fixture.requests().push_back(request);
        fixture.targets().insert(std::make_pair(request._url, request));

        vlog(
          http_imposter_log.trace,
          "S3 imposter request {} - {} - {}",
          request._url,
          request.content_length,
          request._method);

        ss::httpd::request lookup_r{request};
        lookup_r._url = remove_query_params(request._url);

        auto response = fixture.lookup(lookup_r);
        repl.set_status(response.status);
        return response.body;
    }

    http_imposter_fixture& fixture;
};

void http_imposter_fixture::set_routes(ss::httpd::routes& r) {
    using namespace ss::httpd;
    _handler = std::make_unique<function_handler>(
      [this](const_req req, reply& repl) {
          return content_handler(*this).handle(req, repl);
      },
      "txt");
    r.add_default_handler(_handler.get());
}

bool http_imposter_fixture::has_call(std::string_view url) const {
    return std::find_if(
             _requests.cbegin(),
             _requests.cend(),
             [&url](const auto& r) { return r._url == url; })
           != _requests.cend();
}

bool http_imposter_fixture::has_calls_in_order(
  const std::vector<std::string_view>& urls) const {
    auto beg = _requests.cbegin();
    auto end = _requests.cend();

    for (const auto& url : urls) {
        auto found = std::find_if(
          beg, end, [&url](const auto& r) { return r._url == url; });

        if (found == end) {
            return false;
        }

        beg = found;
    }

    return true;
}
