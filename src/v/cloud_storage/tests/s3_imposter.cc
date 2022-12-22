/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/tests/s3_imposter.h"

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/client_probe.h"
#include "seastarx.h"
#include "test_utils/async.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/util/defer.hh>

#include <boost/core/noncopyable.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

using namespace std::chrono_literals;

inline ss::logger fixt_log("fixture"); // NOLINT

static constexpr uint16_t httpd_port_number = 4430;
static constexpr const char* httpd_host_name = "127.0.0.1";

cloud_storage_clients::s3_configuration
s3_imposter_fixture::get_configuration() {
    net::unresolved_address server_addr(httpd_host_name, httpd_port_number);
    cloud_storage_clients::s3_configuration conf;
    conf.uri = cloud_storage_clients::access_point_uri(httpd_host_name);
    conf.access_key = cloud_roles::public_key_str("acess-key");
    conf.secret_key = cloud_roles::private_key_str("secret-key");
    conf.region = cloud_roles::aws_region_name("us-east-1");
    conf.server_addr = server_addr;
    conf._probe = ss::make_shared<cloud_storage_clients::client_probe>(
      net::metrics_disabled::yes,
      net::public_metrics_disabled::yes,
      "us-east-1",
      httpd_host_name);
    return conf;
}

s3_imposter_fixture::s3_imposter_fixture() {
    _server = ss::make_shared<ss::httpd::http_server_control>();
    _server->start().get();
    ss::ipv4_addr ip_addr = {httpd_host_name, httpd_port_number};
    _server_addr = ss::socket_address(ip_addr);
}

s3_imposter_fixture::~s3_imposter_fixture() { _server->stop().get(); }

const std::vector<ss::httpd::request>&
s3_imposter_fixture::get_requests() const {
    return _requests;
}

const std::multimap<ss::sstring, ss::httpd::request>&
s3_imposter_fixture::get_targets() const {
    return _targets;
}

void s3_imposter_fixture::set_expectations_and_listen(
  const std::vector<s3_imposter_fixture::expectation>& expectations) {
    _server
      ->set_routes([this, &expectations](ss::httpd::routes& r) {
          set_routes(r, expectations);
      })
      .get();
    _server->listen(_server_addr).get();
}

void s3_imposter_fixture::set_routes(
  ss::httpd::routes& r,
  const std::vector<s3_imposter_fixture::expectation>& expectations) {
    using namespace ss::httpd;
    struct content_handler {
        content_handler(
          const std::vector<expectation>& exp, s3_imposter_fixture& imp)
          : fixture(imp) {
            for (const auto& e : exp) {
                expectations[e.url] = e;
            }
        }
        ss::sstring handle(const_req request, reply& repl) {
            static const ss::sstring error_payload
              = R"xml(<?xml version="1.0" encoding="UTF-8"?>
                        <Error>
                            <Code>NoSuchKey</Code>
                            <Message>Object not found</Message>
                            <Resource>resource</Resource>
                            <RequestId>requestid</RequestId>
                        </Error>)xml";
            fixture._requests.push_back(request);
            fixture._targets.insert(std::make_pair(request._url, request));
            vlog(
              fixt_log.trace,
              "S3 imposter request {} - {} - {}",
              request._url,
              request.content_length,
              request._method);
            if (request._method == "GET") {
                auto it = expectations.find(request._url);
                if (it == expectations.end() || !it->second.body.has_value()) {
                    vlog(fixt_log.trace, "Reply GET request with error");
                    repl.set_status(reply::status_type::not_found);
                    return error_payload;
                }
                return *it->second.body;
            } else if (request._method == "PUT") {
                expectations[request._url] = {
                  .url = request._url, .body = request.content};
                return "";
            } else if (request._method == "DELETE") {
                auto it = expectations.find(request._url);
                if (it == expectations.end() || !it->second.body.has_value()) {
                    vlog(fixt_log.trace, "Reply DELETE request with error");
                    repl.set_status(reply::status_type::not_found);
                    return error_payload;
                }
                repl.set_status(reply::status_type::no_content);
                it->second.body = std::nullopt;
                return "";
            } else if (request._method == "HEAD") {
                auto it = expectations.find(request._url);
                if (it == expectations.end() || !it->second.body.has_value()) {
                    vlog(fixt_log.trace, "Reply HEAD request with error");
                    repl.add_header("x-amz-request-id", "placeholder-id");
                    repl.set_status(reply::status_type::not_found);
                } else {
                    repl.add_header("ETag", "placeholder-etag");
                    repl.add_header(
                      "Content-Length",
                      ssx::sformat("{}", it->second.body->size()));
                    repl.set_status(reply::status_type::ok);
                }
                vlog(
                  fixt_log.trace,
                  "S3 imposter response: {}",
                  repl.response_line());
                return "";
            }
            BOOST_FAIL("Unexpected request");
            return "";
        }
        std::map<ss::sstring, expectation> expectations;
        s3_imposter_fixture& fixture;
    };
    auto hd = ss::make_shared<content_handler>(expectations, *this);
    _handler = std::make_unique<function_handler>(
      [hd](const_req req, reply& repl) { return hd->handle(req, repl); },
      "txt");
    r.add_default_handler(_handler.get());
}
