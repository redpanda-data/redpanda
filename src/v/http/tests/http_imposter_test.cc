/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iobuf_parser.h"
#include "http/client.h"
#include "http/tests/http_imposter.h"
#include "test_utils/fixture.h"

#include <boost/test/unit_test.hpp>

namespace bh = boost::beast::http;

class fixture : public http_imposter_fixture {
public:
    fixture()
      : http_imposter_fixture(4443) {}
};

FIXTURE_TEST(test_get, fixture) {
    when()
      .request("/foo")
      .with_method(ss::httpd::GET)
      .then_reply_with("bar", ss::http::reply::status_type::ok);

    listen();

    http::client::request_header header;
    header.method(boost::beast::http::verb::get);
    header.target("/foo");
    header.insert(
      bh::field::host, {httpd_host_name.data(), httpd_host_name.size()});

    auto client = http::client{
      {.server_addr = {httpd_host_name.data(), httpd_port_number()}}};

    auto response = client.request(std::move(header)).get0();
    iobuf response_data;
    while (!response->is_done()) {
        response_data.append(response->recv_some().get0());
    }

    auto headers = response->get_headers();
    iobuf_parser p(std::move(response_data));
    auto data = p.read_string(p.bytes_left());

    BOOST_REQUIRE_EQUAL(headers.result(), bh::status::ok);
    BOOST_REQUIRE_EQUAL(data, "bar");
    BOOST_REQUIRE(has_call("/foo"));
}

FIXTURE_TEST(test_post, fixture) {
    when()
      .request("/foo")
      .with_method(ss::httpd::POST)
      .then_reply_with("bar", ss::http::reply::status_type::ok);

    listen();

    auto client = http::client{
      {.server_addr = {httpd_host_name.data(), httpd_port_number()}}};

    http::client::request_header header;
    header.method(boost::beast::http::verb::post);
    header.target("/foo");
    header.insert(
      bh::field::host, {httpd_host_name.data(), httpd_host_name.size()});

    auto response = client.request(std::move(header)).get0();
    iobuf response_data;
    while (!response->is_done()) {
        response_data.append(response->recv_some().get0());
    }

    auto headers = response->get_headers();
    iobuf_parser p(std::move(response_data));
    auto data = p.read_string(p.bytes_left());

    BOOST_REQUIRE_EQUAL(headers.result(), bh::status::ok);
    BOOST_REQUIRE_EQUAL(data, "bar");

    BOOST_REQUIRE(has_call("/foo"));
}

FIXTURE_TEST(test_forbidden, fixture) {
    when()
      .request("/super-secret-area")
      .with_method(ss::httpd::GET)
      .then_reply_with(ss::http::reply::status_type::forbidden);

    listen();

    auto client = http::client{
      {.server_addr = {httpd_host_name.data(), httpd_port_number()}}};

    {
        http::client::request_header header;
        header.method(boost::beast::http::verb::get);
        header.target("/super-secret-area");
        header.insert(
          bh::field::host, {httpd_host_name.data(), httpd_host_name.size()});

        auto response = client.request(std::move(header)).get0();
        iobuf response_data;
        while (!response->is_done()) {
            response_data.append(response->recv_some().get0());
        }

        BOOST_REQUIRE_EQUAL(
          response->get_headers().result(), bh::status::forbidden);
    }

    {
        http::client::request_header header;
        header.method(boost::beast::http::verb::get);
        header.target("/super-secret-area");
        header.insert(
          bh::field::host, {httpd_host_name.data(), httpd_host_name.size()});

        auto response = client.request(std::move(header)).get0();
        iobuf response_data;
        while (!response->is_done()) {
            response_data.append(response->recv_some().get0());
        }

        BOOST_REQUIRE_EQUAL(
          response->get_headers().result(), bh::status::forbidden);
    }

    // test the calls in order api
    BOOST_REQUIRE(
      has_calls_in_order("/super-secret-area", "/super-secret-area"));
}
