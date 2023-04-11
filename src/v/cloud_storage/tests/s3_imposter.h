/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage/base_manifest.h"
#include "config/configuration.h"
#include "http/tests/registered_urls.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/http/httpd.hh>

#include <chrono>
#include <exception>
#include <map>
#include <vector>

/// Emulates S3 REST API for testing purposes.
/// The imposter is a simple KV-store that contains a set of expectations.
/// Expectations are accessible by url via GET, PUT, and DELETE http calls.
/// Expectations are provided before impster starts to listen. They have
/// two field - url and optional body. If body is set to nullopt, attemtp
/// to read it using GET or delete it using DELETE requests will trigger an
/// http response with error code 404 and xml formatted error message.
/// If the body of the expectation is set by the user or PUT request it can
/// be retrieved using the GET request or deleted using the DELETE request.
class s3_imposter_fixture {
public:
    uint16_t httpd_port_number();
    static constexpr const char* httpd_host_name = "127.0.0.1";

    s3_imposter_fixture();
    ~s3_imposter_fixture();

    s3_imposter_fixture(const s3_imposter_fixture&) = delete;
    s3_imposter_fixture& operator=(const s3_imposter_fixture&) = delete;
    s3_imposter_fixture(s3_imposter_fixture&&) = delete;
    s3_imposter_fixture& operator=(s3_imposter_fixture&&) = delete;

    struct expectation {
        ss::sstring url;
        std::optional<ss::sstring> body;
    };

    /// Set expectaitions on REST API calls that supposed to be made
    /// Only the requests that described in this call will be possible
    /// to make. This method can only be called once per test run.
    ///
    /// \param expectations is a collection of access points that allow GET,
    /// PUT, and DELETE requests, each expectation has url and body. The body
    /// will be returned by GET call if it's set or trigger error if its null.
    /// The expectations are statefull. If the body of the expectation was set
    /// to null but there was PUT call that sent some data, subsequent GET call
    /// will retrieve this data.
    void
    set_expectations_and_listen(const std::vector<expectation>& expectations);

    /// Access all http requests ordered by time
    const std::vector<http_test_utils::request_info>& get_requests() const;

    /// Access all http requests ordered by target url
    const std::multimap<ss::sstring, http_test_utils::request_info>&
    get_targets() const;

    cloud_storage_clients::s3_configuration get_configuration();

private:
    void set_routes(
      ss::httpd::routes& r, const std::vector<expectation>& expectations);

    ss::socket_address _server_addr;
    ss::shared_ptr<ss::httpd::http_server_control> _server;

    std::unique_ptr<ss::httpd::handler_base> _handler;
    /// Contains saved requests
    std::vector<http_test_utils::request_info> _requests;
    /// Contains all accessed target urls
    std::multimap<ss::sstring, http_test_utils::request_info> _targets;
};

class enable_cloud_storage_fixture {
public:
    enable_cloud_storage_fixture();

    ~enable_cloud_storage_fixture() {
        config::shard_local_cfg().cloud_storage_enabled.set_value(false);
    }
};
