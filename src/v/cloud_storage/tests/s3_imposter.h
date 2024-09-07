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

#include "cloud_storage_clients/client.h"
#include "config/configuration.h"
#include "http/tests/registered_urls.h"
#include "utils/uuid.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/http/httpd.hh>

#include <absl/container/flat_hash_set.h>

#include <chrono>
#include <exception>
#include <map>
#include <vector>

inline cloud_storage_clients::bucket_name random_test_bucket_name() {
    return cloud_storage_clients::bucket_name{
      "test-bucket-" + ss::sstring{uuid_t::create()}};
}

static constexpr cloud_storage_clients::s3_url_style default_url_style
  = cloud_storage_clients::s3_url_style::virtual_host;

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
    static constexpr size_t default_max_keys = 100;
    uint16_t httpd_port_number();
    static constexpr const char* httpd_host_name = "localhost";
    static constexpr const char* httpd_ip_addr = "127.0.0.1";

    s3_imposter_fixture(
      cloud_storage_clients::s3_url_style url_style = default_url_style);
    ~s3_imposter_fixture();

    s3_imposter_fixture(const s3_imposter_fixture&) = delete;
    s3_imposter_fixture& operator=(const s3_imposter_fixture&) = delete;
    s3_imposter_fixture(s3_imposter_fixture&&) = delete;
    s3_imposter_fixture& operator=(s3_imposter_fixture&&) = delete;

    struct expectation {
        ss::sstring url;
        std::optional<ss::sstring> body;
        bool slowdown = false;
    };

    /// Set expectations on REST API calls that supposed to be made
    /// Only the requests that described in this call will be possible
    /// to make. This method can only be called once per test run.
    ///
    /// \param expectations is a collection of access points that allow GET,
    /// PUT, and DELETE requests, each expectation has url and body. The body
    /// will be returned by GET call if it's set or trigger error if its null.
    /// The expectations are stateful. If the body of the expectation was set
    /// to null but there was PUT call that sent some data, subsequent GET call
    /// will retrieve this data.
    void set_expectations_and_listen(
      std::vector<expectation> expectations,
      std::optional<absl::flat_hash_set<ss::sstring>> headers_to_store
      = std::nullopt);

    /// Update expectations for the REST API.
    void add_expectations(std::vector<expectation> expectations);
    void remove_expectations(std::vector<ss::sstring> urls);

    /// Get object from S3 or nullopt if it doesn't exist
    std::optional<ss::sstring> get_object(const ss::sstring& url) const;

    /// Access all http requests ordered by time
    const std::vector<http_test_utils::request_info>& get_requests() const;

    using req_pred_t
      = std::function<bool(const http_test_utils::request_info&)>;

    /// Access http requests matching the given predicate
    std::vector<http_test_utils::request_info>
    get_requests(req_pred_t predicate) const;

    /// Access all http requests ordered by target url
    const std::multimap<ss::sstring, http_test_utils::request_info>&
    get_targets() const;

    cloud_storage_clients::s3_configuration get_configuration();

    void set_search_on_get_list(bool should) { _search_on_get_list = should; }

    void fail_request_if(req_pred_t pred, http_test_utils::response response);

    std::optional<http_test_utils::response>
    should_fail_request(const http_test_utils::request_info& ri);

    ss::sstring url_base() const;

    const cloud_storage_clients::bucket_name bucket_name
      = random_test_bucket_name();

protected:
    cloud_storage_clients::s3_url_style url_style;
    cloud_storage_clients::s3_configuration conf;

private:
    void set_routes(
      ss::httpd::routes& r,
      const std::vector<expectation>& expectations,
      std::optional<absl::flat_hash_set<ss::sstring>> headers_to_store
      = std::nullopt);

    ss::socket_address _server_addr;
    ss::shared_ptr<ss::httpd::http_server_control> _server;

    struct content_handler;
    friend struct content_handler;
    ss::shared_ptr<content_handler> _content_handler;
    std::unique_ptr<ss::httpd::handler_base> _handler;
    /// Contains saved requests
    std::vector<http_test_utils::request_info> _requests;
    /// Contains all accessed target urls
    std::multimap<ss::sstring, http_test_utils::request_info> _targets;

    /// Whether or not to search through expectations for content when handling
    /// a list GET request.
    bool _search_on_get_list{true};

    std::vector<req_pred_t> _fail_request_if;
    std::vector<http_test_utils::response> _failure_response;
};

class enable_cloud_storage_fixture {
public:
    enable_cloud_storage_fixture();

    ~enable_cloud_storage_fixture() {
        config::shard_local_cfg().cloud_storage_enabled.set_value(false);
    }
};

cloud_storage_clients::http_byte_range parse_byte_header(std::string_view s);

std::vector<cloud_storage_clients::object_key>
keys_from_delete_objects_request(const http_test_utils::request_info&);
