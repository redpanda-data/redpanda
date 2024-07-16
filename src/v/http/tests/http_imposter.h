/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "base/vassert.h"
#include "http/tests/registered_urls.h"
#include "utils/unresolved_address.h"

#include <seastar/core/sstring.hh>
#include <seastar/http/httpd.hh>

class http_imposter_fixture {
public:
    static constexpr std::string_view httpd_host_name = "localhost";
    static constexpr std::string_view httpd_host_ip = "127.0.0.1";

    uint16_t httpd_port_number();

public:
    static constexpr auto imdsv2_token_url = "/latest/api/token";
    using request_predicate
      = ss::noncopyable_function<bool(const ss::http::request&)>;

    using predicates = std::vector<request_predicate>;

    /**
     * Listening on TCP ports from a unit test is not a great practice, but we
     * do it in some places. To enable these unit tests to run in parallel,
     * each unit test binary must declare its own function to pick a port that
     * no other unit test uses.
     */
    explicit http_imposter_fixture(uint16_t port);

    virtual ~http_imposter_fixture();

    http_imposter_fixture(const http_imposter_fixture&) = delete;
    http_imposter_fixture& operator=(const http_imposter_fixture&) = delete;
    http_imposter_fixture(http_imposter_fixture&&) = delete;
    http_imposter_fixture& operator=(http_imposter_fixture&&) = delete;

    /// Before calling this method, we need to set up mappings for URLs for the
    /// server to respond to, using the when() API.
    /// Without any mappings set up, the server responds with 404 and a canned
    /// Not Found response.
    void listen();

    /// Access all http requests ordered by time
    const std::vector<http_test_utils::request_info>& get_requests() const;

    using req_pred_t
      = std::function<bool(const http_test_utils::request_info&)>;

    /// Access http requests matching the given predicate
    std::vector<http_test_utils::request_info>
    get_requests(req_pred_t predicate) const;

    /// Get the latest request to a particular URL
    std::optional<std::reference_wrapper<const http_test_utils::request_info>>
    get_latest_request(
      const ss::sstring& url, bool ignore_url_params = false) const;

    /// Get the number of requests to a particular URL
    size_t get_request_count(const ss::sstring& url) const;

    /// Access all http requests ordered by target url
    const std::multimap<ss::sstring, http_test_utils::request_info>&
    get_targets() const;

    /// Starting point for URL registration fluent API
    /// Example usage:
    /// when("/foo")
    ///     .with_method(POST)
    ///     .then_return("bar");
    http_test_utils::registered_urls& when() { return _urls; }

    bool has_call(std::string_view url, bool ignore_url_params = false) const;

    /// Enables requests with a specific condition to fail. The failing
    /// request is also added to the set of calls stored by fixture.
    void fail_request_if(
      request_predicate predicate, http_test_utils::response response);

    void reset_http_call_state();

    // Helper to progress over a range and check if the current element is
    // present in it. If the element is found at a position, the range for
    // future searches is adjusted to that position. Used for checking if a set
    // of elements is present in the same order in another searched range.
    //
    // eg [A B C] are present in [1 A C B X C D A] in order
    template<typename Iterator>
    struct search_state {
        search_state(Iterator begin, Iterator end)
          : _begin{begin}
          , _end{end} {}

        template<typename T>
        bool operator()(T&& url) {
            auto found = std::find_if(
              _begin, _end, [&url](const auto& u) { return u.url == url; });
            if (found == _end) {
                return false;
            }
            _begin = found;
            return true;
        }

    private:
        Iterator _begin;
        Iterator _end;
    };

    template<typename... Urls>
    bool has_calls_in_order(Urls&&... urls) const {
        search_state s{_requests.cbegin(), _requests.cend()};
        return (... && s(std::forward<Urls>(urls)));
    }

    http_test_utils::response
    lookup(const http_test_utils::request_info& req) const {
        return _urls.lookup(req);
    }

    void log_requests() const;

    /// Makes the server return the canned response for duration, without
    /// logging any incoming requests.
    void start_request_masking(
      http_test_utils::response canned_response,
      ss::lowres_clock::duration duration);

    net::unresolved_address address() const { return _address; }

private:
    struct request_masking {
        http_test_utils::response canned_response;
        ss::lowres_clock::duration duration;
        ss::lowres_clock::time_point started;
    };

    void set_routes(ss::httpd::routes& r);

    uint16_t _port;
    ss::socket_address _server_addr;
    ss::httpd::http_server_control _server;

    std::unique_ptr<ss::httpd::handler_base> _handler;
    /// Contains saved requests
    std::vector<http_test_utils::request_info> _requests;
    /// Contains all accessed target urls
    std::multimap<ss::sstring, http_test_utils::request_info> _targets;

    http_test_utils::registered_urls _urls;
    predicates _fail_requests_when;

    absl::flat_hash_map<size_t, http_test_utils::response> _fail_responses;
    ss::sstring _id;
    std::optional<request_masking> _masking_active;

    net::unresolved_address _address;
};
