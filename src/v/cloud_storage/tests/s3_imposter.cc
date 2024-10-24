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

#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "cloud_storage_clients/client.h"
#include "cloud_storage_clients/client_probe.h"
#include "test_utils/async.h"
#include "test_utils/test_macros.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/util/defer.hh>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/core/noncopyable.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/xml_parser.hpp>

#include <charconv>

using namespace std::chrono_literals;

inline ss::logger fixt_log("s3_imposter_fixture"); // NOLINT

/// For http_imposter to run this binary with a unique port
uint16_t unit_test_httpd_port_number() { return 4442; }

namespace {

using expectation_map_t
  = std::map<ss::sstring, s3_imposter_fixture::expectation>;

// Takes the input map of keys to expectations and returns a stringified XML
// corresponding to the appropriate S3 response.
ss::sstring list_objects_resp(
  const expectation_map_t& objects,
  ss::sstring prefix,
  ss::sstring delimiter,
  std::optional<size_t> max_keys_opt,
  std::optional<ss::sstring> continuation_token_opt) {
    std::map<ss::sstring, size_t> content_key_to_size;
    std::set<ss::sstring> common_prefixes;
    // Filter by prefix and group by the substring between the prefix and first
    // delimiter.
    auto max_keys = max_keys_opt.has_value()
                      ? std::min(
                          max_keys_opt.value(),
                          s3_imposter_fixture::default_max_keys)
                      : s3_imposter_fixture::default_max_keys;
    auto it = (continuation_token_opt.has_value())
                ? objects.find(continuation_token_opt.value())
                : objects.begin();
    auto end_it = objects.end();
    ss::sstring next_continuation_token = "";
    for (; it != end_it; ++it) {
        const auto& expectation = it->second;

        if (content_key_to_size.size() == max_keys) {
            next_continuation_token = it->first;
            break;
        }

        auto key = expectation.url;
        if (!key.empty() && key[0] == '/') {
            // Remove / character that S3 client adds
            key = key.substr(1);
        }
        vlog(fixt_log.trace, "Comparing {} to prefix {}", key, prefix);
        if (key.size() < prefix.size()) {
            continue;
        }
        if (key.compare(0, prefix.size(), prefix) != 0) {
            continue;
        }
        vlog(fixt_log.trace, "{} matches prefix {}", key, prefix);
        if (delimiter.empty()) {
            // No delimiter, we just need to return the content and not
            // prefixes.
            content_key_to_size.emplace(
              key,
              expectation.body.has_value() ? expectation.body.value().size()
                                           : 0);
            continue;
        }
        auto delimiter_pos = key.find(delimiter, prefix.size());
        if (delimiter_pos == std::string::npos) {
            common_prefixes.emplace(key);
            continue;
        }
        vlog(
          fixt_log.trace,
          "Delimiter pos {} prefix size {}",
          delimiter_pos,
          prefix.size());
        common_prefixes.emplace(
          prefix
          + key.substr(prefix.size(), delimiter_pos - prefix.size() + 1));
    }

    const bool is_truncated = (it != end_it);
    // Populate the returned XML.
    ss::sstring ret;
    ret += fmt::format(
      R"xml(
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>test-bucket</Name>
  <Prefix>{}</Prefix>
  <KeyCount>{}</KeyCount>
  <MaxKeys>{}</MaxKeys>
  <Delimiter>{}</Delimiter>
  <IsTruncated>{}</IsTruncated>
  <NextContinuationToken>{}</NextContinuationToken>
)xml",
      prefix,
      content_key_to_size.size(),
      max_keys,
      delimiter,
      is_truncated,
      next_continuation_token);
    for (const auto& [key, size] : content_key_to_size) {
        ret += fmt::format(
          R"xml(
  <Contents>
    <Key>{}</Key>
    <LastModified>2021-01-10T01:00:00.000Z</LastModified>
    <ETag>"test-etag-1"</ETag>
    <Size>{}</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
)xml",
          key,
          size);
    }
    ret += "<CommonPrefixes>\n";
    for (const auto& prefix : common_prefixes) {
        ret += fmt::format("<Prefix>{}</Prefix>\n", prefix);
    }
    ret += "</CommonPrefixes>\n";
    ret += "</ListBucketResult>\n";
    return ret;
}

uint64_t string_view_to_ul(std::string_view sv) {
    uint64_t result;
    auto conv_res = std::from_chars(sv.data(), sv.data() + sv.size(), result);
    vassert(
      conv_res.ec != std::errc::invalid_argument,
      "failed to convert {} to an unsigned long",
      sv);
    return result;
}

} // anonymous namespace

struct s3_imposter_fixture::content_handler {
    content_handler(
      const std::vector<s3_imposter_fixture::expectation>& exp,
      s3_imposter_fixture& imp,
      std::optional<absl::flat_hash_set<ss::sstring>> headers_to_store
      = std::nullopt)
      : fixture(imp)
      , headers(std::move(headers_to_store)) {
        for (const auto& e : exp) {
            expectations[e.url] = e;
        }
    }

    void insert(const std::vector<s3_imposter_fixture::expectation>& list) {
        for (const auto& exp : list) {
            expectations.insert(std::make_pair(exp.url, exp));
        }
    }

    void remove(const std::vector<ss::sstring>& urls) {
        for (const auto& u : urls) {
            expectations.erase(u);
        }
    }

    using const_req = const ss::http::request&;
    using reply = ss::http::reply;

    ss::sstring handle(const_req request, reply& repl) {
        static constexpr auto error_payload
          = R"xml(<?xml version="1.0" encoding="UTF-8"?>
                        <Error>
                            <Code>NoSuchKey</Code>
                            <Message>Object not found</Message>
                            <Resource>resource</Resource>
                            <RequestId>requestid</RequestId>
                        </Error>)xml";
        static constexpr auto slow_down_response
          = R"xml(<?xml version="1.0" encoding="UTF-8"?>
                            <Error>
                                <Code>SlowDown</Code>
                                <Message>Object not found</Message>
                                <Resource>resource</Resource>
                                <RequestId>requestid</RequestId>
                            </Error>)xml";
        http_test_utils::request_info ri(request);

        if (headers) {
            for (const auto& h : headers.value()) {
                ri.headers[h] = request.get_header(h);
            }
        }

        fixture._requests.push_back(ri);
        fixture._targets.insert(std::make_pair(ri.url, ri));
        vlog(
          fixt_log.trace,
          "S3 imposter request {} - {} - {}",
          request._url,
          request.content_length,
          request._method);

        if (auto maybe_resp = fixture.should_fail_request(ri);
            maybe_resp.has_value()) {
            vlog(
              fixt_log.debug,
              "S3 imposter request {} - {} - {} marked as failed, response {}",
              request._url,
              request.content_length,
              request._method,
              maybe_resp->status);
            repl.set_status(maybe_resp->status);
            return maybe_resp->body;
        }

        auto expect_iter = expectations.find(request._url);
        if (expect_iter != expectations.end() && expect_iter->second.slowdown) {
            repl.set_status(reply::status_type::service_unavailable);
            return slow_down_response;
        }

        if (request._method == "GET") {
            if (
              fixture._search_on_get_list
              && request.get_query_param("list-type") == "2") {
                auto prefix = request.get_query_param("prefix");
                auto delimiter = request.get_query_param("delimiter");
                auto max_keys_str = request.get_query_param("max-keys");
                auto continuation_token_str = request.get_query_param(
                  "continuation-token");
                std::optional<size_t> max_keys = (max_keys_str.empty())
                                                   ? std::optional<size_t>{}
                                                   : std::stoi(max_keys_str);
                std::optional<ss::sstring> continuation_token
                  = (continuation_token_str.empty())
                      ? std::optional<ss::sstring>{}
                      : continuation_token_str;
                vlog(
                  fixt_log.trace,
                  "S3 imposter list request {} - {} - {} - {} - {}",
                  prefix,
                  delimiter,
                  max_keys,
                  continuation_token,
                  request._method);
                return list_objects_resp(
                  expectations,
                  prefix,
                  delimiter,
                  max_keys,
                  continuation_token);
            }
            if (
              expect_iter == expectations.end()
              || !expect_iter->second.body.has_value()) {
                vlog(fixt_log.trace, "Reply GET request with error");
                repl.set_status(reply::status_type::not_found);
                return error_payload;
            }

            auto bytes_requested = request.get_header("Range");
            const auto& body = expect_iter->second.body.value();
            if (!bytes_requested.empty()) {
                auto byte_range = parse_byte_header(bytes_requested);
                return body.substr(
                  byte_range.first, byte_range.second - byte_range.first + 1);
            }
            return body;
        } else if (request._method == "PUT") {
            vlog(fixt_log.trace, "Received PUT request to {}", request._url);
            expectations[request._url] = {
              .url = request._url, .body = request.content};
            return "";
        } else if (request._method == "DELETE") {
            vlog(fixt_log.trace, "Received DELETE request to {}", request._url);
            // TODO (abhijat) - enable conditionally failing requests
            // instead of this hardcoding
            if (request._url == "/failme") {
                repl.set_status(reply::status_type::internal_server_error);
                return "";
            }

            if (
              expect_iter == expectations.end()
              || !expect_iter->second.body.has_value()) {
                vlog(fixt_log.trace, "Reply DELETE request with error");
                repl.set_status(reply::status_type::not_found);
                return error_payload;
            }

            repl.set_status(reply::status_type::no_content);
            expect_iter->second.body = std::nullopt;
            return "";
        } else if (request._method == "HEAD") {
            if (
              expect_iter == expectations.end()
              || !expect_iter->second.body.has_value()) {
                vlog(fixt_log.trace, "Reply HEAD request with error");
                repl.add_header("x-amz-request-id", "placeholder-id");
                repl.set_status(reply::status_type::not_found);
            } else {
                repl.add_header("ETag", "placeholder-etag");
                repl.add_header(
                  "Content-Length",
                  ssx::sformat("{}", expect_iter->second.body->size()));
                repl.set_status(reply::status_type::ok);
            }
            vlog(
              fixt_log.trace, "S3 imposter response: {}", repl.response_line());
            return "";
        } else if (
          request._method == "POST"
          && request.query_parameters.contains("delete")) {
            vlog(fixt_log.trace, "Received DELETE request to {}", request._url);
            if (
              expect_iter != expectations.end()
              && expect_iter->second.body.has_value()) {
                return expect_iter->second.body.value();
            }
            // Special handling for the batched Delete objects requests.
            auto keys_to_delete = keys_from_delete_objects_request(ri);
            vlog(
              fixt_log.trace,
              "Parsed batched DELETE request with {} keys",
              keys_to_delete.size());
            for (const auto& key : keys_to_delete) {
                const ss::sstring to_delete = fmt::format(
                  "/{}", key().string());
                auto expect_iter = expectations.find(to_delete);
                if (
                  expect_iter == expectations.end()
                  || !expect_iter->second.body.has_value()) {
                    // Missing objects are assumed to be not an error (e.g.
                    // caused by a delete retry).
                    vlog(
                      fixt_log.debug,
                      "Requested DELETE request of {}, not found",
                      to_delete);
                    continue;
                }
                expect_iter->second.body = std::nullopt;
                vlog(fixt_log.trace, "Batched DELETE request of {}", to_delete);
            }

            return R"xml(<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></DeleteResult>)xml";
        }
        RPTEST_ADD_FAIL("Unexpected request");
        return "";
    }
    expectation_map_t expectations;

    s3_imposter_fixture& fixture;
    std::optional<absl::flat_hash_set<ss::sstring>> headers = std::nullopt;
};

cloud_storage_clients::s3_configuration
s3_imposter_fixture::get_configuration() {
    net::unresolved_address server_addr(httpd_host_name, httpd_port_number());
    cloud_storage_clients::s3_configuration conf;
    conf.uri = cloud_storage_clients::access_point_uri(httpd_host_name);
    conf.access_key = cloud_roles::public_key_str("access-key");
    conf.secret_key = cloud_roles::private_key_str("secret-key");
    conf.region = cloud_roles::aws_region_name("us-east-1");
    conf.url_style = url_style;
    conf.server_addr = server_addr;
    conf._probe = ss::make_shared<cloud_storage_clients::client_probe>(
      net::metrics_disabled::yes,
      net::public_metrics_disabled::yes,
      cloud_roles::aws_region_name{"us-east-1"},
      cloud_storage_clients::endpoint_url{httpd_host_name});
    return conf;
}

s3_imposter_fixture::s3_imposter_fixture(
  cloud_storage_clients::s3_url_style url_style)
  : url_style(url_style)
  , conf(get_configuration()) {
    _server = ss::make_shared<ss::httpd::http_server_control>();
    _server->start().get();
    ss::ipv4_addr ip_addr = {httpd_ip_addr, httpd_port_number()};
    _server_addr = ss::socket_address(ip_addr);
}

s3_imposter_fixture::~s3_imposter_fixture() { _server->stop().get(); }

uint16_t s3_imposter_fixture::httpd_port_number() {
    return unit_test_httpd_port_number();
}

const std::vector<http_test_utils::request_info>&
s3_imposter_fixture::get_requests() const {
    return _requests;
}

std::vector<http_test_utils::request_info> s3_imposter_fixture::get_requests(
  s3_imposter_fixture::req_pred_t predicate) const {
    std::vector<http_test_utils::request_info> matching_requests;
    matching_requests.reserve(_requests.size());
    std::copy_if(
      _requests.cbegin(),
      _requests.cend(),
      std::back_inserter(matching_requests),
      std::move(predicate));
    return matching_requests;
}

const std::multimap<ss::sstring, http_test_utils::request_info>&
s3_imposter_fixture::get_targets() const {
    return _targets;
}

void s3_imposter_fixture::set_expectations_and_listen(
  std::vector<s3_imposter_fixture::expectation> expectations,
  std::optional<absl::flat_hash_set<ss::sstring>> headers_to_store) {
    const ss::sstring url_prefix = "/" + url_base();
    for (auto& expectation : expectations) {
        expectation.url.insert(
          expectation.url.begin(), url_prefix.begin(), url_prefix.end());
    }
    _server
      ->set_routes(
        [this, &expectations, headers_to_store = std::move(headers_to_store)](
          ss::httpd::routes& r) mutable {
            set_routes(r, expectations, std::move(headers_to_store));
        })
      .get();
    _server->listen(_server_addr).get();
}

void s3_imposter_fixture::add_expectations(
  std::vector<s3_imposter_fixture::expectation> expectations) {
    vassert(_content_handler != nullptr, "Imposter is not initialized");
    const ss::sstring url_prefix = "/" + url_base();
    for (auto& expectation : expectations) {
        expectation.url.insert(
          expectation.url.begin(), url_prefix.begin(), url_prefix.end());
    }
    _content_handler->insert(expectations);
}

void s3_imposter_fixture::remove_expectations(std::vector<ss::sstring> urls) {
    vassert(_content_handler != nullptr, "Imposter is not initialized");
    const ss::sstring url_prefix = "/" + url_base();
    for (auto& url : urls) {
        url.insert(url.begin(), url_prefix.begin(), url_prefix.end());
    }
    _content_handler->remove(urls);
}

std::optional<ss::sstring>
s3_imposter_fixture::get_object(const ss::sstring& url) const {
    auto it = _content_handler->expectations.find(url);
    if (it == _content_handler->expectations.end()) {
        return std::nullopt;
    }
    return it->second.body;
}

ss::sstring s3_imposter_fixture::url_base() const {
    switch (conf.url_style) {
    case cloud_storage_clients::s3_url_style::virtual_host:
        return fmt::format("");
    case cloud_storage_clients::s3_url_style::path:
        return fmt::format("{}/", bucket_name);
    }
}

void s3_imposter_fixture::set_routes(
  ss::httpd::routes& r,
  const std::vector<s3_imposter_fixture::expectation>& expectations,
  std::optional<absl::flat_hash_set<ss::sstring>> headers_to_store) {
    using namespace ss::httpd;
    using reply = ss::http::reply;
    _content_handler = ss::make_shared<content_handler>(
      expectations, *this, std::move(headers_to_store));
    _handler = std::make_unique<function_handler>(
      [this](const_req req, reply& repl) {
          return _content_handler->handle(req, repl);
      },
      "txt");
    r.add_default_handler(_handler.get());
}

void s3_imposter_fixture::fail_request_if(
  req_pred_t pred, http_test_utils::response response) {
    _fail_request_if.push_back(std::move(pred));
    _failure_response.push_back(std::move(response));
}

std::optional<http_test_utils::response>
s3_imposter_fixture::should_fail_request(
  const http_test_utils::request_info& ri) {
    for (size_t i = 0; i < _fail_request_if.size(); ++i) {
        if (_fail_request_if[i](ri)) {
            return _failure_response[i];
        }
    }
    return std::nullopt;
}

enable_cloud_storage_fixture::enable_cloud_storage_fixture() {
    ss::smp::invoke_on_all([]() {
        auto& cfg = config::shard_local_cfg();
        cfg.cloud_storage_enabled.set_value(true);
        cfg.cloud_storage_api_endpoint.set_value(
          std::optional<ss::sstring>{s3_imposter_fixture::httpd_host_name});
        cfg.cloud_storage_api_endpoint_port.set_value(
          static_cast<int16_t>(unit_test_httpd_port_number()));
        cfg.cloud_storage_access_key.set_value(
          std::optional<ss::sstring>{"access-key"});
        cfg.cloud_storage_secret_key.set_value(
          std::optional<ss::sstring>{"secret-key"});
        cfg.cloud_storage_region.set_value(
          std::optional<ss::sstring>{"us-east1"});
        cfg.cloud_storage_bucket.set_value(
          std::optional<ss::sstring>{random_test_bucket_name()()});
    }).get();
}

cloud_storage_clients::http_byte_range parse_byte_header(std::string_view s) {
    auto bytes_start = s.find('=');
    vassert(bytes_start != s.npos, "invalid byte range {}", s);
    std::string_view bytes_value = s.substr(bytes_start + 1);

    auto split_at = bytes_value.find('-');
    vassert(split_at != bytes_value.npos, "invalid byte range {}", bytes_value);

    return std::make_pair(
      string_view_to_ul(bytes_value.substr(0, split_at)),
      string_view_to_ul(bytes_value.substr(split_at + 1)));
}

std::vector<cloud_storage_clients::object_key>
keys_from_delete_objects_request(const http_test_utils::request_info& req) {
    std::vector<cloud_storage_clients::object_key> keys;

    auto buffer_stream = std::istringstream{std::string{req.content}};
    boost::property_tree::ptree tree;
    boost::property_tree::read_xml(buffer_stream, tree);

    for (const auto& v : tree.get_child("Delete")) {
        if (v.first == "Object") {
            keys.emplace_back(v.second.get<std::string>("Key"));
        }
    }

    return keys;
}
