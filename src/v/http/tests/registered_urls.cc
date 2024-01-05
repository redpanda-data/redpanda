/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "http/tests/registered_urls.h"

#include "base/vassert.h"

namespace {
const auto not_found = http_test_utils::response{
  .body = "not found", .status = ss::http::reply::status_type::not_found};

constexpr std::string_view default_content = "DEFAULT";
constexpr std::string_view default_response = "canned-response";
} // namespace

namespace http_test_utils {
void registered_urls::add_mapping::add_mapping_when::then_reply_with(
  ss::sstring content) {
    then_reply_with(std::move(content), ss::http::reply::status_type::ok);
}

void registered_urls::add_mapping::add_mapping_when::then_reply_with(
  ss::http::reply::status_type status) {
    then_reply_with(default_response.data(), status);
}

void registered_urls::add_mapping::add_mapping_when::then_reply_with(
  ss::sstring content, ss::http::reply::status_type status) {
    if (!r.contains(url)) {
        r[url] = method_reply_map{};
    }

    if (!r[url].contains(method)) {
        r[url][method] = content_reply_map{};
    }

    r[url][method][request_content] = response{
      .body = std::move(content), .status = status};
}

void registered_urls::add_mapping::add_mapping_when::then_reply_with(
  std::vector<std::pair<ss::sstring, ss::sstring>> headers,
  ss::http::reply::status_type status) {
    if (!r.contains(url)) {
        r[url] = method_reply_map{};
    }

    if (!r[url].contains(method)) {
        r[url][method] = content_reply_map{};
    }

    r[url][method][request_content] = response{
      .headers = std::move(headers), .status = status};
}

registered_urls::add_mapping::add_mapping_when&
registered_urls::add_mapping::add_mapping_when::with_method(
  ss::httpd::operation_type m) {
    method = m;
    return *this;
}

registered_urls::add_mapping::add_mapping_when&
registered_urls::add_mapping::add_mapping_when::with_content(
  ss::sstring content) {
    request_content = std::move(content);
    return *this;
}

registered_urls::add_mapping::add_mapping_when registered_urls::request(
  ss::sstring url,
  ss::httpd::operation_type method,
  ss::sstring request_content) {
    return add_mapping::add_mapping_when{
      .r = request_response_map,
      .url = std::move(url),
      .method = method,
      .request_content = std::move(request_content)};
}

registered_urls::add_mapping::add_mapping_when
registered_urls::request(ss::sstring url, ss::httpd::operation_type method) {
    return request(std::move(url), method, default_content.data());
}

registered_urls::add_mapping::add_mapping_when
registered_urls::request(ss::sstring url) {
    return request(std::move(url), ss::httpd::GET, default_content.data());
}

response registered_urls::lookup(const request_info& req) const {
    auto url = req.url;
    auto it = request_response_map.find(url);
    if (it == request_response_map.end()) {
        return not_found;
    }

    auto method_mapping = it->second;
    auto m_it = method_mapping.find(ss::httpd::str2type(req.method));
    if (m_it == method_mapping.end()) {
        return not_found;
    }

    auto content_mapping = m_it->second;
    auto content = req.content;
    if (auto c_it = content_mapping.find(content);
        c_it != content_mapping.end()) {
        return c_it->second;
    }

    vassert(
      content_mapping.contains(default_content.data()),
      "The request map is not set with a default content key: {}",
      default_content);
    return content_mapping[default_content.data()];
}

std::ostream& operator<<(std::ostream& os, const response& resp) {
    fmt::print(
      os,
      "{{status: {}, body: {}}}",
      static_cast<uint>(resp.status),
      resp.body);
    return os;
}

} // namespace http_test_utils
