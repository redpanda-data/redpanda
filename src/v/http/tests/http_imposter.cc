/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "http/tests/http_imposter.h"

#include "base/vlog.h"
#include "utils/uuid.h"

#include <seastar/http/function_handlers.hh>

#include <utility>

static ss::logger http_imposter_log("http_imposter"); // NOLINT

http_imposter_fixture::http_imposter_fixture(uint16_t port)
  : _port(port)
  , _server_addr{ss::ipv4_addr{httpd_ip_addr.data(), httpd_port_number()}}
  , _address{
      {httpd_host_name.data(), httpd_host_name.size()}, httpd_port_number()} {
    _id = fmt::format("{}", uuid_t::create());
    _server.start().get();
}

http_imposter_fixture::~http_imposter_fixture() { _server.stop().get(); }

uint16_t http_imposter_fixture::httpd_port_number() { return _port; }

void http_imposter_fixture::start_request_masking(
  http_test_utils::response canned_response,
  ss::lowres_clock::duration duration) {
    _masking_active = {canned_response, duration, ss::lowres_clock::now()};
}

const std::vector<http_test_utils::request_info>&
http_imposter_fixture::get_requests() const {
    return _requests;
}

static ss::sstring remove_query_params(std::string_view url) {
    return ss::sstring{url.substr(0, url.find('?'))};
}

std::optional<std::reference_wrapper<const http_test_utils::request_info>>
http_imposter_fixture::get_latest_request(
  const ss::sstring& url, bool ignore_url_params) const {
    auto i = std::ranges::upper_bound(
      _targets, url, std::less<>{}, [=](auto const& url_ri) {
          return ignore_url_params ? remove_query_params(url_ri.first)
                                   : url_ri.first;
      });
    if (i == _targets.begin()) {
        return std::nullopt;
    } else {
        --i;
        return std::ref(i->second);
    }
}

size_t http_imposter_fixture::get_request_count(const ss::sstring& url) const {
    auto [begin, end] = get_targets().equal_range(url);
    size_t len = std::distance(begin, end);
    return len;
}

const std::multimap<ss::sstring, http_test_utils::request_info>&
http_imposter_fixture::get_targets() const {
    return _targets;
}

void http_imposter_fixture::listen() {
    _server.set_routes([this](ss::httpd::routes& r) { set_routes(r); }).get();
    _server.listen(_server_addr).get();
    vlog(http_imposter_log.trace, "HTTP imposter {} started", _id);
}

void http_imposter_fixture::set_routes(ss::httpd::routes& r) {
    using namespace ss::httpd;
    _handler = std::make_unique<function_handler>(
      [this](const_req req, ss::http::reply& repl) -> ss::sstring {
          if (_masking_active) {
              if (
                ss::lowres_clock::now() - _masking_active->started
                > _masking_active->duration) {
                  _masking_active.reset();
              } else {
                  repl.set_status(_masking_active->canned_response.status);
                  return _masking_active->canned_response.body;
              }
          }

          http_test_utils::request_info ri(req);
          for (const auto& [k, v] : req._headers) {
              ri.headers[k] = v;
          }
          _requests.push_back(ri);
          _targets.insert(std::make_pair(ri.url, ri));

          const auto& fp = _fail_requests_when;
          for (size_t i = 0; i < fp.size(); ++i) {
              if (fp[i](req)) {
                  auto response = _fail_responses[i];
                  repl.set_status(response.status);
                  vlog(
                    http_imposter_log.trace,
                    "HTTP imposter id {} failing request {} - {} - {} with "
                    "response: {}",
                    _id,
                    req._url,
                    req.content_length,
                    req._method,
                    response);
                  return response.body;
              }
          }

          vlog(
            http_imposter_log.trace,
            "HTTP imposter id {} request {} - {} - {}",
            _id,
            req._url,
            req.content_length,
            req._method);

          if (req._method == "PUT" && req._url == imdsv2_token_url) {
              repl.set_status(ss::http::reply::status_type::ok);
              return "IMDSv2-TOKEN";
          } else if (req._method == "PUT") {
              when().request(req._url).then_reply_with(req.content);
              repl.set_status(ss::http::reply::status_type::ok);
              return "";
          } else if (req._method == "DELETE") {
              repl.set_status(ss::http::reply::status_type::no_content);
              return "";
          } else if (
            req._method == "POST" && req.query_parameters.contains("delete")) {
              // Delete objects
              return R"xml(<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></DeleteResult>)xml";
          } else {
              auto lookup_r = ri;
              lookup_r.url = remove_query_params(req._url);

              auto response = lookup(lookup_r);
              repl.set_status(response.status);
              for (const auto& [k, v] : response.headers) {
                  repl.add_header(k, v);
              }

              return response.body;
          }
      },
      "txt");
    r.add_default_handler(_handler.get());
}

bool http_imposter_fixture::has_call(
  std::string_view url, bool ignore_params) const {
    return std::ranges::find_if(
             _requests,
             [&](http_test_utils::request_info const& ri) {
                 return url
                        == (ignore_params ? remove_query_params(ri.url) : ri.url);
             })
           != _requests.end();
}

void http_imposter_fixture::fail_request_if(
  http_imposter_fixture::request_predicate predicate,
  http_test_utils::response response) {
    _fail_requests_when.push_back(std::move(predicate));
    _fail_responses[_fail_requests_when.size() - 1] = std::move(response);
}

void http_imposter_fixture::reset_http_call_state() {
    _requests.clear();
    _targets.clear();
}

void http_imposter_fixture::log_requests() const {
    for (const auto& req : _requests) {
        vlog(
          http_imposter_log.info,
          "{}: {} - {} ({} bytes)",
          _id,
          req.method,
          req.url,
          req.content_length);
    }
}
