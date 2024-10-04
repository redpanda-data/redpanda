/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "http/request_builder.h"

#include <boost/algorithm/string/join.hpp>

namespace bh = boost::beast::http;

namespace http {

request_builder& request_builder::host(std::string_view host) {
    _url = ada::parse<ada::url_aggregator>(host);
    if (!_url) {
        _error = fmt::format("failed to parse host: {}", host);
    }
    return *this;
}

request_builder& request_builder::target(std::string_view target) {
    _target.emplace(target);
    return *this;
}

request_builder& request_builder::method(bh::verb verb) {
    _request.method(verb);
    return *this;
}

request_builder&
request_builder::query_param_kv(std::string_view key, std::string_view value) {
    _query_params_kv.emplace(key, value);
    return *this;
}

request_builder& request_builder::query_param(std::string_view q) {
    _query_params.emplace(q);
    return *this;
}

request_builder& request_builder::query_params_kv(
  absl::flat_hash_map<std::string_view, std::string_view> query_params) {
    _query_params_kv.insert(query_params.begin(), query_params.end());
    return *this;
}

request_builder& request_builder::query_params(
  absl::flat_hash_set<std::string_view> query_params) {
    _query_params.insert(query_params.begin(), query_params.end());
    return *this;
}

request_builder&
request_builder::header(std::string_view key, std::string_view value) {
    _request.set(key, value);
    return *this;
}

request_builder& request_builder::headers(
  absl::flat_hash_map<std::string_view, std::string_view> headers) {
    for (const auto& [k, v] : headers) {
        _request.set(k, v);
    }
    return *this;
}

request_builder& request_builder::with_size_of(const iobuf& b) {
    _request.set(bh::field::content_length, fmt::format("{}", b.size_bytes()));
    return *this;
}

request_builder& request_builder::with_bearer_auth(std::string_view token) {
    _request.set(bh::field::authorization, fmt::format("Bearer {}", token));
    return *this;
}

request_builder&
request_builder::with_content_type(std::string_view content_type) {
    _request.set(bh::field::content_type, content_type);
    return *this;
}

request_builder::expected request_builder::build() {
    if (!_url) {
        return tl::unexpected(_error);
    }

    if (_target.has_value()) {
        _url->set_pathname(_target.value());
    }

    std::vector<ss::sstring> qparams;
    for (const auto& [k, v] : _query_params_kv) {
        qparams.emplace_back(fmt::format("{}={}", k, v));
    }

    qparams.insert(qparams.end(), _query_params.begin(), _query_params.end());
    if (auto search = boost::algorithm::join(qparams, "&"); !search.empty()) {
        _url->set_search(search);
    }

    _request.set(bh::field::host, _url->get_host());
    _request.target(
      fmt::format("{}{}", _url->get_pathname(), _url->get_search()));

    return _request;
}

} // namespace http
