/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "pandaproxy/parsing/exceptions.h"
#include "pandaproxy/parsing/from_chars.h"

#include <seastar/http/request.hh>

namespace pandaproxy::parse {

namespace detail {

template<typename T>
T parse_param(std::string_view type, std::string_view key, ss::sstring value) {
    if (value.empty()) {
        throw error(
          error_code::empty_param,
          fmt::format("Missing mandatory {} '{}'", type, key));
    }
    auto res = parse::from_chars<T>{}(value);
    if (res.has_error()) {
        throw error(
          error_code::invalid_param,
          fmt::format("Invalid {} '{}' got '{}'", type, key, value));
    }
    return res.value();
}

} // namespace detail

template<typename T>
T header(const ss::httpd::request& req, const ss::sstring& name) {
    return detail::parse_param<T>("header", name, req.get_header(name));
}

template<typename T>
T request_param(const ss::httpd::request& req, const ss::sstring& name) {
    return detail::parse_param<T>("parameter", name, req.param[name]);
}

template<typename T>
T query_param(const ss::httpd::request& req, const ss::sstring& name) {
    return detail::parse_param<T>("parameter", name, req.get_query_param(name));
}

} // namespace pandaproxy::parse
