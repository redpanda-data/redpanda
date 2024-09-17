/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "json/validator.h"

#include <seastar/http/request.hh>
#include <seastar/json/json_elements.hh>

#pragma once

namespace admin {
/**
 * A helper to apply a schema validator to a request and on error,
 * string-ize any schema errors in the 400 response to help
 * caller see what went wrong.
 */
void apply_validator(
  json::validator& validator, const json::Document::ValueType& doc);

/**
 * Helper for requests with boolean URL query parameters that should
 * be treated as false if absent, or true if "true" (case insensitive) or "1"
 */
bool get_boolean_query_param(
  const ss::http::request& req, std::string_view name);

template<typename T>
ss::json::json_return_type write_via_body_writer(T response) {
    auto f = [&]() -> std::function<ss::future<>(ss::output_stream<char>&&)> {
        return [respo = std::move(response)](
                 ss::output_stream<char>&& stream) mutable -> ss::future<> {
            return ss::do_with(
              std::move(stream), std::move(respo), [](auto& stream, auto resp) {
                  return ss::json::formatter::write(stream, std::move(resp))
                    .then([&stream]() { return stream.close(); });
              });
        };
    };

    return f();
};

} // namespace admin
