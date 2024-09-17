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

#include "redpanda/admin/util.h"

#include <seastar/http/exception.hh>

namespace admin {
void apply_validator(
  json::validator& validator, const json::Document::ValueType& doc) {
    try {
        json::validate(validator, doc);
    } catch (const json::json_validation_error& err) {
        throw ss::httpd::bad_request_exception(fmt::format(
          "JSON request body does not conform to schema: {}", err.what()));
    }
}

bool get_boolean_query_param(
  const ss::http::request& req, std::string_view name) {
    auto key = ss::sstring(name);
    if (!req.query_parameters.contains(key)) {
        return false;
    }

    const ss::sstring& str_param = req.query_parameters.at(key);
    return ss::internal::case_insensitive_cmp()(str_param, "true")
           || str_param == "1";
}

} // namespace admin
