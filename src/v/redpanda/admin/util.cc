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
  json::validator& validator, json::Document::ValueType const& doc) {
    try {
        json::validate(validator, doc);
    } catch (json::json_validation_error& err) {
        throw ss::httpd::bad_request_exception(fmt::format(
          "JSON request body does not conform to schema: {}", err.what()));
    }
}
} // namespace admin
