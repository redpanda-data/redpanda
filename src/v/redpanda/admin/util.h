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

#pragma once

namespace admin {

template<typename C>
class lw_shared_container {
public:
    using iterator = C::iterator;
    using value_type = C::value_type;

    explicit lw_shared_container(C&& c)
      : c_{ss::make_lw_shared<C>(std::move(c))} {}

    iterator begin() const { return c_->begin(); }
    iterator end() const { return c_->end(); }

private:
    ss::lw_shared_ptr<C> c_;
};

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

} // namespace admin
