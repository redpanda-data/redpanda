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
 * Helper for decoding path parameters.
 *
 * It is a slightly modified version of ss::http::internal::url_decode
 * which only differs in that it correctly does not replace +
 * with ' ' in the input. It only replaces percent encoded values.
 */
bool path_decode(const std::string_view in, ss::sstring& out);

} // namespace admin
