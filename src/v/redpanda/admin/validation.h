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

/**
 * A helper to apply a schema validator to a request and on error,
 * string-ize any schema errors in the 400 response to help
 * caller see what went wrong.
 */
void apply_validator(
  json::validator& validator, const json::Document::ValueType& doc);

} // namespace admin
