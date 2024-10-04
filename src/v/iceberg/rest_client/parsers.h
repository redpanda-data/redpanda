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

#pragma once

#include "iceberg/rest_client/types.h"
#include "json/document.h"
#include "json/schema.h"

namespace iceberg::rest_client {

// TODO move to json_utils?
// Extracts a readable/loggable error from a json schema validator which has
// failed.
ss::sstring get_schema_validation_error(const json::SchemaValidator& v);

// Parses oauth token from a JSON response sent from catalog server
expected<oauth_token> parse_oauth_token(json::Document&& doc);

} // namespace iceberg::rest_client
