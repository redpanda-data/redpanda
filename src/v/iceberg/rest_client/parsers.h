/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "iceberg/rest_client/types.h"
#include "json/document.h"
#include "json/schema.h"

namespace iceberg::rest_client {

// Extracts a readable/loggable error from a json schema validator which has
// failed.
parse_error_msg get_schema_validation_error(const json::SchemaValidator& v);

// Parses oauth token from a JSON response sent from catalog server
expected<oauth_token> parse_oauth_token(json::Document&& doc);

} // namespace iceberg::rest_client
