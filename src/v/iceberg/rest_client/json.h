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

#include "iceberg/rest_client/oauth_token.h"
#include "json/document.h"

namespace iceberg::rest_client {

// Parses oauth token from a JSON response sent from catalog server
oauth_token parse_oauth_token(const json::Document& doc);

} // namespace iceberg::rest_client
