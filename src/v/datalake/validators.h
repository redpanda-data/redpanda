/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/seastarx.h"

#include <seastar/core/sstring.hh>

#include <ada.h>

namespace datalake {

/// Parses and validates an `iceberg_rest_catalog_endpoint` value.
/// Returns the parsed URL on success, or an error message describing why
/// parsing failed.
tl::expected<ada::url_aggregator, ss::sstring>
parse_iceberg_rest_catalog_endpoint(const ss::sstring& url_str);

} // namespace datalake
