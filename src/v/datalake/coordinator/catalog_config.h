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
#include "iceberg/field_name_comparison.h"

namespace datalake::coordinator {

/// Returns true if the configured Iceberg catalog is AWS Glue (identified
/// by a REST catalog with SigV4 authentication targeting the "glue" service).
bool using_glue_catalog();

/// Resolves whether schema field name comparison should be case-insensitive
/// based on the cluster-level iceberg_schema_case_insensitive config.
/// "auto" resolves to yes when the catalog is AWS Glue, no otherwise.
iceberg::field_name_comparison resolve_field_name_comparison();

} // namespace datalake::coordinator
