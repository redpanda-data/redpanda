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

#include "iceberg/datatypes.h"
#include "serde/parquet/schema.h"

namespace datalake {
/**
 * Translates an Iceberg schema to a parquet schema that is required to write
 * parquet files. This function do not do any validation of the schema as it
 * assumes that the schema was validated before.
 */
serde::parquet::schema_element schema_to_parquet(const iceberg::struct_type&);
} // namespace datalake
