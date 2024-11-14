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

#include "datalake/conversion_outcome.h"

#include <serde/parquet/value.h>

namespace datalake {
using parquet_conversion_outcome
  = checked<serde::parquet::value, value_conversion_exception>;

/**
 * Converts iceberg to parquet value, the value conversion only involves
 * physical type, the logical type is not considered. The corresponding logical
 * type can be determined from corresponding schema.
 */
ss::future<parquet_conversion_outcome> to_parquet_value(iceberg::value);

} // namespace datalake
