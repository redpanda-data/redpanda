/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/parquet_write_config.h"

namespace datalake {

parquet_write_config parquet_write_config::from_properties(
  const std::optional<iceberg::table_properties_t>&) {
    return {};
}

} // namespace datalake
