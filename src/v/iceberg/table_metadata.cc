/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "iceberg/table_metadata.h"

#include "iceberg/compatibility_utils.h"

namespace iceberg {
const schema*
table_metadata::get_equivalent_schema(const struct_type& type) const {
    auto schemas_reversed = std::ranges::reverse_view(schemas);
    auto it = std::ranges::find_if(
      schemas_reversed,
      [&type](const iceberg::struct_type& source) {
          return iceberg::schemas_equivalent(source, type);
      },
      &iceberg::schema::schema_struct);
    return it != schemas_reversed.end() ? &*it : nullptr;
}
} // namespace iceberg
