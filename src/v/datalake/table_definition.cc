/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/table_definition.h"

#include "iceberg/transform.h"

namespace datalake {
using namespace iceberg;
struct_type schemaless_struct_type() {
    using namespace iceberg;
    struct_type system_fields;
    system_fields.fields.emplace_back(
      nested_field::create(2, "offset", field_required::yes, long_type{}));
    system_fields.fields.emplace_back(nested_field::create(
      3, "timestamp", field_required::yes, timestamp_type{}));
    system_fields.fields.emplace_back(
      nested_field::create(4, "key", field_required::no, binary_type{}));
    system_fields.fields.emplace_back(
      nested_field::create(5, "value", field_required::no, binary_type{}));
    struct_type res;
    res.fields.emplace_back(nested_field::create(
      1,
      ss::sstring{rp_struct_name},
      field_required::yes,
      std::move(system_fields)));

    return res;
}

schema default_schema() {
    return {
      .schema_struct = schemaless_struct_type(),
      .schema_id = iceberg::schema::id_t{0},
      .identifier_field_ids = {},
    };
}

partition_spec hour_partition_spec() {
    chunked_vector<partition_field> fields;
    fields.emplace_back(partition_field{
      .source_id = nested_field::id_t{3},
      .field_id = partition_field::id_t{1000},
      .name = "redpanda_timestamp_hour",
      .transform = hour_transform{},
    });
    return partition_spec{
      .spec_id = partition_spec::id_t{0},
      .fields = std::move(fields),
    };
}

} // namespace datalake
