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

#include "container/chunked_vector.h"
#include "iceberg/json_writer.h"
#include "iceberg/partition.h"
#include "json/document.h"

namespace iceberg {

partition_field parse_partition_field(const json::Value&);
chunked_vector<partition_field>
parse_partition_fields(const json::Value::ConstArray&);
partition_spec parse_partition_spec(const json::Value&);

} // namespace iceberg

namespace json {

void rjson_serialize(
  iceberg::json_writer& w, const iceberg::partition_field& m);
void rjson_serialize(
  iceberg::json_writer& w,
  const chunked_vector<iceberg::partition_field>& fields);
void rjson_serialize(iceberg::json_writer& w, const iceberg::partition_spec& m);

} // namespace json
