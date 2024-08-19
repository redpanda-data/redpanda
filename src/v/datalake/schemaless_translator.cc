/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/schemaless_translator.h"

#include "iceberg/datatypes.h"
#include "iceberg/values.h"

namespace datalake {

iceberg::struct_value schemaless_translator::translate_event(
  iobuf key, iobuf value, int64_t timestamp, int64_t offset) const {
    using namespace iceberg;
    struct_value res;
    res.fields.emplace_back(long_value(offset));
    res.fields.emplace_back(timestamp_value(timestamp));
    res.fields.emplace_back(string_value(std::move(key)));
    res.fields.emplace_back(string_value(std::move(value)));
    return res;
}

iceberg::struct_type schemaless_translator::get_schema() const {
    using namespace iceberg;
    struct_type res;
    res.fields.emplace_back(nested_field::create(
      1, "redpanda_offset", field_required::yes, long_type{}));
    res.fields.emplace_back(nested_field::create(
      2, "redpanda_timestamp", field_required::yes, timestamp_type{}));
    res.fields.emplace_back(nested_field::create(
      3, "redpanda_key", field_required::no, string_type{}));
    res.fields.emplace_back(nested_field::create(
      4, "redpanda_value", field_required::no, string_type{}));

    return res;
}

} // namespace datalake
