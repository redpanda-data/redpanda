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

#include "datalake/table_definition.h"
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
    return schemaless_struct_type();
}

} // namespace datalake
