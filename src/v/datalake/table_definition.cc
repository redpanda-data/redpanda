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

namespace datalake {

iceberg::struct_type schemaless_struct_type() {
    return schemaless_desc::build();
}

iceberg::schema default_schema() {
    return iceberg::schema{
      .schema_struct = schemaless_struct_type(),
      .schema_id = iceberg::schema::default_id,
      .identifier_field_ids = {},
    };
}

namespace {

std::optional<iceberg::value>
build_headers_value(const chunked_vector<model::record_header>& headers) {
    if (headers.empty()) {
        return std::nullopt;
    }
    auto hdr_list = std::make_unique<iceberg::list_value>();
    for (const auto& hdr : headers) {
        auto kv = header_kv_desc::build_value(
          val<"key">(
            hdr.key_size() >= 0 ? std::make_optional<iceberg::value>(
                                    iceberg::string_value(hdr.key().copy()))
                                : std::nullopt),
          val<"value">(
            hdr.value_size() >= 0 ? std::make_optional<iceberg::value>(
                                      iceberg::binary_value(hdr.value().copy()))
                                  : std::nullopt));
        hdr_list->elements.emplace_back(std::move(kv));
    }
    return hdr_list;
}

} // namespace

std::unique_ptr<iceberg::struct_value> build_rp_struct(
  model::partition_id pid,
  kafka::offset o,
  std::optional<iobuf> key,
  model::timestamp ts,
  model::timestamp_type ts_t,
  const chunked_vector<model::record_header>& headers) {
    return rp_desc::build_value(
      val<"partition">(iceberg::int_value(pid)),
      val<"offset">(iceberg::long_value(o)),
      val<"timestamp">(iceberg::timestamptz_value(ts.value() * 1000)),
      val<"headers">(build_headers_value(headers)),
      val<"key">(
        key ? std::make_optional<iceberg::value>(
                iceberg::binary_value(std::move(*key)))
            : std::nullopt),
      val<"timestamp_type">(iceberg::int_value{static_cast<int32_t>(ts_t)}));
}

} // namespace datalake
