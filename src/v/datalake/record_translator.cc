/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/record_translator.h"

#include "absl/container/flat_hash_set.h"
#include "base/vlog.h"
#include "datalake/logger.h"
#include "datalake/record_schema_resolver.h"
#include "datalake/table_definition.h"
#include "iceberg/avro_utils.h"
#include "iceberg/compatibility_utils.h"
#include "iceberg/conversion/conversion_outcome.h"
#include "iceberg/conversion/values_avro.h"
#include "iceberg/conversion/values_json.h"
#include "iceberg/conversion/values_protobuf.h"
#include "iceberg/datatypes.h"
#include "iceberg/values.h"
#include "model/fundamental.h"
#include "model/record.h"

#include <avro/Generic.hh>
#include <avro/GenericDatum.hh>

namespace datalake {

namespace {

struct value_translating_visitor {
    // Buffer ready to be parsed, e.g. no schema ID or protobuf offsets.
    iobuf parsable_buf;
    const iceberg::field_type& type;

    ss::future<iceberg::optional_value_outcome>
    operator()(const google::protobuf::Descriptor& d) {
        return iceberg::deserialize_protobuf(std::move(parsable_buf), d);
    }
    ss::future<iceberg::optional_value_outcome>
    operator()(const avro::ValidSchema& s) {
        auto value = co_await iceberg::deserialize_avro(
          std::move(parsable_buf), s);
        if (value.has_error()) {
            co_return iceberg::optional_value_outcome(value.error());
        }
        co_return std::move(value.value());
    }

    ss::future<iceberg::optional_value_outcome>
    operator()(const iceberg::json_conversion_ir& s) {
        auto value = co_await iceberg::deserialize_json(
          std::move(parsable_buf), s);
        if (value.has_error()) {
            co_return iceberg::optional_value_outcome(value.error());
        }
        co_return std::move(value.value());
    }
};

std::optional<size_t> get_redpanda_idx(const iceberg::struct_type& val_type) {
    for (size_t i = 0; i < val_type.fields.size(); ++i) {
        if (val_type.fields[i]->name == rp_struct_name) {
            return i;
        }
    }
    return std::nullopt;
}

// Builds a struct value meant to be used as the base of the "redpanda" struct.
// Additional fields specific to the mode (e.g. "value" for key-value mode) may
// be appended to the end.
std::unique_ptr<iceberg::struct_value> build_rp_struct(
  model::partition_id pid,
  kafka::offset o,
  std::optional<iobuf> key,
  model::timestamp ts,
  model::timestamp_type ts_t,
  const chunked_vector<model::record_header>& headers) {
    auto system_data = std::make_unique<iceberg::struct_value>();
    system_data->fields.reserve(6);

    system_data->fields.emplace_back(iceberg::int_value(pid));
    system_data->fields.emplace_back(iceberg::long_value(o));
    // NOTE: Kafka uses milliseconds, Iceberg uses microseconds.
    system_data->fields.emplace_back(
      iceberg::timestamptz_value(ts.value() * 1000));

    if (headers.empty()) {
        system_data->fields.emplace_back(std::nullopt);
    } else {
        auto headers_list = std::make_unique<iceberg::list_value>();
        for (const auto& hdr : headers) {
            auto header_kv_struct = std::make_unique<iceberg::struct_value>();
            header_kv_struct->fields.emplace_back(
              hdr.key_size() >= 0 ? std::make_optional<iceberg::value>(
                                      iceberg::string_value(hdr.key().copy()))
                                  : std::nullopt);
            header_kv_struct->fields.emplace_back(
              hdr.value_size() >= 0
                ? std::make_optional<iceberg::value>(
                    iceberg::binary_value(hdr.value().copy()))
                : std::nullopt);
            headers_list->elements.emplace_back(std::move(header_kv_struct));
        }
        system_data->fields.emplace_back(std::move(headers_list));
    }

    system_data->fields.emplace_back(
      key ? std::make_optional<iceberg::value>(
              iceberg::binary_value(std::move(*key)))
          : std::nullopt);

    system_data->fields.emplace_back(
      iceberg::int_value{static_cast<int32_t>(ts_t)});

    return system_data;
}

} // namespace

std::ostream& operator<<(std::ostream& o, const record_translator::errc& e) {
    switch (e) {
    case record_translator::errc::translation_error:
        return o << "record_translator::errc::translation_error";
    case record_translator::errc::unexpected_schema:
        return o << "record_translator::errc::unexpected_schema";
    }
}

record_type
default_translator::build_type(std::optional<shared_resolved_type_t> val_type) {
    if (val_type.has_value()) {
        return structured_translator.build_type(std::move(val_type));
    }
    return kv_translator.build_type(std::move(val_type));
}

ss::future<checked<iceberg::struct_value, record_translator::errc>>
default_translator::translate_data(
  model::partition_id pid,
  kafka::offset o,
  std::optional<iobuf> key,
  const std::optional<shared_resolved_type_t>& val_type,
  std::optional<iobuf> parsable_val,
  model::timestamp ts,
  model::timestamp_type ts_t,
  const chunked_vector<model::record_header>& headers) {
    if (val_type.has_value()) {
        co_return co_await structured_translator.translate_data(
          pid,
          o,
          std::move(key),
          val_type,
          std::move(parsable_val),
          ts,
          ts_t,
          headers);
    }
    co_return co_await kv_translator.translate_data(
      pid,
      o,
      std::move(key),
      val_type,
      std::move(parsable_val),
      ts,
      ts_t,
      headers);
}

record_type
key_value_translator::build_type(std::optional<shared_resolved_type_t>) {
    auto ret_type = schemaless_struct_type();
    ret_type.fields.emplace_back(
      iceberg::nested_field::create(
        11, "value", iceberg::field_required::no, iceberg::binary_type{}));
    return record_type{
      .comps = record_schema_components{
          .key_identifier = std::nullopt,
          .val_identifier = std::nullopt,
      },
      .type = std::move(ret_type),
    };
}

ss::future<checked<iceberg::struct_value, record_translator::errc>>
key_value_translator::translate_data(
  model::partition_id pid,
  kafka::offset o,
  std::optional<iobuf> key,
  const std::optional<shared_resolved_type_t>& val_type,
  std::optional<iobuf> parsable_val,
  model::timestamp ts,
  model::timestamp_type ts_t,
  const chunked_vector<model::record_header>& headers) {
    if (val_type.has_value()) {
        vlog(
          datalake_log.error,
          "Must not have parsed schema when using key-value mode");
        co_return record_translator::errc::unexpected_schema;
    }
    auto ret_data = iceberg::struct_value{};

    auto system_data = build_rp_struct(
      pid, o, std::move(key), ts, ts_t, headers);
    ret_data.fields.emplace_back(std::move(system_data));
    ret_data.fields.emplace_back(
      parsable_val ? std::make_optional<iceberg::value>(
                       iceberg::binary_value(std::move(*parsable_val)))
                   : std::nullopt);
    co_return ret_data;
}

record_type structured_data_translator::build_type(
  std::optional<shared_resolved_type_t> val_type) {
    auto ret_type = schemaless_struct_type();
    std::optional<schema_identifier> val_id;
    if (val_type.has_value()) {
        val_id = val_type.value()->id;
        auto struct_type = std::get<iceberg::struct_type>(
          iceberg::make_copy(val_type.value()->type));
        // The various schema languages differ significantly in their semantics
        // and best practices around required fields, and Iceberg has its own.
        // By forcing all schema fields to non-required, we provide a maximally
        // permissive allowance for schema evolution which is certainly a
        // superset superset of what any particular schema language allows.
        // TODO(iceberg): this behavior could be made configurable
        //
        // Keys must be marked as required per the Iceberg spec:
        // https://iceberg.apache.org/spec/#nested-types.
        // This approach of storing the `key` fields in a set below is
        // guaranteed to work because `iceberg::for_each_field()` is a depth
        // first traversal of the fields, i.e., the parent `map` field will be
        // visited before the child `key` field.
        absl::flat_hash_set<iceberg::nested_field*> map_keys;
        std::ignore = iceberg::for_each_field(
          struct_type, [&map_keys](iceberg::nested_field* f) {
              f->required = map_keys.contains(f) ? iceberg::field_required::yes
                                                 : iceberg::field_required::no;

              if (std::holds_alternative<iceberg::map_type>(f->type)) {
                  auto& kv = std::get<iceberg::map_type>(f->type);
                  map_keys.insert(kv.key_field.get());
              }
          });
        for (auto& field : struct_type.fields) {
            if (field->name == rp_struct_name) {
                // To avoid collisions, move user fields named "redpanda" into
                // the nested "redpanda" system field.
                auto& system_fields = std::get<iceberg::struct_type>(
                  ret_type.fields[0]->type);
                // Use the next id of the system defaults.
                system_fields.fields.emplace_back(
                  iceberg::nested_field::create(
                    10, "data", field->required, std::move(field->type)));
                continue;
            }
            // Add the extra user-defined fields.
            ret_type.fields.emplace_back(std::move(field));
        }
    }
    return record_type{
      .comps = record_schema_components{
          .key_identifier = std::nullopt,
          .val_identifier = std::move(val_id),
      },
      .type = std::move(ret_type),
    };
}

ss::future<checked<iceberg::struct_value, record_translator::errc>>
structured_data_translator::translate_data(
  model::partition_id pid,
  kafka::offset o,
  std::optional<iobuf> key,
  const std::optional<shared_resolved_type_t>& val_type,
  std::optional<iobuf> parsable_val,
  model::timestamp ts,
  model::timestamp_type ts_t,
  const chunked_vector<model::record_header>& headers) {
    if (!val_type.has_value()) {
        vlog(
          datalake_log.error,
          "Must have parsed schema when using structured data mode");
        co_return record_translator::errc::unexpected_schema;
    }
    if (!parsable_val.has_value()) {
        vlog(datalake_log.error, "Tombstones cannot be translated");
        co_return record_translator::errc::translation_error;
    }
    auto ret_data = iceberg::struct_value{};
    auto system_data = build_rp_struct(
      pid, o, std::move(key), ts, ts_t, headers);
    // Fill in the internal value field.
    ret_data.fields.emplace_back(std::move(system_data));

    auto& resolved = *val_type.value();
    auto translated_val = co_await std::visit(
      value_translating_visitor{std::move(*parsable_val), resolved.type},
      resolved.schema.get_schema_ref());
    if (translated_val.has_error()) {
        vlog(
          datalake_log.warn,
          "Error converting buffer: {}",
          translated_val.error());
        co_return errc::translation_error;
    }

    auto redpanda_field_idx = get_redpanda_idx(
      std::get<iceberg::struct_type>(resolved.type));
    // Unwrap the struct fields.
    auto& val_struct = std::get<std::unique_ptr<iceberg::struct_value>>(
      translated_val.value().value());
    for (size_t i = 0; i < val_struct->fields.size(); ++i) {
        auto& field = val_struct->fields[i];
        if (redpanda_field_idx == i) {
            // To avoid collisions, move user fields named "redpanda" into
            // the nested "redpanda" system field.
            auto& system_vals
              = std::get<std::unique_ptr<iceberg::struct_value>>(
                ret_data.fields[0].value());
            system_vals->fields.emplace_back(std::move(field));
            continue;
        }
        ret_data.fields.emplace_back(std::move(field));
    }
    co_return ret_data;
}

} // namespace datalake
