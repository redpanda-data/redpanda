/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/debezium_translator.h"

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

/// Find a field by name in a struct_type and return its index.
std::optional<size_t>
find_field_idx(const iceberg::struct_type& st, std::string_view name) {
    for (size_t i = 0; i < st.fields.size(); ++i) {
        if (st.fields[i]->name == name) {
            return i;
        }
    }
    return std::nullopt;
}

/// Extract the inner struct_type from a field that is itself a struct.
const iceberg::struct_type*
get_inner_struct(const iceberg::nested_field& field) {
    if (auto* st = std::get_if<iceberg::struct_type>(&field.type)) {
        return st;
    }
    return nullptr;
}

/// Extract the iobuf from an iceberg::value that holds a string_value.
const iobuf* extract_string_buf(const std::optional<iceberg::value>& val) {
    if (!val.has_value()) {
        return nullptr;
    }
    auto* prim = std::get_if<iceberg::primitive_value>(&val.value());
    if (!prim) {
        return nullptr;
    }
    auto* sv = std::get_if<iceberg::string_value>(prim);
    if (!sv) {
        return nullptr;
    }
    return &sv->val;
}

/// Given an inner struct_value and its type, extract only the fields whose
/// names match the key field names, returning them as a new struct_value.
std::optional<iceberg::struct_value> extract_key_fields(
  const iceberg::struct_value& src,
  const iceberg::struct_type& src_type,
  const chunked_vector<ss::sstring>& key_field_names) {
    auto key = iceberg::struct_value{};
    for (const auto& name : key_field_names) {
        bool found = false;
        for (size_t i = 0; i < src_type.fields.size(); ++i) {
            if (src_type.fields[i]->name == name) {
                if (src.fields[i].has_value()) {
                    key.fields.emplace_back(
                      iceberg::make_copy(src.fields[i].value()));
                } else {
                    key.fields.emplace_back(std::nullopt);
                }
                found = true;
                break;
            }
        }
        if (!found) {
            return std::nullopt;
        }
    }
    return key;
}

/// Force all fields in a struct_type to non-required (except map keys).
void make_fields_optional(iceberg::struct_type& struct_type) {
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
}

} // namespace

iceberg::struct_type
debezium_envelope_to_table_type(const iceberg::struct_type& envelope_type) {
    auto ret_type = schemaless_struct_type();

    auto after_idx = find_field_idx(envelope_type, "after");
    if (!after_idx.has_value()) {
        vlog(
          datalake_log.error, "Debezium envelope schema missing 'after' field");
        return ret_type;
    }

    auto* after_struct = get_inner_struct(*envelope_type.fields[*after_idx]);
    if (!after_struct) {
        vlog(
          datalake_log.error,
          "Debezium envelope 'after' field is not a struct");
        return ret_type;
    }

    auto inner_type = after_struct->copy();
    make_fields_optional(inner_type);

    for (auto& field : inner_type.fields) {
        if (field->name == rp_struct_name) {
            auto& system_fields = rp_struct_type(ret_type);
            system_fields.fields.emplace_back(
              iceberg::nested_field::create(
                schemaless_next_field_id,
                "data",
                field->required,
                std::move(field->type)));
            continue;
        }
        ret_type.fields.emplace_back(std::move(field));
    }

    return ret_type;
}

debezium_translator::debezium_translator(type_resolver& key_resolver)
  : _key_resolver(key_resolver) {}

record_type debezium_translator::build_type(
  std::optional<shared_resolved_type_t> val_type) {
    std::optional<schema_identifier> val_id;
    if (!val_type.has_value()) {
        return record_type{
          .comps = record_schema_components{
            .key_identifier = std::nullopt,
            .val_identifier = std::nullopt,
            .is_debezium = true,
          },
          .type = schemaless_struct_type(),
          .key_field_names = std::nullopt,
        };
    }

    val_id = val_type.value()->id;
    auto envelope_type = std::get<iceberg::struct_type>(
      iceberg::make_copy(val_type.value()->type));
    auto ret_type = debezium_envelope_to_table_type(envelope_type);

    return record_type{
      .comps = record_schema_components{
        .key_identifier = std::nullopt,
        .val_identifier = std::move(val_id),
        .is_debezium = true,
      },
      .type = std::move(ret_type),
      .key_field_names
      = [this]() -> std::optional<chunked_vector<ss::sstring>> {
          if (!_cached_key_field_names) {
              return std::nullopt;
          }
          return _cached_key_field_names->copy();
      }(),
    };
}

ss::future<checked<translated_record, record_translator::errc>>
debezium_translator::translate_data(
  model::partition_id pid,
  kafka::offset o,
  std::optional<iobuf> key,
  const std::optional<shared_resolved_type_t>& val_type,
  std::optional<iobuf> parsable_val,
  model::timestamp ts,
  model::timestamp_type ts_t,
  const chunked_vector<model::record_header>& headers) {
    // Tombstone: both fields nullopt.
    if (!parsable_val.has_value()) {
        co_return translated_record{
          .data_row = std::nullopt,
          .delete_key = std::nullopt,
        };
    }

    if (!key.has_value()) {
        vlog(datalake_log.warn, "Debezium record missing key, routing to DLQ");
        co_return errc::translation_error;
    }

    if (!val_type.has_value()) {
        vlog(
          datalake_log.error,
          "Must have parsed schema when using debezium mode");
        co_return errc::unexpected_schema;
    }

    // Deserialize the full envelope.
    auto& resolved = *val_type.value();
    auto translated_val = co_await std::visit(
      value_translating_visitor{std::move(*parsable_val), resolved.type},
      resolved.schema.get_schema_ref());
    if (translated_val.has_error()) {
        vlog(
          datalake_log.warn,
          "Error converting Debezium envelope: {}",
          translated_val.error());
        co_return errc::translation_error;
    }

    auto& envelope_struct = std::get<std::unique_ptr<iceberg::struct_value>>(
      translated_val.value().value());

    // Look up field positions in the envelope schema.
    auto& envelope_type = std::get<iceberg::struct_type>(resolved.type);
    auto op_idx = find_field_idx(envelope_type, "op");
    auto before_idx = find_field_idx(envelope_type, "before");
    auto after_idx = find_field_idx(envelope_type, "after");

    if (!op_idx.has_value() || !after_idx.has_value()) {
        vlog(
          datalake_log.error,
          "Debezium envelope missing required fields (op/after)");
        co_return errc::translation_error;
    }

    auto* op_buf = extract_string_buf(envelope_struct->fields[*op_idx]);
    if (!op_buf) {
        vlog(datalake_log.error, "Debezium envelope 'op' is not a string");
        co_return errc::translation_error;
    }

    // Get the inner table struct_type from the envelope schema.
    auto* after_type_ptr = get_inner_struct(*envelope_type.fields[*after_idx]);
    if (!after_type_ptr) {
        vlog(
          datalake_log.error,
          "Debezium envelope 'after' field is not a struct type");
        co_return errc::translation_error;
    }
    const auto& after_type = *after_type_ptr;

    // Resolve key field IDs on first call.
    if (!_cached_key_field_names) {
        auto key_type_res = co_await _key_resolver.resolve_buf_type(
          key->copy());
        if (key_type_res.has_error()) {
            vlog(
              datalake_log.warn,
              "Failed to resolve key schema: {}",
              key_type_res.error());
            co_return errc::translation_error;
        }
        auto& key_resolved = key_type_res.value();
        if (key_resolved.type.has_value()) {
            auto& key_iceberg_type = std::get<iceberg::struct_type>(
              key_resolved.type.value()->type);
            chunked_vector<ss::sstring> names;
            for (const auto& key_field : key_iceberg_type.fields) {
                names.emplace_back(key_field->name);
            }
            _cached_key_field_names = std::move(names);
        } else {
            _cached_key_field_names.emplace();
        }
    }

    // Helper: extract the struct_value from the before or after envelope field.
    auto get_inner_value =
      [](std::optional<iceberg::value>& field) -> iceberg::struct_value* {
        if (!field.has_value()) {
            return nullptr;
        }
        auto* sv = std::get_if<std::unique_ptr<iceberg::struct_value>>(
          &field.value());
        if (!sv || !*sv) {
            return nullptr;
        }
        return sv->get();
    };

    auto* after_value = get_inner_value(envelope_struct->fields[*after_idx]);
    iceberg::struct_value* before_value = nullptr;
    if (before_idx.has_value()) {
        before_value = get_inner_value(envelope_struct->fields[*before_idx]);
    }

    // Helper: build a data row from the after struct value, following the
    // structured_data_translator pattern.
    auto build_data_row =
      [&](
        iceberg::struct_value* inner) -> std::optional<iceberg::struct_value> {
        if (!inner) {
            return std::nullopt;
        }
        auto ret_data = iceberg::struct_value{};
        auto system_data = build_rp_struct(
          pid, o, key->copy(), ts, ts_t, headers);
        ret_data.fields.emplace_back(std::move(system_data));

        auto redpanda_field_idx = get_redpanda_idx(after_type);
        for (size_t i = 0; i < inner->fields.size(); ++i) {
            auto& field = inner->fields[i];
            if (redpanda_field_idx == i) {
                rp_struct_value(ret_data).fields.emplace_back(std::move(field));
                continue;
            }
            ret_data.fields.emplace_back(std::move(field));
        }
        return ret_data;
    };

    // Helper: extract key fields from a struct_value.
    auto extract_key =
      [&](iceberg::struct_value* src) -> std::optional<iceberg::struct_value> {
        if (
          !src || !_cached_key_field_names
          || _cached_key_field_names->empty()) {
            return std::nullopt;
        }
        return extract_key_fields(*src, after_type, *_cached_key_field_names);
    };

    const auto& op = *op_buf;
    if (op == "c" || op == "r") {
        auto data_row = build_data_row(after_value);
        if (!data_row.has_value()) {
            vlog(
              datalake_log.warn, "Debezium create/read op but 'after' is null");
            co_return errc::translation_error;
        }
        co_return translated_record{
          .data_row = std::move(data_row),
          .delete_key = std::nullopt,
        };
    }

    if (op == "u") {
        auto data_row = build_data_row(after_value);
        if (!data_row.has_value()) {
            vlog(datalake_log.warn, "Debezium update op but 'after' is null");
            co_return errc::translation_error;
        }
        auto delete_key = extract_key(before_value);
        co_return translated_record{
          .data_row = std::move(data_row),
          .delete_key = std::move(delete_key),
        };
    }

    if (op == "d") {
        auto delete_key = extract_key(before_value);
        co_return translated_record{
          .data_row = std::nullopt,
          .delete_key = std::move(delete_key),
        };
    }

    vlog(datalake_log.warn, "Unsupported Debezium op type");
    co_return errc::translation_error;
}

} // namespace datalake
