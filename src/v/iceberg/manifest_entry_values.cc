/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "iceberg/manifest_entry_values.h"

#include "bytes/iobuf_parser.h"
#include "strings/string_switch.h"

namespace iceberg {

namespace {

ss::sstring from_iobuf(iobuf b) {
    auto size_bytes = b.size_bytes();
    iobuf_parser parser{std::move(b)};
    return parser.read_string(size_bytes);
}

template<typename PrimitiveV>
auto get_required_primitive(value v, std::string_view name) {
    if (!holds_alternative<primitive_value>(v)) {
        throw std::invalid_argument(
          fmt::format("Value {} is not a primitive", name));
    }
    auto& as_primitive = std::get<primitive_value>(v);
    if (!holds_alternative<PrimitiveV>(as_primitive)) {
        throw std::invalid_argument(
          fmt::format(
            "Value of '{}' is not the expected type {}: actual {}",
            name,
            PrimitiveV::name(),
            as_primitive));
    }
    // NOTE: values that contain iobufs must be moved.
    auto& as_t = std::get<PrimitiveV>(as_primitive);
    return std::move(as_t.val);
}
template<typename PrimitiveV>
auto get_required_primitive(std::optional<value> v, std::string_view name) {
    if (!v.has_value()) {
        throw std::invalid_argument(
          fmt::format("Expected primitive value '{}' is null", name));
    }
    return get_required_primitive<PrimitiveV>(std::move(*v), name);
}

template<typename T, typename PrimitiveV>
std::optional<T>
get_optional_primitive(std::optional<value> v, std::string_view name) {
    if (!v.has_value()) {
        return std::nullopt;
    }
    return T{get_required_primitive<PrimitiveV>(std::move(*v), name)};
}

std::unique_ptr<struct_value>
get_required_struct(std::optional<value> v, std::string_view name) {
    if (!v.has_value()) {
        throw std::invalid_argument(
          fmt::format("Expected struct value {} is null", name));
    }
    if (!std::holds_alternative<std::unique_ptr<struct_value>>(*v)) {
        throw std::invalid_argument(
          fmt::format("Value of {} is not a struct: {}", name, *v));
    }
    auto ret = std::get<std::unique_ptr<struct_value>>(std::move(*v));
    if (!ret) {
        throw std::invalid_argument(
          fmt::format("Struct {} value is nullptr", name));
    }
    return ret;
}

chunked_hash_map<nested_field::id_t, size_t>
get_counts_map(std::optional<value> v, std::string_view name) {
    if (!v.has_value()) {
        return {};
    }
    if (!holds_alternative<std::unique_ptr<map_value>>(*v)) {
        throw std::invalid_argument(
          fmt::format("Value for {} is not a map: {}", name, *v));
    }
    auto& as_map = std::get<std::unique_ptr<map_value>>(*v);
    chunked_hash_map<nested_field::id_t, size_t> ret;
    for (auto& kv : as_map->kvs) {
        try {
            auto k = get_required_primitive<int_value>(
              std::move(kv.key), "key");
            auto v = get_required_primitive<long_value>(
              std::move(kv.val), "val");
            ret.emplace(nested_field::id_t{k}, v);
        } catch (const std::exception& e) {
            throw std::runtime_error(
              fmt::format("Error parsing '{}' map: {}", name, e.what()));
        }
    }
    return ret;
}

template<typename ValueT, typename T>
std::optional<value> to_optional_value(std::optional<T> v) {
    if (!v.has_value()) {
        return std::nullopt;
    }
    return ValueT{*v};
}

int status_to_int(manifest_entry_status s) {
    switch (s) {
    case manifest_entry_status::existing:
        return 0;
    case manifest_entry_status::added:
        return 1;
    case manifest_entry_status::deleted:
        return 2;
    }
}

manifest_entry_status status_from_int(int s) {
    if (s == 0) {
        return manifest_entry_status::existing;
    }
    if (s == 1) {
        return manifest_entry_status::added;
    }
    if (s == 2) {
        return manifest_entry_status::deleted;
    }
    throw std::invalid_argument(
      fmt::format("Invalid manifest entry status: {}", s));
}

int content_to_int(data_file_content_type c) {
    switch (c) {
    case data_file_content_type::data:
        return 0;
    case data_file_content_type::position_deletes:
        return 1;
    case data_file_content_type::equality_deletes:
        return 2;
    }
}

data_file_content_type content_from_int(int c) {
    if (c == 0) {
        return data_file_content_type::data;
    }
    if (c == 1) {
        return data_file_content_type::position_deletes;
    }
    if (c == 2) {
        return data_file_content_type::equality_deletes;
    }
    throw std::invalid_argument(
      fmt::format("Invalid data file content type: {}", c));
}

iobuf format_to_str(data_file_format f) {
    switch (f) {
    case data_file_format::avro:
        return iobuf::from("AVRO");
    case data_file_format::orc:
        return iobuf::from("ORC");
    case data_file_format::parquet:
        return iobuf::from("PARQUET");
    }
}

data_file_format format_from_str(std::string_view s) {
    ss::sstring str(s);
    std::transform(str.begin(), str.end(), str.begin(), [](char c) {
        return std::tolower(c);
    });
    return string_switch<data_file_format>(str)
      .match("avro", data_file_format::avro)
      .match("orc", data_file_format::orc)
      .match("parquet", data_file_format::parquet);
}

std::unique_ptr<struct_value> data_file_to_value(const data_file& file) {
    auto ret = std::make_unique<struct_value>();
    ret->fields.emplace_back(int_value(content_to_int(file.content_type)));
    ret->fields.emplace_back(string_value(iobuf::from(file.file_path())));
    ret->fields.emplace_back(string_value(format_to_str(file.file_format)));
    ret->fields.emplace_back(std::move(file.partition.copy().val));
    ret->fields.emplace_back(
      long_value(static_cast<int64_t>(file.record_count)));
    ret->fields.emplace_back(
      long_value(static_cast<int64_t>(file.file_size_bytes)));

    // TODO: serialize the rest of the optional fields.
    // column_sizes
    ret->fields.emplace_back(std::nullopt);
    // value_counts
    ret->fields.emplace_back(std::nullopt);
    // null_value_counts
    ret->fields.emplace_back(std::nullopt);
    // nan_value_counts
    ret->fields.emplace_back(std::nullopt);
    // lower_bounds
    ret->fields.emplace_back(std::nullopt);
    // upper_bounds
    ret->fields.emplace_back(std::nullopt);
    // key_metadata
    ret->fields.emplace_back(std::nullopt);
    // split_offsets
    ret->fields.emplace_back(std::nullopt);
    // equality_ids
    ret->fields.emplace_back(std::nullopt);
    // sort_order_id
    ret->fields.emplace_back(std::nullopt);
    return ret;
}

data_file data_file_from_value(struct_value v) {
    data_file file;
    auto& fs = v.fields;
    if (fs.size() < 10) {
        throw std::invalid_argument("Expected more values");
    }
    file.content_type = content_from_int(
      get_required_primitive<int_value>(std::move(fs[0]), "content_type"));
    file.file_path = uri(from_iobuf(
      get_required_primitive<string_value>(std::move(fs[1]), "file_path")));
    file.file_format = format_from_str(from_iobuf(
      get_required_primitive<string_value>(std::move(fs[2]), "file_format")));
    file.partition = {get_required_struct(std::move(fs[3]), "partition")};
    file.record_count = get_required_primitive<long_value>(
      std::move(fs[4]), "record_count");
    file.file_size_bytes = get_required_primitive<long_value>(
      std::move(fs[5]), "file_size_bytes");
    file.column_sizes = get_counts_map(std::move(fs[6]), "column_sizes");
    file.value_counts = get_counts_map(std::move(fs[7]), "value_counts");
    file.null_value_counts = get_counts_map(
      std::move(fs[8]), "null_value_counts");
    file.nan_value_counts = get_counts_map(
      std::move(fs[9]), "nan_value_counts");
    return file;
}

} // namespace

struct_value manifest_entry_to_value(const manifest_entry& entry) {
    // NOTE: for correct manifest serialization, the order here must match both
    // the deserialization code and the manifest entry struct_type definition.
    struct_value ret;
    ret.fields.emplace_back(int_value(status_to_int(entry.status)));
    ret.fields.emplace_back(to_optional_value<long_value>(entry.snapshot_id));
    ret.fields.emplace_back(
      to_optional_value<long_value>(entry.sequence_number));
    ret.fields.emplace_back(
      to_optional_value<long_value>(entry.file_sequence_number));
    ret.fields.emplace_back(data_file_to_value(entry.data_file));
    return ret;
}

manifest_entry manifest_entry_from_value(struct_value v) {
    // NOTE: for correct manifest serialization,the order here must match both
    // the serialization code and the manifest entry struct_type definition.
    manifest_entry e;
    auto& fs = v.fields;
    if (fs.size() < 5) {
        throw std::invalid_argument("Expected more values");
    }
    e.status = status_from_int(
      get_required_primitive<int_value>(std::move(fs[0]), "status"));
    e.snapshot_id = get_optional_primitive<snapshot_id, long_value>(
      std::move(fs[1]), "snapshot_id");
    e.sequence_number = get_optional_primitive<sequence_number, long_value>(
      std::move(fs[2]), "sequence_number");
    e.file_sequence_number
      = get_optional_primitive<file_sequence_number, long_value>(
        std::move(fs[3]), "file_sequence_number");
    e.data_file = data_file_from_value(
      std::move(*get_required_struct(std::move(fs[4]), "data_file")));
    return e;
}

} // namespace iceberg
