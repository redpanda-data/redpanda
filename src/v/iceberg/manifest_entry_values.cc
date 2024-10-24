// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
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
auto get_required_primitive(value v) {
    if (!holds_alternative<primitive_value>(v)) {
        throw std::invalid_argument("Value is not a primitive");
    }
    auto& as_primitive = std::get<primitive_value>(v);
    if (!holds_alternative<PrimitiveV>(as_primitive)) {
        throw std::invalid_argument("Value is not the expected type");
    }
    // NOTE: values that contain iobufs must be moved.
    auto& as_t = std::get<PrimitiveV>(as_primitive);
    return std::move(as_t.val);
}
template<typename PrimitiveV>
auto get_required_primitive(std::optional<value> v) {
    if (!v.has_value()) {
        throw std::invalid_argument("Expected primitive value is null");
    }
    return get_required_primitive<PrimitiveV>(std::move(*v));
}

template<typename T, typename PrimitiveV>
std::optional<T> get_optional_primitive(std::optional<value> v) {
    if (!v.has_value()) {
        return std::nullopt;
    }
    return T{get_required_primitive<PrimitiveV>(std::move(*v))};
}

std::unique_ptr<struct_value> get_required_struct(std::optional<value> v) {
    if (!v.has_value()) {
        throw std::invalid_argument("Expected struct value is null");
    }
    if (!std::holds_alternative<std::unique_ptr<struct_value>>(*v)) {
        throw std::invalid_argument("Value is not a struct");
    }
    auto ret = std::get<std::unique_ptr<struct_value>>(std::move(*v));
    if (!ret) {
        throw std::invalid_argument("Struct value is nullptr");
    }
    return ret;
}

chunked_hash_map<nested_field::id_t, size_t>
get_counts_map(std::optional<value> v) {
    if (!v.has_value()) {
        return {};
    }
    if (!holds_alternative<std::unique_ptr<map_value>>(*v)) {
        throw std::invalid_argument("Value is not a map");
    }
    auto& as_map = std::get<std::unique_ptr<map_value>>(*v);
    chunked_hash_map<nested_field::id_t, size_t> ret;
    for (auto& kv : as_map->kvs) {
        auto k = get_required_primitive<int_value>(std::move(kv.key));
        auto v = get_required_primitive<long_value>(std::move(kv.val));
        ret.emplace(nested_field::id_t{k}, v);
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
        return iobuf::from("avro");
    case data_file_format::orc:
        return iobuf::from("orc");
    case data_file_format::parquet:
        return iobuf::from("parquet");
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
    ret->fields.emplace_back(string_value(iobuf::from(file.file_path)));
    ret->fields.emplace_back(string_value(format_to_str(file.file_format)));
    ret->fields.emplace_back(std::move(file.partition.copy().val));
    ret->fields.emplace_back(
      long_value(static_cast<int64_t>(file.record_count)));
    ret->fields.emplace_back(
      long_value(static_cast<int64_t>(file.file_size_bytes)));

    // TODO: serialize the rest of the optional fields.
    ret->fields.emplace_back(std::nullopt);
    ret->fields.emplace_back(std::nullopt);
    ret->fields.emplace_back(std::nullopt);
    ret->fields.emplace_back(std::nullopt);
    ret->fields.emplace_back(std::nullopt);
    ret->fields.emplace_back(std::nullopt);
    ret->fields.emplace_back(std::nullopt);
    ret->fields.emplace_back(std::nullopt);
    ret->fields.emplace_back(std::nullopt);
    ret->fields.emplace_back(std::nullopt);
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
      get_required_primitive<int_value>(std::move(fs[0])));
    file.file_path = from_iobuf(
      get_required_primitive<string_value>(std::move(fs[1])));
    file.file_format = format_from_str(
      from_iobuf(get_required_primitive<string_value>(std::move(fs[2]))));
    file.partition = {get_required_struct(std::move(fs[3]))};
    file.record_count = get_required_primitive<long_value>(std::move(fs[4]));
    file.file_size_bytes = get_required_primitive<long_value>(std::move(fs[5]));
    file.column_sizes = get_counts_map(std::move(fs[6]));
    file.value_counts = get_counts_map(std::move(fs[7]));
    file.null_value_counts = get_counts_map(std::move(fs[8]));
    file.distinct_counts = get_counts_map(std::move(fs[9]));
    file.nan_value_counts = get_counts_map(std::move(fs[10]));
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
      get_required_primitive<int_value>(std::move(fs[0])));
    e.snapshot_id = get_optional_primitive<snapshot_id, long_value>(
      std::move(fs[1]));
    e.sequence_number = get_optional_primitive<sequence_number, long_value>(
      std::move(fs[2]));
    e.file_sequence_number
      = get_optional_primitive<file_sequence_number, long_value>(
        std::move(fs[3]));
    e.data_file = data_file_from_value(
      std::move(*get_required_struct(std::move(fs[4]))));
    return e;
}

} // namespace iceberg
