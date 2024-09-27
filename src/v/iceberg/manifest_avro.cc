// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "iceberg/manifest_avro.h"

#include "base/units.h"
#include "bytes/iobuf.h"
#include "iceberg/avro_utils.h"
#include "iceberg/datatypes_json.h"
#include "iceberg/json_utils.h"
#include "iceberg/manifest.h"
#include "iceberg/manifest_entry_type.h"
#include "iceberg/manifest_entry_values.h"
#include "iceberg/partition_json.h"
#include "iceberg/partition_key_type.h"
#include "iceberg/schema.h"
#include "iceberg/schema_avro.h"
#include "iceberg/schema_json.h"
#include "iceberg/values_avro.h"
#include "strings/string_switch.h"

#include <seastar/core/temporary_buffer.hh>

#include <avro/Compiler.hh>
#include <avro/DataFile.hh>
#include <avro/Generic.hh>
#include <avro/GenericDatum.hh>
#include <avro/Schema.hh>

namespace iceberg {

namespace {
schema schema_from_str(std::string_view s) {
    json::Document parsed_schema;
    parsed_schema.Parse(s.data(), s.size());
    return parse_schema(parsed_schema);
}

constexpr std::string_view content_type_to_str(manifest_content_type t) {
    switch (t) {
    case manifest_content_type::data:
        return "data";
    case manifest_content_type::deletes:
        return "deletes";
    }
}
manifest_content_type content_type_from_str(std::string_view s) {
    return string_switch<manifest_content_type>(s)
      .match(
        content_type_to_str(manifest_content_type::data),
        manifest_content_type::data)
      .match(
        content_type_to_str(manifest_content_type::deletes),
        manifest_content_type::deletes);
}

constexpr std::string_view format_to_str(format_version v) {
    switch (v) {
    case format_version::v1:
        return "1";
    case format_version::v2:
        return "2";
    }
}
format_version format_from_str(std::string_view s) {
    return string_switch<format_version>(s)
      .match(format_to_str(format_version::v1), format_version::v1)
      .match(format_to_str(format_version::v2), format_version::v2);
}

struct partition_spec_strs {
    ss::sstring spec_id_str;
    ss::sstring fields_json_str;
};
partition_spec partition_spec_from_str(const partition_spec_strs& strs) {
    auto spec_id = std::stoi(strs.spec_id_str);
    json::Document parsed_spec_json;
    parsed_spec_json.Parse(strs.fields_json_str);
    auto parsed_spec = parse_partition_spec(parsed_spec_json);
    if (parsed_spec.spec_id() != spec_id) {
        throw std::invalid_argument(fmt::format(
          "Mismatched partition spec id {} vs {}",
          spec_id,
          parsed_spec.spec_id()));
    }
    return parsed_spec;
}

std::map<std::string, std::string>
metadata_to_map(const manifest_metadata& meta) {
    return {
      {"schema", to_json_str(meta.schema)},
      {"content", std::string{content_type_to_str(meta.manifest_content_type)}},
      {"partition-spec", to_json_str(meta.partition_spec)},
      {"partition-spec-id", fmt::to_string(meta.partition_spec.spec_id())},
      {"format-version", std::string{format_to_str(meta.format_version)}}};
}
// TODO: make DataFileReader::getMetadata const!
manifest_metadata
metadata_from_reader(avro::DataFileReader<avro::GenericDatum>& rdr) {
    const auto find_required_str = [&rdr](const std::string& key) {
        auto val = rdr.getMetadata(key);
        if (!val) {
            throw std::invalid_argument(
              fmt::format("Manifest metadata missing field '{}'", key));
        }
        return *val;
    };
    manifest_metadata m;
    m.manifest_content_type = content_type_from_str(
      find_required_str("content"));
    m.format_version = format_from_str(find_required_str("format-version"));
    m.partition_spec = partition_spec_from_str(partition_spec_strs{
      .spec_id_str = find_required_str("partition-spec-id"),
      .fields_json_str = find_required_str("partition-spec"),
    });
    m.schema = schema_from_str(find_required_str("schema"));
    return m;
}

} // anonymous namespace

iobuf serialize_avro(const manifest& m) {
    size_t bytes_streamed = 0;
    avro_iobuf_ostream::buf_container_t bufs;
    static constexpr size_t avro_default_sync_bytes = 16_KiB;
    auto meta = metadata_to_map(m.metadata);
    auto pk_type = partition_key_type::create(
      m.metadata.partition_spec, m.metadata.schema);
    auto entry_type = manifest_entry_type(std::move(pk_type));
    auto entry_schema = avro::ValidSchema(
      struct_type_to_avro(entry_type, "manifest_entry"));
    {
        auto out = std::make_unique<avro_iobuf_ostream>(
          4_KiB, &bufs, &bytes_streamed);
        avro::DataFileWriter<avro::GenericDatum> writer(
          std::move(out),
          entry_schema,
          avro_default_sync_bytes,
          avro::NULL_CODEC,
          meta);

        for (auto& e : m.entries) {
            auto entry_struct = manifest_entry_to_value(e);
            auto entry_datum = struct_to_avro(
              entry_struct, entry_schema.root());
            writer.write(entry_datum);
        }
        writer.flush();
        writer.close();

        // NOTE: ~DataFileWriter does a final sync which may write to the
        // chunks. Destruct the writer before moving ownership of the chunks.
    }
    iobuf buf;
    for (auto& b : bufs) {
        buf.append(std::move(b));
    }
    buf.trim_back(buf.size_bytes() - bytes_streamed);
    return buf;
}

manifest parse_manifest(const partition_key_type& pk_type, iobuf buf) {
    auto entry_type = field_type{manifest_entry_type(pk_type.copy())};
    auto entry_schema = avro::ValidSchema(
      struct_type_to_avro(std::get<struct_type>(entry_type), "manifest_entry"));
    auto in = std::make_unique<avro_iobuf_istream>(std::move(buf));
    avro::DataFileReader<avro::GenericDatum> reader(
      std::move(in), entry_schema);
    auto meta = metadata_from_reader(reader);
    chunked_vector<manifest_entry> entries;
    while (true) {
        avro::GenericDatum d(entry_schema);
        auto did_read = reader.read(d);
        if (!did_read) {
            break;
        }
        auto parsed_struct = std::get<std::unique_ptr<struct_value>>(
          *val_from_avro(d, entry_type, field_required::yes));
        entries.emplace_back(
          manifest_entry_from_value(std::move(*parsed_struct)));
    }
    manifest m;
    m.metadata = std::move(meta);
    m.entries = std::move(entries);
    return m;
}

} // namespace iceberg
