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
#include "iceberg/schema.h"
#include "iceberg/schema_json.h"
#include "strings/string_switch.h"

#include <avro/DataFile.hh>

namespace iceberg {

namespace {
ss::sstring schema_to_json_str(const schema& s) {
    json::StringBuffer buf;
    json::Writer<json::StringBuffer> w(buf);
    rjson_serialize(w, s);
    return buf.GetString();
}
schema schema_from_str(const std::string& s) {
    json::Document parsed_schema;
    parsed_schema.Parse(s);
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
    json::Document parsed_spec;
    parsed_spec.Parse(strs.fields_json_str);
    if (!parsed_spec.IsObject()) {
        throw std::invalid_argument(fmt::format(
          "'partition-spec' metadata has type '{}' instead of object",
          parsed_spec.GetType()));
    }
    auto spec_spec_id = parse_required_i32(parsed_spec, "spec-id");
    if (spec_spec_id != spec_id) {
        throw std::invalid_argument(fmt::format(
          "Mismatched partition spec id {} vs {}", spec_id, spec_spec_id));
    }
    return partition_spec{
      .spec_id = partition_spec::id_t{spec_id},
      // TODO: implement me!
      .fields = {},
    };
}
partition_spec_strs partition_spec_to_str(const partition_spec& spec) {
    partition_spec_strs strs;
    strs.spec_id_str = fmt::format("{}", spec.spec_id());
    // TODO: implement me!
    strs.fields_json_str = fmt::format(
      "{{\"spec-id\":{},\"fields\":[]}}", spec.spec_id());
    return strs;
}

std::map<std::string, std::string>
metadata_to_map(const manifest_metadata& meta) {
    auto partition_spec_strs = partition_spec_to_str(meta.partition_spec);
    return {
      {"schema", schema_to_json_str(meta.schema)},
      {"content", std::string{content_type_to_str(meta.manifest_content_type)}},
      {"partition-spec", partition_spec_strs.fields_json_str},
      {"partition-spec-id", partition_spec_strs.spec_id_str},
      {"format-version", std::string{format_to_str(meta.format_version)}}};
}
// TODO: make DataFileReader::getMetadata const!
manifest_metadata
metadata_from_reader(avro::DataFileReader<manifest_entry>& rdr) {
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
    iobuf buf;
    size_t bytes_streamed = 0;
    auto out = std::make_unique<avro_iobuf_ostream>(
      4_KiB, &buf, &bytes_streamed);
    static constexpr size_t avro_default_sync_bytes = 16_KiB;
    auto meta = metadata_to_map(m.metadata);
    avro::DataFileWriter<manifest_entry> writer(
      std::move(out),
      manifest_entry::valid_schema(),
      avro_default_sync_bytes,
      avro::NULL_CODEC,
      meta);

    // TODO: the Avro code-generated manifest_entry doesn't have the r102
    // partition field defined, as it relies on runtime information of the
    // partition spec!
    for (const auto& e : m.entries) {
        writer.write(e);
    }
    writer.flush();
    writer.close();
    buf.trim_back(buf.size_bytes() - bytes_streamed);
    return buf;
}

manifest parse_manifest(iobuf buf) {
    auto in = std::make_unique<avro_iobuf_istream>(buf.copy());
    avro::DataFileReader<manifest_entry> reader(
      std::move(in), manifest_entry::valid_schema());
    auto meta = metadata_from_reader(reader);
    chunked_vector<manifest_entry> entries;
    while (true) {
        manifest_entry e;
        auto did_read = reader.read(e);
        if (!did_read) {
            break;
        }
        entries.emplace_back(std::move(e));
    }
    manifest m;
    m.metadata = std::move(meta);
    m.entries = std::move(entries);
    return m;
}

} // namespace iceberg
