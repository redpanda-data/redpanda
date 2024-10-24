// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/units.h"
#include "bytes/iobuf.h"
#include "container/fragmented_vector.h"
#include "iceberg/avro_utils.h"
#include "iceberg/manifest.h"
#include "iceberg/manifest_avro.h"
#include "iceberg/manifest_entry.avrogen.h"
#include "iceberg/manifest_file.avrogen.h"
#include "iceberg/manifest_list.h"
#include "iceberg/manifest_list_avro.h"
#include "iceberg/partition_key_type.h"
#include "iceberg/schema_json.h"
#include "iceberg/tests/test_schemas.h"
#include "test_utils/runfiles.h"
#include "utils/file_io.h"

#include <seastar/core/reactor.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/file.hh>

#include <avro/DataFile.hh>
#include <avro/Stream.hh>
#include <gtest/gtest.h>

using namespace iceberg;

namespace {

// Returns true if the trivial, non-union type fields match between the two
// manifest entries.
// TODO: define a manifest_entry struct that isn't tied to Avro codegen, that
// has a trivial operator==.
bool trivial_fields_eq(
  const avrogen::manifest_entry& lhs, const avrogen::manifest_entry& rhs) {
    return lhs.status == rhs.status
           && lhs.data_file.content == rhs.data_file.content
           && lhs.data_file.file_format == rhs.data_file.file_format
           && lhs.data_file.file_path == rhs.data_file.file_path
           && lhs.data_file.record_count == rhs.data_file.record_count
           && lhs.data_file.file_size_in_bytes
                == rhs.data_file.file_size_in_bytes;
}

} // anonymous namespace

TEST(ManifestSerializationTest, TestManifestEntry) {
    avrogen::manifest_entry entry;
    entry.status = 1;
    entry.data_file.content = 2;
    entry.data_file.file_path = "path/to/file";
    entry.data_file.file_format = "PARQUET";
    entry.data_file.partition = {};
    entry.data_file.record_count = 3;
    entry.data_file.file_size_in_bytes = 1024;

    size_t bytes_streamed{0};
    avro_iobuf_ostream::buf_container_t bufs;
    auto out = std::make_unique<avro_iobuf_ostream>(
      4096, &bufs, &bytes_streamed);

    // Encode to the output stream.
    avro::EncoderPtr encoder = avro::binaryEncoder();
    encoder->init(*out);
    avro::encode(*encoder, entry);
    encoder->flush();

    iobuf buf;
    for (auto& b : bufs) {
        buf.append(std::move(b));
    }
    // Decode the iobuf from the input stream.
    auto in = std::make_unique<avro_iobuf_istream>(std::move(buf));
    avro::DecoderPtr decoder = avro::binaryDecoder();
    decoder->init(*in);
    avrogen::manifest_entry dentry;
    avro::decode(*decoder, dentry);

    EXPECT_TRUE(trivial_fields_eq(entry, dentry));
}

TEST(ManifestSerializationTest, TestManyManifestEntries) {
    avrogen::manifest_entry entry;
    entry.status = 1;
    entry.data_file.content = 2;
    entry.data_file.file_path = "path/to/file";
    entry.data_file.file_format = "PARQUET";
    entry.data_file.partition = {};
    entry.data_file.record_count = 3;
    entry.data_file.file_size_in_bytes = 1024;

    size_t bytes_streamed{0};
    avro_iobuf_ostream::buf_container_t bufs;
    auto out = std::make_unique<avro_iobuf_ostream>(
      4096, &bufs, &bytes_streamed);

    // Encode many entries. This is a regression test for a bug where
    // serializing large Avro files would handle iobuf fragments improperly,
    // leading to incorrect serialization.
    avro::EncoderPtr encoder = avro::binaryEncoder();
    encoder->init(*out);
    for (int i = 0; i < 1024; i++) {
        avro::encode(*encoder, entry);
        encoder->flush();
    }

    iobuf buf;
    for (auto& b : bufs) {
        buf.append(std::move(b));
    }
    // Decode the iobuf from the input stream.
    auto in = std::make_unique<avro_iobuf_istream>(std::move(buf));
    avro::DecoderPtr decoder = avro::binaryDecoder();
    decoder->init(*in);
    for (int i = 0; i < 1024; i++) {
        avrogen::manifest_entry dentry;
        avro::decode(*decoder, dentry);
        EXPECT_TRUE(trivial_fields_eq(entry, dentry));
    }
}

TEST(ManifestSerializationTest, TestManifestList) {
    manifest_list l;
    for (int i = 0; i < 1024; ++i) {
        manifest_file file;
        file.manifest_path = "path/to/file";
        file.partition_spec_id = partition_spec::id_t{1};
        file.content = manifest_file_content::data;
        file.seq_number = sequence_number{3};
        file.min_seq_number = sequence_number{4};
        file.added_snapshot_id = snapshot_id{5};
        file.added_files_count = 6;
        file.existing_files_count = 7;
        file.deleted_files_count = 8;
        file.added_rows_count = 9;
        file.existing_rows_count = 10;
        file.deleted_rows_count = 11;
        l.files.emplace_back(std::move(file));
    }

    auto buf = serialize_avro(l);
    for (int i = 0; i < 10; ++i) {
        auto roundtrip_l = parse_manifest_list(std::move(buf));
        ASSERT_EQ(1024, roundtrip_l.files.size());
        ASSERT_EQ(roundtrip_l, l);
        buf = serialize_avro(roundtrip_l);
    }
}

TEST(ManifestSerializationTest, TestManifestFile) {
    avrogen::manifest_file manifest;
    manifest.manifest_path = "path/to/file";
    manifest.partition_spec_id = 1;
    manifest.content = 2;
    manifest.sequence_number = 3;
    manifest.min_sequence_number = 4;
    manifest.added_snapshot_id = 5;
    manifest.added_data_files_count = 6;
    manifest.existing_data_files_count = 7;
    manifest.deleted_data_files_count = 8;
    manifest.added_rows_count = 9;
    manifest.existing_rows_count = 10;
    manifest.deleted_rows_count = 11;

    size_t bytes_streamed{0};
    avro_iobuf_ostream::buf_container_t bufs;
    auto out = std::make_unique<avro_iobuf_ostream>(
      4096, &bufs, &bytes_streamed);

    // Encode to the output stream.
    avro::EncoderPtr encoder = avro::binaryEncoder();
    encoder->init(*out);
    avro::encode(*encoder, manifest);
    encoder->flush();

    iobuf buf;
    for (auto& b : bufs) {
        buf.append(std::move(b));
    }
    // Decode the iobuf from the input stream.
    auto in = std::make_unique<avro_iobuf_istream>(std::move(buf));
    avro::DecoderPtr decoder = avro::binaryDecoder();
    decoder->init(*in);
    avrogen::manifest_file dmanifest;
    avro::decode(*decoder, dmanifest);

    EXPECT_EQ(manifest.manifest_path, dmanifest.manifest_path);
    EXPECT_EQ(manifest.partition_spec_id, dmanifest.partition_spec_id);
    EXPECT_EQ(manifest.content, dmanifest.content);
    EXPECT_EQ(manifest.sequence_number, dmanifest.sequence_number);
    EXPECT_EQ(manifest.min_sequence_number, dmanifest.min_sequence_number);
    EXPECT_EQ(manifest.added_snapshot_id, dmanifest.added_snapshot_id);
    EXPECT_EQ(
      manifest.added_data_files_count, dmanifest.added_data_files_count);
    EXPECT_EQ(
      manifest.existing_data_files_count, dmanifest.existing_data_files_count);
    EXPECT_EQ(
      manifest.deleted_data_files_count, dmanifest.deleted_data_files_count);
    EXPECT_EQ(manifest.added_rows_count, dmanifest.added_rows_count);
    EXPECT_EQ(manifest.existing_rows_count, dmanifest.existing_rows_count);
    EXPECT_EQ(manifest.deleted_rows_count, dmanifest.deleted_rows_count);
}

TEST(ManifestSerializationTest, TestManifestAvroReaderWriter) {
    const auto& manifest_file_schema = avrogen::manifest_file::valid_schema();
    avrogen::manifest_file manifest;
    manifest.manifest_path = "path/to/file";
    manifest.partition_spec_id = 1;
    manifest.content = 2;
    manifest.sequence_number = 3;
    manifest.min_sequence_number = 4;
    manifest.added_snapshot_id = 5;
    manifest.added_data_files_count = 6;
    manifest.existing_data_files_count = 7;
    manifest.deleted_data_files_count = 8;
    manifest.added_rows_count = 9;
    manifest.existing_rows_count = 10;
    manifest.deleted_rows_count = 11;
    std::map<std::string, std::string> metadata;
    auto f1 = "{\"type\": \"dummyjson\"}";
    auto f2 = "2";
    metadata["f1"] = f1;
    metadata["f2"] = f2;

    size_t bytes_streamed{0};
    avro_iobuf_ostream::buf_container_t bufs;
    auto out = std::make_unique<avro_iobuf_ostream>(
      4_KiB, &bufs, &bytes_streamed);
    {
        avro::DataFileWriter<avrogen::manifest_file> writer(
          std::move(out),
          manifest_file_schema,
          16_KiB,
          avro::NULL_CODEC,
          metadata);
        writer.write(manifest);
        writer.flush();

        // NOTE: ~DataFileWriter does a final sync which may write to the
        // chunks. Destruct the writer before moving ownership of the chunks.
    }
    iobuf buf;
    for (auto& b : bufs) {
        buf.append(std::move(b));
    }
    auto in = std::make_unique<avro_iobuf_istream>(buf.copy());
    avro::DataFileReader<avrogen::manifest_file> reader(
      std::move(in), manifest_file_schema);
    avrogen::manifest_file dmanifest;
    reader.read(dmanifest);
    EXPECT_STREQ(reader.getMetadata("f1")->c_str(), f1);
    EXPECT_STREQ(reader.getMetadata("f2")->c_str(), f2);
    EXPECT_EQ(manifest.manifest_path, dmanifest.manifest_path);
    EXPECT_EQ(manifest.partition_spec_id, dmanifest.partition_spec_id);
    EXPECT_EQ(manifest.content, dmanifest.content);
    EXPECT_EQ(manifest.sequence_number, dmanifest.sequence_number);
    EXPECT_EQ(manifest.min_sequence_number, dmanifest.min_sequence_number);
    EXPECT_EQ(manifest.added_snapshot_id, dmanifest.added_snapshot_id);
    EXPECT_EQ(
      manifest.added_data_files_count, dmanifest.added_data_files_count);
    EXPECT_EQ(
      manifest.existing_data_files_count, dmanifest.existing_data_files_count);
    EXPECT_EQ(
      manifest.deleted_data_files_count, dmanifest.deleted_data_files_count);
    EXPECT_EQ(manifest.added_rows_count, dmanifest.added_rows_count);
    EXPECT_EQ(manifest.existing_rows_count, dmanifest.existing_rows_count);
    EXPECT_EQ(manifest.deleted_rows_count, dmanifest.deleted_rows_count);
}

TEST(ManifestSerializationTest, TestMetadataWithPartitionSpec) {
    schema s{
      .schema_struct = std::get<struct_type>(test_nested_schema_type()),
      .schema_id = schema::id_t{12},
      .identifier_field_ids = {nested_field::id_t{1}},
    };
    partition_spec p{
      .spec_id = partition_spec::id_t{8},
        .fields = {
            partition_field{
              .source_id = nested_field::id_t{2},
              .field_id = partition_field::id_t{1000},
              .name = "p0",
              .transform = bucket_transform{10},
            },
        },
    };
    manifest_metadata meta{
      .schema = std::move(s),
      .partition_spec = std::move(p),
      .format_version = format_version::v1,
      .manifest_content_type = manifest_content_type::data,
    };
    manifest m{
      .metadata = std::move(meta),
      .entries = {},
    };
    auto pk_type = partition_key_type::create(
      m.metadata.partition_spec, m.metadata.schema);
    auto serialized_buf = serialize_avro(m);
    for (int i = 0; i < 10; i++) {
        auto m_roundtrip = parse_manifest(pk_type, std::move(serialized_buf));
        ASSERT_EQ(m.metadata, m_roundtrip.metadata);
        ASSERT_EQ(0, m_roundtrip.entries.size());

        serialized_buf = serialize_avro(m_roundtrip);
    }
}

TEST(ManifestSerializationTest, TestSerializeManifestData) {
    // File may not be on mount that supports O_DIRECT.
    ss::engine().set_strict_dma(false);
    auto manifest_path = test_utils::get_runfile_path(
      "src/v/iceberg/tests/testdata/nested_manifest.avro");
    if (!manifest_path.has_value()) {
        manifest_path = "nested_manifest.avro";
    }
    auto orig_buf = iobuf{
      ss::util::read_entire_file(manifest_path.value()).get()};
    auto m = parse_manifest({struct_type{}}, orig_buf.copy());
    ASSERT_EQ(100, m.entries.size());
    ASSERT_EQ(m.metadata.manifest_content_type, manifest_content_type::data);
    ASSERT_EQ(m.metadata.format_version, format_version::v2);
    ASSERT_EQ(m.metadata.partition_spec.spec_id, 0);
    ASSERT_EQ(m.metadata.partition_spec.fields.size(), 0);
    ASSERT_EQ(
      m.metadata.schema.schema_struct,
      std::get<struct_type>(test_nested_schema_type()));

    auto serialized_buf = serialize_avro(m);
    for (int i = 0; i < 10; i++) {
        auto m_roundtrip = parse_manifest(
          {struct_type{}}, std::move(serialized_buf));
        ASSERT_EQ(m.metadata, m_roundtrip.metadata);
        ASSERT_EQ(100, m_roundtrip.entries.size());
        ASSERT_EQ(
          m_roundtrip.metadata.manifest_content_type,
          manifest_content_type::data);
        ASSERT_EQ(m_roundtrip.metadata.format_version, format_version::v2);
        ASSERT_EQ(m_roundtrip.metadata.partition_spec.spec_id, 0);
        ASSERT_EQ(m_roundtrip.metadata.partition_spec.fields.size(), 0);
        ASSERT_EQ(
          m_roundtrip.metadata.schema.schema_struct,
          std::get<struct_type>(test_nested_schema_type()));

        serialized_buf = serialize_avro(m_roundtrip);
    }
}
