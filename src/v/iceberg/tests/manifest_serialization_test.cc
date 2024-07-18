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
#include "iceberg/avro_utils.h"
#include "iceberg/manifest.h"
#include "iceberg/manifest_avro.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_file.h"
#include "iceberg/schema_json.h"
#include "iceberg/tests/test_schemas.h"
#include "utils/file_io.h"

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
bool trivial_fields_eq(const manifest_entry& lhs, const manifest_entry& rhs) {
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
    manifest_entry entry;
    entry.status = 1;
    entry.data_file.content = 2;
    entry.data_file.file_path = "path/to/file";
    entry.data_file.file_format = "PARQUET";
    entry.data_file.partition = {};
    entry.data_file.record_count = 3;
    entry.data_file.file_size_in_bytes = 1024;

    iobuf buf;
    size_t bytes_streamed{0};
    auto out = std::make_unique<avro_iobuf_ostream>(
      4096, &buf, &bytes_streamed);

    // Encode to the output stream.
    avro::EncoderPtr encoder = avro::binaryEncoder();
    encoder->init(*out);
    avro::encode(*encoder, entry);
    encoder->flush();

    // Decode the iobuf from the input stream.
    auto in = std::make_unique<avro_iobuf_istream>(std::move(buf));
    avro::DecoderPtr decoder = avro::binaryDecoder();
    decoder->init(*in);
    manifest_entry dentry;
    avro::decode(*decoder, dentry);

    EXPECT_TRUE(trivial_fields_eq(entry, dentry));
}

TEST(ManifestSerializationTest, TestManyManifestEntries) {
    manifest_entry entry;
    entry.status = 1;
    entry.data_file.content = 2;
    entry.data_file.file_path = "path/to/file";
    entry.data_file.file_format = "PARQUET";
    entry.data_file.partition = {};
    entry.data_file.record_count = 3;
    entry.data_file.file_size_in_bytes = 1024;

    iobuf buf;
    size_t bytes_streamed{0};
    auto out = std::make_unique<avro_iobuf_ostream>(
      4096, &buf, &bytes_streamed);

    // Encode many entries. This is a regression test for a bug where
    // serializing large Avro files would handle iobuf fragments improperly,
    // leading to incorrect serialization.
    avro::EncoderPtr encoder = avro::binaryEncoder();
    encoder->init(*out);
    for (int i = 0; i < 1024; i++) {
        avro::encode(*encoder, entry);
        encoder->flush();
    }

    // Decode the iobuf from the input stream.
    auto in = std::make_unique<avro_iobuf_istream>(std::move(buf));
    avro::DecoderPtr decoder = avro::binaryDecoder();
    decoder->init(*in);
    for (int i = 0; i < 1024; i++) {
        manifest_entry dentry;
        avro::decode(*decoder, dentry);
        EXPECT_TRUE(trivial_fields_eq(entry, dentry));
    }
}

TEST(ManifestSerializationTest, TestManifestFile) {
    manifest_file manifest;
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

    iobuf buf;
    size_t bytes_streamed{0};
    auto out = std::make_unique<avro_iobuf_ostream>(
      4096, &buf, &bytes_streamed);

    // Encode to the output stream.
    avro::EncoderPtr encoder = avro::binaryEncoder();
    encoder->init(*out);
    avro::encode(*encoder, manifest);
    encoder->flush();

    // Decode the iobuf from the input stream.
    auto in = std::make_unique<avro_iobuf_istream>(std::move(buf));
    avro::DecoderPtr decoder = avro::binaryDecoder();
    decoder->init(*in);
    manifest_file dmanifest;
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
    const auto& manifest_file_schema = manifest_file::valid_schema();
    manifest_file manifest;
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

    iobuf buf;
    size_t bytes_streamed{0};
    auto out = std::make_unique<avro_iobuf_ostream>(
      4_KiB, &buf, &bytes_streamed);
    avro::DataFileWriter<manifest_file> writer(
      std::move(out), manifest_file_schema, 16_KiB, avro::NULL_CODEC, metadata);
    writer.write(manifest);
    writer.flush();
    auto in = std::make_unique<avro_iobuf_istream>(buf.copy());
    avro::DataFileReader<manifest_file> reader(
      std::move(in), manifest_file_schema);
    manifest_file dmanifest;
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

TEST(ManifestSerializationTest, TestSerializeManifestData) {
    auto orig_buf = iobuf{
      ss::util::read_entire_file("nested_manifest.avro").get0()};
    auto m = parse_manifest(orig_buf.copy());
    ASSERT_EQ(100, m.entries.size());
    ASSERT_EQ(m.metadata.manifest_content_type, manifest_content_type::data);
    ASSERT_EQ(m.metadata.format_version, format_version::v2);
    ASSERT_EQ(m.metadata.partition_spec.spec_id, 0);
    ASSERT_EQ(m.metadata.partition_spec.fields.size(), 0);
    ASSERT_EQ(
      m.metadata.schema.schema_struct,
      std::get<struct_type>(test_nested_schema_type()));

    auto serialized_buf = serialize_avro(m);
    auto m_roundtrip = parse_manifest(serialized_buf.copy());
    ASSERT_EQ(m.metadata, m_roundtrip.metadata);
    ASSERT_EQ(100, m_roundtrip.entries.size());

    auto roundtrip_buf = serialize_avro(m_roundtrip);
    ASSERT_EQ(serialized_buf, roundtrip_buf);
}
