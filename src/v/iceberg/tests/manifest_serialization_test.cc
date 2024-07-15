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
#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_file.h"

#include <seastar/core/temporary_buffer.hh>

#include <avro/DataFile.hh>
#include <avro/Stream.hh>
#include <gtest/gtest.h>

using namespace iceberg;

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
    auto out = std::make_unique<avro_iobuf_ostream>(4096, &buf);

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

    EXPECT_EQ(entry.status, dentry.status);
    EXPECT_EQ(entry.data_file.content, dentry.data_file.content);
    EXPECT_EQ(entry.data_file.file_path, dentry.data_file.file_path);
    EXPECT_EQ(entry.data_file.file_format, dentry.data_file.file_format);
    EXPECT_EQ(entry.data_file.record_count, dentry.data_file.record_count);
    EXPECT_EQ(
      entry.data_file.file_size_in_bytes, dentry.data_file.file_size_in_bytes);
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
    auto out = std::make_unique<avro_iobuf_ostream>(4096, &buf);

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
    auto out = std::make_unique<avro_iobuf_ostream>(4_KiB, &buf);
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
