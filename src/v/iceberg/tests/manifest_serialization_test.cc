// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/vlog.h"
#include "bytes/iobuf.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_file.h"

#include <seastar/core/temporary_buffer.hh>

#include <avro/Stream.hh>
#include <gtest/gtest.h>

using namespace iceberg;

namespace {

ss::logger icelog("icebergtest");

// Near-identical implementation of avro::MemoryOutputStream, but backed by an
// iobuf that can be released.
class avro_iobuf_ostream : public avro::OutputStream {
public:
    explicit avro_iobuf_ostream(size_t chunk_size)
      : chunk_size_(chunk_size)
      , available_(0)
      , byte_count_(0) {}
    ~avro_iobuf_ostream() override = default;

    iobuf release() {
        buf_.trim_back(buf_.size_bytes() - byte_count_);
        available_ = 0;
        byte_count_ = 0;
        vlog(icelog.trace, "ostream::release({})", buf_.size_bytes());
        return std::exchange(buf_, iobuf{});
    }

    // If there's no available space in the buffer, allocates `chunk_size_`
    // bytes at the end of the buffer.
    //
    // Returns the current position in the buffer, and the available remaining
    // space.
    bool next(uint8_t** data, size_t* len) final {
        vlog(icelog.trace, "ostream::next()");
        if (available_ == 0) {
            buf_.append(ss::temporary_buffer<char>{chunk_size_});
            available_ = chunk_size_;
        }
        auto back_frag = buf_.rbegin();
        *data = reinterpret_cast<uint8_t*>(
          back_frag->share(chunk_size_ - available_, available_).get_write());
        *len = available_;
        byte_count_ += available_;
        available_ = 0;
        return true;
    }

    void backup(size_t len) final {
        vlog(icelog.trace, "ostream::backup({})", len);
        available_ += len;
        byte_count_ -= len;
    }

    uint64_t byteCount() const final {
        vlog(icelog.trace, "ostream::bytecount()");
        return byte_count_;
    }

    void flush() final {}

private:
    // Size in bytes with which to allocate new fragments.
    const size_t chunk_size_;

    iobuf buf_;

    // Bytes remaining in the last fragment in the buffer.
    size_t available_;

    // Total number of bytes.
    size_t byte_count_;
};

// InputStream implementation that takes an iobuf as input.
// Iterates through the fragments of the given iobuf.
class avro_iobuf_istream : public avro::InputStream {
public:
    explicit avro_iobuf_istream(iobuf buf)
      : buf_(std::move(buf))
      , cur_frag_(buf_.begin())
      , cur_frag_pos_(0)
      , cur_pos_(0) {}

    // Returns the contiguous chunk of memory and the length of that memory.
    bool next(const uint8_t** data, size_t* len) final {
        vlog(icelog.trace, "istream::next()");
        if (cur_frag_ == buf_.end()) {
            return false;
        }
        auto left_in_frag = cur_frag_->size() - cur_frag_pos_;
        while (left_in_frag == 0) {
            ++cur_frag_;
            cur_frag_pos_ = 0;
            if (cur_frag_ == buf_.end()) {
                return false;
            }
            left_in_frag = cur_frag_->size();
        }
        *data = reinterpret_cast<const uint8_t*>(
          cur_frag_->share(cur_frag_pos_, left_in_frag).get());
        *len = left_in_frag;
        cur_frag_pos_ = cur_frag_->size();
        cur_pos_ += cur_frag_->size();
        return true;
    }

    void backup(size_t len) final {
        vlog(icelog.trace, "istream::backup({})", len);
        cur_pos_ -= len;
        if (cur_frag_ == buf_.end()) {
            cur_frag_ = std::prev(buf_.end());
            cur_frag_pos_ = cur_frag_->size();
        }
        while (cur_frag_pos_ < len) {
            len -= cur_frag_pos_;
            if (cur_frag_ == buf_.begin()) {
                return;
            }
            --cur_frag_;
            cur_frag_pos_ = cur_frag_->size();
        }
        cur_frag_pos_ -= len;
    }

    void skip(size_t len) final {
        vlog(icelog.trace, "istream::skip({})", len);
        if (cur_frag_ == buf_.end()) {
            return;
        }
        cur_pos_ += len;
        while (cur_frag_->size() - cur_frag_pos_ > len) {
            len -= cur_frag_->size() - cur_frag_pos_;
            ++cur_frag_;
            if (cur_frag_ == buf_.end()) {
                return;
            }
            cur_frag_pos_ = 0;
        }
        cur_frag_pos_ += len;
    }

    size_t byteCount() const final {
        vlog(icelog.trace, "istream::bytecount()");
        return cur_pos_;
    }

private:
    iobuf buf_;
    iobuf::iterator cur_frag_;
    size_t cur_frag_pos_;
    size_t cur_pos_;
};

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

    auto out = std::make_unique<avro_iobuf_ostream>(4096);

    // Encode to the output stream.
    avro::EncoderPtr encoder = avro::binaryEncoder();
    encoder->init(*out);
    avro::encode(*encoder, entry);
    encoder->flush();

    // Decode the iobuf from the input stream.
    auto buf = out->release();
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

    auto out = std::make_unique<avro_iobuf_ostream>(4096);

    // Encode to the output stream.
    avro::EncoderPtr encoder = avro::binaryEncoder();
    encoder->init(*out);
    avro::encode(*encoder, manifest);
    encoder->flush();

    // Decode the iobuf from the input stream.
    auto buf = out->release();
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
