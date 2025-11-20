// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/seastarx.h"
#include "random/generators.h"
#include "storage/snapshot.h"
#include "test_utils/gtest_exception.h"
#include "test_utils/random_bytes.h"

#include <gtest/gtest.h>

using namespace redpanda::test_utils;

TEST(SnapshotTest, MissingSnapshotIsNotError) {
    storage::simple_snapshot_manager mgr(
      "d/n/e", storage::simple_snapshot_manager::default_snapshot_filename);
    auto reader = mgr.open_snapshot().get();
    ASSERT_FALSE(reader);
}

TEST(SnapshotTest, ReadingFromEmptySnapshotIsError) {
    storage::simple_snapshot_manager mgr(
      ".", storage::simple_snapshot_manager::default_snapshot_filename);
    try {
        ss::remove_file(mgr.snapshot_path().string()).get();
    } catch (...) {
    }

    auto fd = ss::open_file_dma(
                mgr.snapshot_path().string(),
                ss::open_flags::wo | ss::open_flags::create)
                .get();
    fd.truncate(0).get();
    fd.close().get();

    auto reader = mgr.open_snapshot().get();
    ASSERT_TRUE(reader);
    ASSERT_THROWS_WITH_PREDICATE(
      reader.value().read_metadata().get(),
      std::runtime_error,
      [](const std::runtime_error& e) {
          return std::string(e.what()).find(
                   "Snapshot file does not contain full header")
                 != std::string::npos;
      });
    reader.value().close().get();
}

TEST(SnapshotTest, ReaderVerifiesHeaderCrc) {
    storage::simple_snapshot_manager mgr(
      ".", storage::simple_snapshot_manager::default_snapshot_filename);
    try {
        ss::remove_file(mgr.snapshot_path().string()).get();
    } catch (...) {
    }

    auto writer = mgr.start_snapshot().get();
    writer.write_metadata(iobuf()).get();
    writer.close().get();
    mgr.finish_snapshot(writer).get();

    {
        // write some junk into the metadata. we're not using seastar i/o here
        // because for a test its too much to deal with i/o alignment, etc..
        int fd = ::open(mgr.snapshot_path().c_str(), O_WRONLY);
        ASSERT_GT(fd, 0);
        ASSERT_GT(::write(fd, &fd, sizeof(fd)), 0);
        ASSERT_EQ(::fsync(fd), 0);
        ASSERT_EQ(::close(fd), 0);
    }

    auto reader = mgr.open_snapshot().get();
    ASSERT_TRUE(reader);
    ASSERT_THROWS_WITH_PREDICATE(
      reader.value().read_metadata().get(),
      std::runtime_error,
      [](const std::runtime_error& e) {
          return std::string(e.what()).find("Failed to verify header crc")
                 != std::string::npos;
      });
    reader.value().close().get();
}

TEST(SnapshotTest, ReaderVerifiesMetadataCrc) {
    storage::simple_snapshot_manager mgr(
      ".", storage::simple_snapshot_manager::default_snapshot_filename);
    try {
        ss::remove_file(mgr.snapshot_path().string()).get();
    } catch (...) {
    }

    auto writer = mgr.start_snapshot().get();
    auto metadata = bytes_to_iobuf(tests::random_bytes(10));
    writer.write_metadata(std::move(metadata)).get();
    writer.close().get();
    mgr.finish_snapshot(writer).get();

    {
        // write some junk into the header. we're not using seastar i/o here
        // because for a test its too much to deal with i/o alignment, etc..
        int fd = ::open(mgr.snapshot_path().c_str(), O_WRONLY);
        ASSERT_GT(fd, 0);
        ::lseek(fd, storage::snapshot_header::ondisk_size, SEEK_SET);
        ASSERT_GT(::write(fd, &fd, sizeof(fd)), 0);
        ASSERT_EQ(::fsync(fd), 0);
        ASSERT_EQ(::close(fd), 0);
    }

    auto reader = mgr.open_snapshot().get();
    ASSERT_TRUE(reader);
    ASSERT_THROWS_WITH_PREDICATE(
      reader.value().read_metadata().get(),
      std::runtime_error,
      [](const std::runtime_error& e) {
          return std::string(e.what()).find("Failed to verify metadata crc")
                 != std::string::npos;
      });
    reader.value().close().get();
}

TEST(SnapshotTest, ReadWrite) {
    storage::simple_snapshot_manager mgr(
      ".", storage::simple_snapshot_manager::default_snapshot_filename);
    try {
        ss::remove_file(mgr.snapshot_path().string()).get();
    } catch (...) {
    }

    auto metadata_orig = bytes_to_iobuf(tests::random_bytes(33));

    const auto blob = random_generators::gen_alphanum_string(1234);

    auto writer = mgr.start_snapshot().get();
    writer.write_metadata(metadata_orig.copy()).get();
    writer.output().write(blob).get();
    writer.close().get();
    mgr.finish_snapshot(writer).get();

    auto reader = mgr.open_snapshot().get();
    ASSERT_TRUE(reader);
    auto read_metadata = reader.value().read_metadata().get();
    EXPECT_EQ(read_metadata, metadata_orig);
    auto blob_read = reader.value().input().read_exactly(blob.size()).get();
    EXPECT_EQ(
      reader.value().get_snapshot_size().get(), mgr.get_snapshot_size().get());
    reader.value().close().get();
    EXPECT_EQ(blob_read.size(), 1234);
    EXPECT_EQ(blob, ss::to_sstring(blob_read.clone()));
}

TEST(SnapshotTest, RemovePartialSnapshots) {
    storage::simple_snapshot_manager mgr(
      ".", storage::simple_snapshot_manager::default_snapshot_filename);

    auto mk_partial = [&] {
        auto writer = mgr.start_snapshot().get();
        writer.close().get();
        return writer.path();
    };

    auto p1 = mk_partial();
    auto p2 = mk_partial();

    ASSERT_TRUE(ss::file_exists(p1.string()).get());
    ASSERT_TRUE(ss::file_exists(p2.string()).get());

    mgr.remove_partial_snapshots().get();

    ASSERT_FALSE(ss::file_exists(p1.string()).get());
    ASSERT_FALSE(ss::file_exists(p2.string()).get());
}
