// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "random/generators.h"
#include "seastarx.h"
#include "storage/snapshot.h"

#include <seastar/testing/thread_test_case.hh>

SEASTAR_THREAD_TEST_CASE(missing_snapshot_is_not_error) {
    storage::simple_snapshot_manager mgr(
      "d/n/e",
      storage::simple_snapshot_manager::default_snapshot_filename,
      ss::default_priority_class());
    auto reader = mgr.open_snapshot().get0();
    BOOST_REQUIRE(!reader);
}

SEASTAR_THREAD_TEST_CASE(reading_from_empty_snapshot_is_error) {
    storage::simple_snapshot_manager mgr(
      ".",
      storage::simple_snapshot_manager::default_snapshot_filename,
      ss::default_priority_class());
    try {
        ss::remove_file(mgr.snapshot_path().string()).get();
    } catch (...) {
    }

    auto fd = ss::open_file_dma(
                mgr.snapshot_path().string(),
                ss::open_flags::wo | ss::open_flags::create)
                .get0();
    fd.truncate(0).get();
    fd.close().get();

    auto reader = mgr.open_snapshot().get0();
    BOOST_REQUIRE(reader);
    BOOST_CHECK_EXCEPTION(
      reader->read_metadata().get0(),
      std::runtime_error,
      [](std::runtime_error e) {
          return std::string(e.what()).find(
                   "Snapshot file does not contain full header")
                 != std::string::npos;
      });
    reader->close().get0();
}

SEASTAR_THREAD_TEST_CASE(reader_verifies_header_crc) {
    storage::simple_snapshot_manager mgr(
      ".",
      storage::simple_snapshot_manager::default_snapshot_filename,
      ss::default_priority_class());
    try {
        ss::remove_file(mgr.snapshot_path().string()).get();
    } catch (...) {
    }

    auto writer = mgr.start_snapshot().get0();
    writer.write_metadata(iobuf()).get0();
    writer.close().get();
    mgr.finish_snapshot(writer).get();

    {
        // write some junk into the metadata. we're not using seastar i/o here
        // because for a test its too much to deal with i/o alignment, etc..
        int fd = ::open(mgr.snapshot_path().c_str(), O_WRONLY);
        BOOST_REQUIRE(fd > 0);
        ::write(fd, &fd, sizeof(fd));
        ::fsync(fd);
        ::close(fd);
    }

    auto reader = mgr.open_snapshot().get0();
    BOOST_REQUIRE(reader);
    BOOST_CHECK_EXCEPTION(
      reader->read_metadata().get0(),
      std::runtime_error,
      [](std::runtime_error e) {
          return std::string(e.what()).find("Failed to verify header crc")
                 != std::string::npos;
      });
    reader->close().get0();
}

SEASTAR_THREAD_TEST_CASE(reader_verifies_metadata_crc) {
    storage::simple_snapshot_manager mgr(
      ".",
      storage::simple_snapshot_manager::default_snapshot_filename,
      ss::default_priority_class());
    try {
        ss::remove_file(mgr.snapshot_path().string()).get();
    } catch (...) {
    }

    auto writer = mgr.start_snapshot().get0();
    auto metadata = bytes_to_iobuf(random_generators::get_bytes(10));
    writer.write_metadata(std::move(metadata)).get0();
    writer.close().get();
    mgr.finish_snapshot(writer).get();

    {
        // write some junk into the header. we're not using seastar i/o here
        // because for a test its too much to deal with i/o alignment, etc..
        int fd = ::open(mgr.snapshot_path().c_str(), O_WRONLY);
        BOOST_REQUIRE(fd > 0);
        ::lseek(fd, storage::snapshot_header::ondisk_size, SEEK_SET);
        ::write(fd, &fd, sizeof(fd));
        ::fsync(fd);
        ::close(fd);
    }

    auto reader = mgr.open_snapshot().get0();
    BOOST_REQUIRE(reader);
    BOOST_CHECK_EXCEPTION(
      reader->read_metadata().get0(),
      std::runtime_error,
      [](std::runtime_error e) {
          return std::string(e.what()).find("Failed to verify metadata crc")
                 != std::string::npos;
      });
    reader->close().get0();
}

SEASTAR_THREAD_TEST_CASE(read_write) {
    storage::simple_snapshot_manager mgr(
      ".",
      storage::simple_snapshot_manager::default_snapshot_filename,
      ss::default_priority_class());
    try {
        ss::remove_file(mgr.snapshot_path().string()).get();
    } catch (...) {
    }

    auto metadata_orig = bytes_to_iobuf(random_generators::get_bytes(33));

    const auto blob = random_generators::gen_alphanum_string(1234);

    auto writer = mgr.start_snapshot().get0();
    writer.write_metadata(metadata_orig.copy()).get();
    writer.output().write(blob).get();
    writer.close().get();
    mgr.finish_snapshot(writer).get();

    auto reader = mgr.open_snapshot().get0();
    BOOST_REQUIRE(reader);
    auto read_metadata = reader->read_metadata().get0();
    BOOST_TEST(read_metadata == metadata_orig);
    auto blob_read = reader->input().read_exactly(blob.size()).get0();
    BOOST_TEST(
      reader->get_snapshot_size().get0() == mgr.get_snapshot_size().get0());
    reader->close().get0();
    BOOST_TEST(blob_read.size() == 1234);
    BOOST_TEST(blob == ss::to_sstring(blob_read.clone()));
}

SEASTAR_THREAD_TEST_CASE(remove_partial_snapshots) {
    storage::simple_snapshot_manager mgr(
      ".",
      storage::simple_snapshot_manager::default_snapshot_filename,
      ss::default_priority_class());

    auto mk_partial = [&] {
        auto writer = mgr.start_snapshot().get0();
        writer.close().get();
        return writer.path();
    };

    auto p1 = mk_partial();
    auto p2 = mk_partial();

    BOOST_REQUIRE(ss::file_exists(p1.string()).get0());
    BOOST_REQUIRE(ss::file_exists(p2.string()).get0());

    mgr.remove_partial_snapshots().get();

    BOOST_REQUIRE(!ss::file_exists(p1.string()).get0());
    BOOST_REQUIRE(!ss::file_exists(p2.string()).get0());
}
