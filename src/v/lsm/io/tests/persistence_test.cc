/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cloud_io/remote.h"
#include "cloud_io/tests/s3_imposter.h"
#include "cloud_io/tests/scoped_remote.h"
#include "cloud_storage_clients/types.h"
#include "lsm/core/internal/files.h"
#include "lsm/io/cloud_persistence.h"
#include "lsm/io/disk_persistence.h"
#include "lsm/io/memory_persistence.h"
#include "lsm/io/persistence.h"
#include "utils/uuid.h"

#include <seastar/core/coroutine.hh>
#include <seastar/util/defer.hh>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

using namespace lsm::io;
namespace lsm_internal = lsm::internal;
using lsm_internal::operator""_file_id;
using lsm_internal::operator""_db_epoch;

using data_persistence_factory
  = std::function<ss::future<std::unique_ptr<data_persistence>>()>;

class PersistenceTest
  : public ::testing::TestWithParam<data_persistence_factory> {
protected:
    void SetUp() override { persistence = GetParam()().get(); }

    void TearDown() override {
        if (persistence) {
            persistence->close().get();
        }
    }

    ss::future<std::vector<lsm_internal::file_handle>> list_files() {
        auto gen = persistence->list_files();
        std::vector<lsm_internal::file_handle> files;
        while (auto file = co_await gen()) {
            files.push_back(*file);
        }
        co_return files;
    }

    std::unique_ptr<data_persistence> persistence;
};

TEST_P(PersistenceTest, CanWriteAndReadAFile) {
    {
        auto w = persistence->open_sequential_writer({}).get();
        auto _ = ss::defer([&w] { w->close().get(); });
        w->append(iobuf::from("hello")).get();
        w->append(iobuf::from("world")).get();
    }
    {
        auto maybe_r = persistence->open_random_access_reader({}).get();
        ASSERT_TRUE(bool(maybe_r));
        auto r = std::move(*maybe_r);
        auto _ = ss::defer([&r] { r->close().get(); });
        auto buf = r->read(1, 4).get().as_iobuf();
        EXPECT_EQ(buf, iobuf::from("ello")) << buf.hexdump(32);
        buf = r->read(5, 5).get().as_iobuf();
        EXPECT_EQ(buf, iobuf::from("world")) << buf.hexdump(32);
        EXPECT_ANY_THROW(r->read(8, 4).get());
    }
}

TEST_P(PersistenceTest, ListFiles) {
    std::vector<lsm_internal::file_handle> files;
    {
        for (auto i = 0_file_id; i < 25_file_id; ++i) {
            files.emplace_back(i, 0_db_epoch);
            auto w = persistence
                       ->open_sequential_writer({.id = i, .epoch = 0_db_epoch})
                       .get();
            auto _ = ss::defer([&w] { w->close().get(); });
            w->append(iobuf::from(fmt::format("hello, world: {}", i))).get();
        }
    }
    EXPECT_THAT(list_files().get(), testing::UnorderedElementsAreArray(files));
    persistence->remove_file({.id = 10_file_id, .epoch = 0_db_epoch}).get();
    files.erase(files.begin() + 10);
    EXPECT_THAT(list_files().get(), testing::UnorderedElementsAreArray(files));
}

TEST_P(PersistenceTest, OverwriteFile) {
    for (int i = 0; i < 3; ++i) {
        auto w = persistence->open_sequential_writer({}).get();
        auto _ = ss::defer([&w] { w->close().get(); });
        w->append(iobuf::from(fmt::format("hello, world: {}", i))).get();
    }
    auto maybe_r = persistence->open_random_access_reader({}).get();
    ASSERT_TRUE(bool(maybe_r));
    auto r = std::move(*maybe_r);
    auto _ = ss::defer([&r] { r->close().get(); });
    auto buf = r->read(0, 15).get();
    EXPECT_EQ(buf.as_iobuf(), iobuf::from("hello, world: 2"))
      << buf.as_iobuf().hexdump(32);
}

TEST_P(PersistenceTest, ReadNonExisting) {
    auto maybe_r = persistence->open_random_access_reader({}).get();
    EXPECT_FALSE(bool(maybe_r));
}

TEST_P(PersistenceTest, RandomAccessReaderComprehensive) {
    // Create a single 8MiB file with predictable content
    constexpr size_t file_size = 8_MiB;

    // Create file with a pattern that's easy to verify
    {
        auto w = persistence->open_sequential_writer({}).get();
        auto _ = ss::defer([&w] { w->close().get(); });
        iobuf content;
        // Build content in chunks for efficiency
        constexpr size_t chunk_size = 4096;
        std::string chunk;
        chunk.reserve(chunk_size);
        for (size_t i = 0; i < file_size; ++i) {
            // Create a repeating pattern: a-z repeated
            chunk.push_back('a' + (i % 26));
            if (chunk.size() == chunk_size) {
                content.append(chunk.data(), chunk.size());
                chunk.clear();
            }
        }
        if (!chunk.empty()) {
            content.append(chunk.data(), chunk.size());
        }
        w->append(std::move(content)).get();
    }

    // Open reader for all tests
    auto maybe_r = persistence->open_random_access_reader({}).get();
    ASSERT_TRUE(bool(maybe_r));
    auto r = std::move(*maybe_r);

    // Test many different offset/length combinations
    std::vector<std::pair<size_t, size_t>> test_cases;

    // Small reads at various alignments
    for (auto offset : {0, 1, 2, 3, 4, 5, 7, 15, 31, 63, 127, 255, 511}) {
        for (auto length : {1, 2, 4, 8, 16, 32, 64, 128, 256}) {
            test_cases.emplace_back(offset, length);
        }
    }

    // Reads at DMA alignment boundaries (512 bytes)
    for (auto offset :
         {510, 511, 512, 513, 514, 1022, 1023, 1024, 1025, 1026}) {
        for (auto length :
             {1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048}) {
            test_cases.emplace_back(offset, length);
        }
    }

    // Reads at page alignment boundaries (4096 bytes)
    for (auto offset : {4094, 4095, 4096, 4097, 4098, 8190, 8191, 8192, 8193}) {
        for (auto length : {1, 2, 4, 8, 512, 1024, 2048, 4096, 8192}) {
            test_cases.emplace_back(offset, length);
        }
    }

    // Reads around ioarray chunk boundaries (128 KiB)
    for (auto offset :
         {128_KiB - 10,
          128_KiB - 1,
          128_KiB,
          128_KiB + 1,
          128_KiB + 10,
          256_KiB - 10,
          256_KiB - 1,
          256_KiB,
          256_KiB + 1,
          256_KiB + 10}) {
        for (auto length : std::to_array<size_t>(
               {1, 10, 100, 1024, 4096, 8192, 64_KiB, 128_KiB, 256_KiB})) {
            test_cases.emplace_back(offset, length);
        }
    }

    // Large reads at various offsets
    for (auto offset :
         std::to_array<size_t>({0, 1, 511, 512, 4095, 4096, 128_KiB, 1_MiB})) {
        for (auto length : {128_KiB, 256_KiB, 512_KiB, 1_MiB, 2_MiB, 4_MiB}) {
            test_cases.emplace_back(offset, length);
        }
    }

    // Reads near end of file
    for (auto offset :
         {file_size - 1_MiB,
          file_size - 128_KiB,
          file_size - 4096,
          file_size - 512,
          file_size - 100,
          file_size - 10,
          file_size - 1}) {
        for (auto length :
             std::to_array<size_t>({1, 10, 100, 512, 4096, 128_KiB})) {
            test_cases.emplace_back(offset, length);
        }
    }

    // Test all valid cases
    for (const auto& [offset, length] : test_cases) {
        if (offset + length > file_size) {
            continue;
        }

        // Read and verify the data
        auto array = r->read(offset, length).get();
        ASSERT_EQ(array.size(), length)
          << "offset=" << offset << " length=" << length;

        // Verify the content matches the expected pattern
        size_t i = 0;
        for (char c : array.as_range()) {
            char expected = 'a' + ((offset + i) % 26);
            ASSERT_EQ(c, expected) << "offset=" << offset
                                   << " length=" << length << " position=" << i;
            ++i;
        }

        // Sanity check: we read exactly length bytes
        ASSERT_EQ(i, length) << "offset=" << offset << " length=" << length;
    }

    // Test reading past end of file - should throw
    EXPECT_ANY_THROW(r->read(file_size, 1).get());
    EXPECT_ANY_THROW(r->read(file_size - 100, 200).get());
    EXPECT_ANY_THROW(r->read(0, file_size + 1).get());

    // Close reader before removing file
    r->close().get();

    // Clean up
    persistence->remove_file({}).get();
}

// Mock cloud data persistence that owns s3_imposter and proxies to the real
// cloud_data_persistence implementation. This allows cloud persistence to be
// tested within the parameterized test framework.
class mock_cloud_data_persistence : public data_persistence {
public:
    mock_cloud_data_persistence(
      std::unique_ptr<s3_imposter_fixture> fixture,
      std::unique_ptr<cloud_io::scoped_remote> sr,
      std::unique_ptr<data_persistence> impl)
      : fixture_(std::move(fixture))
      , sr_(std::move(sr))
      , impl_(std::move(impl)) {}

    ss::future<std::unique_ptr<sequential_file_writer>>
    open_sequential_writer(lsm_internal::file_handle handle) override {
        return impl_->open_sequential_writer(handle);
    }

    ss::future<optional_pointer<random_access_file_reader>>
    open_random_access_reader(lsm_internal::file_handle handle) override {
        return impl_->open_random_access_reader(handle);
    }

    ss::future<> remove_file(lsm_internal::file_handle handle) override {
        co_await impl_->remove_file(handle);
        // We also need to remove the expectation from the s3 imposter as well
        fixture_->remove_expectations(
          chunked_vector<ss::sstring>::single(
            fmt::format(
              "test-prefix/{}", lsm_internal::sst_file_name(handle))));
    }

    ss::coroutine::experimental::generator<lsm_internal::file_handle>
    list_files() override {
        return impl_->list_files();
    }

    ss::future<> close() override { return impl_->close(); }

private:
    std::unique_ptr<s3_imposter_fixture> fixture_;
    std::unique_ptr<cloud_io::scoped_remote> sr_;
    std::unique_ptr<data_persistence> impl_;
};

INSTANTIATE_TEST_SUITE_P(
  PersistenceSuite,
  PersistenceTest,
  testing::Values(
    [] { return ss::as_ready_future(make_memory_data_persistence()); },
    [] {
        std::filesystem::path tmpdir = std::getenv("TEST_TMPDIR");
        // Ensure each testcase has it's own directory.
        auto subdir = ss::sstring(uuid_t::create());
        return open_disk_data_persistence(tmpdir / std::string_view(subdir));
    },
    []() -> ss::future<std::unique_ptr<data_persistence>> {
        std::filesystem::path tmpdir = std::getenv("TEST_TMPDIR");
        auto staging_subdir = ss::sstring(uuid_t::create());
        auto staging = tmpdir / std::string_view(staging_subdir);

        auto fixture = std::make_unique<s3_imposter_fixture>();
        fixture->set_expectations_and_listen({});
        auto sr = cloud_io::scoped_remote::create(10, fixture->conf);

        auto impl = co_await open_cloud_data_persistence(
          staging,
          &sr->remote.local(),
          fixture->bucket_name,
          cloud_storage_clients::object_key("test-prefix"),
          "test-prefix");

        co_return std::make_unique<mock_cloud_data_persistence>(
          std::move(fixture), std::move(sr), std::move(impl));
    }),
  [](const testing::TestParamInfo<data_persistence_factory>& info) {
      switch (info.index) {
      case 0:
          return "memory";
      case 1:
          return "disk";
      case 2:
          return "cloud";
      default:
          return "unknown";
      }
  });
