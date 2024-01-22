/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "base/units.h"
#include "io/persistence.h"
#include "io/tests/common.h"
#include "test_utils/test.h"

#include <seastar/util/defer.hh>
#include <seastar/util/file.hh>
#include <seastar/util/tmp_file.hh>

namespace io = experimental::io;

template<typename T>
class PersistenceTest : public ::testing::Test {
public:
    void SetUp() override {
        dir = seastar::make_tmp_dir(".").get();
        path = make_filename();
    }

    void TearDown() override {
        for (auto& file : open_files) {
            file->close().get();
        }
        dir.remove().get();
    }

    std::filesystem::path make_filename() {
        return dir.get_path() / fmt::format("file.{}", count++);
    }

    // fs::create wrapper that records file to close after test finishes
    auto create(std::filesystem::path path) {
        auto f = this->fs.create(path).get();
        open_files.push_back(f);
        return f;
    }

    // fs::open wrapper that records file to close after test finishes
    auto open(std::filesystem::path path) {
        auto f = this->fs.open(path).get();
        open_files.push_back(f);
        return f;
    }

    T fs;
    seastar::tmp_dir dir;
    std::filesystem::path path;
    int count{0};
    std::vector<seastar::shared_ptr<io::persistence::file>> open_files;
};

/*
 * used to create memory file system types with different default alignments
 * that can be used to drive the TYPED_TEST_SUITE below.
 */
template<uint64_t read, uint64_t write, uint64_t memory>
class memfs_align : public io::memory_persistence {
public:
    memfs_align()
      : memory_persistence({read, write, memory}) {}
};

using PersistenceTypes = ::testing::Types<
  io::disk_persistence,
  io::memory_persistence,
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers)
  memfs_align<512, 1024, 8192>,
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers)
  memfs_align<16_KiB, 32_KiB, 64_KiB>,
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers)
  memfs_align<4096, 1024, 8192>,
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers)
  memfs_align<4096, 1024, 2048>,
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers)
  memfs_align<512, 2048, 1024>>;

TYPED_TEST_SUITE(PersistenceTest, PersistenceTypes);

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define EXPECT_THROW_WITH_ALIGNMENT_WARNING(expression, exception)             \
    EXPECT_THROW(expression, exception)                                        \
      << "Expected failure due to alignment. Are you running tests on "        \
         "`tmpfs` or another file system that doesn't enforce direct I/O "     \
         "alignemnt?";

TYPED_TEST(PersistenceTest, Create) { EXPECT_TRUE(this->create(this->path)); }

TYPED_TEST(PersistenceTest, CreateAlreadyExists) {
    this->create(this->path);
    EXPECT_THROW(
      this->fs.create(this->path).get(), std::filesystem::filesystem_error);
}

TYPED_TEST(PersistenceTest, Open) {
    this->create(this->path);
    EXPECT_TRUE(this->open(this->path));
}

TYPED_TEST(PersistenceTest, OpenDoesNotExist) {
    EXPECT_THROW(
      this->fs.open(this->path).get(), std::filesystem::filesystem_error);
}

TYPED_TEST(PersistenceTest, WriteOffsetAlignment) {
    auto f = this->create(this->path);

    // normal alignment and size
    const auto buf = this->fs.allocate(
      f->memory_dma_alignment(), f->disk_write_dma_alignment());

    EXPECT_NO_THROW(f->dma_write(0, buf.get(), buf.size()).get());

    // write offset is not aligned
    EXPECT_THROW_WITH_ALIGNMENT_WARNING(
      f->dma_write(1, buf.get(), buf.size()).get(), std::system_error);
}

TYPED_TEST(PersistenceTest, WriteMemoryAlignment) {
    auto f = this->create(this->path);

    // correct memory alignment, extra space allocated for adjustments
    const auto buf = this->fs.allocate(
      f->memory_dma_alignment(), f->disk_write_dma_alignment() + 1);

    // correct memory alignment, correct size alignment
    EXPECT_NO_THROW(f->dma_write(0, buf.get(), buf.size() - 1).get());

    // incorrect memory alignment, correct size alignment
    EXPECT_THROW_WITH_ALIGNMENT_WARNING(
      f->dma_write(0, buf.get() + 1, buf.size() - 1).get(), std::system_error);
}

TYPED_TEST(PersistenceTest, WriteSizeAlignment) {
    auto f = this->create(this->path);

    // normal alignment and size
    const auto buf = this->fs.allocate(
      f->memory_dma_alignment(), f->disk_write_dma_alignment());

    EXPECT_NO_THROW(f->dma_write(0, buf.get(), buf.size()).get());

    // write size is not aligned
    EXPECT_THROW_WITH_ALIGNMENT_WARNING(
      f->dma_write(0, buf.get(), buf.size() - 1).get(), std::system_error);
}

TYPED_TEST(PersistenceTest, ReadOffsetAlignment) {
    auto f = this->create(this->path);

    {
        // need data to make it to alignment checks in read path. need at least
        // read size, but it also needs to be write aligned
        const auto size = seastar::align_up(
          f->disk_read_dma_alignment(), f->disk_write_dma_alignment());
        const auto buf = this->fs.allocate(f->memory_dma_alignment(), size);
        f->dma_write(0, buf.get(), buf.size()).get();
    }

    // normal alignment and size
    auto buf = this->fs.allocate(
      f->memory_dma_alignment(), f->disk_read_dma_alignment());

    EXPECT_NO_THROW(f->dma_read(0, buf.get_write(), buf.size()).get());

    // read offset is not aligned
    EXPECT_THROW_WITH_ALIGNMENT_WARNING(
      f->dma_read(1, buf.get_write(), buf.size()).get(), std::system_error);
}

TYPED_TEST(PersistenceTest, ReadMemoryAlignment) {
    auto f = this->create(this->path);

    {
        // need data to make it to alignment checks in read path. need at least
        // read size, but it also needs to be write aligned
        const auto size = seastar::align_up(
          f->disk_read_dma_alignment() + 1, f->disk_write_dma_alignment());
        const auto buf = this->fs.allocate(f->memory_dma_alignment(), size);
        f->dma_write(0, buf.get(), buf.size()).get();
    }

    auto buf = this->fs.allocate(
      f->memory_dma_alignment(), f->disk_read_dma_alignment() + 1);

    EXPECT_NO_THROW(f->dma_read(0, buf.get_write(), buf.size() - 1).get());

    // read memory is not aligned
    EXPECT_THROW_WITH_ALIGNMENT_WARNING(
      f->dma_read(0, buf.get_write() + 1, buf.size() - 1).get(),
      std::system_error);
}

TYPED_TEST(PersistenceTest, ReadSizeAlignment) {
    auto f = this->create(this->path);

    {
        // need data to make it to alignment checks in read path. need at least
        // read size, but it also needs to be write aligned
        const auto size = seastar::align_up(
          f->disk_read_dma_alignment(), f->disk_write_dma_alignment());
        const auto buf = this->fs.allocate(f->memory_dma_alignment(), size);
        f->dma_write(0, buf.get(), buf.size()).get();
    }

    auto buf = this->fs.allocate(
      f->memory_dma_alignment(), f->disk_read_dma_alignment());

    EXPECT_NO_THROW(f->dma_read(0, buf.get_write(), buf.size()).get());

    // read size is not aligned
    EXPECT_THROW_WITH_ALIGNMENT_WARNING(
      f->dma_read(0, buf.get_write(), buf.size() - 1).get(), std::system_error);
}

TYPED_TEST(PersistenceTest, Close) {
    auto f = this->fs.create(this->path).get();
    f->close().get();
}

TYPED_TEST(PersistenceTest, ReadWrite) {
    constexpr auto test_size = 64_KiB;

    auto f = this->create(this->path);

    for (auto write_len = f->disk_write_dma_alignment(); write_len <= test_size;
         write_len += f->disk_write_dma_alignment()) {
        /*
         * for a particular write length, write the entire file
         */
        const auto input = make_random_data(test_size).get();
        for (uint64_t offset = 0; offset < test_size;) {
            auto length = std::min(write_len, test_size - offset);
            auto tmp = this->fs.allocate(f->memory_dma_alignment(), length);
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            std::memcpy(tmp.get_write(), input.get() + offset, tmp.size());
            auto size = f->dma_write(offset, tmp.get(), tmp.size()).get();
            offset += size;
        }

        /*
         * for a particular read length, read the entire file
         */
        for (auto read_len = f->disk_read_dma_alignment();
             read_len <= test_size;
             read_len += f->disk_read_dma_alignment()) {
            auto output = make_random_data(input.size()).get();
            EXPECT_NE(input, output);

            for (uint64_t offset = 0; offset < test_size;) {
                auto length = std::min(read_len, test_size - offset);
                auto tmp = this->fs.allocate(f->memory_dma_alignment(), length);
                auto size
                  = f->dma_read(offset, tmp.get_write(), tmp.size()).get();
                // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
                std::memcpy(output.get_write() + offset, tmp.get(), tmp.size());
                offset += size;
            }

            /*
             * data read should be the same as that which was written
             */
            EXPECT_EQ(input, output);
        }
    }
}

class fault_injection : std::exception {};
class fault_injection2 : std::exception {};

TYPED_TEST(PersistenceTest, ThrowCreate) {
    this->fs.fail_next_create(std::make_exception_ptr(fault_injection()));
    EXPECT_THROW(this->create(this->path), fault_injection);

    this->fs.fail_next_create(fault_injection2());
    EXPECT_THROW(this->create(this->path), fault_injection2);

    EXPECT_NO_THROW(this->create(this->path));
}

TYPED_TEST(PersistenceTest, ThrowOpen) {
    this->create(this->path);

    this->fs.fail_next_open(std::make_exception_ptr(fault_injection()));
    EXPECT_THROW(this->open(this->path), fault_injection);

    this->fs.fail_next_open(fault_injection2());
    EXPECT_THROW(this->open(this->path), fault_injection2);

    EXPECT_NO_THROW(this->open(this->path));
}

TYPED_TEST(PersistenceTest, ThrowRead) {
    auto f = this->create(this->path);
    auto tmp = this->fs.allocate(
      f->memory_dma_alignment(), f->disk_read_dma_alignment());

    f->fail_next_read(std::make_exception_ptr(fault_injection()));
    EXPECT_THROW(
      f->dma_read(0, tmp.get_write(), tmp.size()).get(), fault_injection);

    f->fail_next_read(fault_injection2());
    EXPECT_THROW(
      f->dma_read(0, tmp.get_write(), tmp.size()).get(), fault_injection2);

    EXPECT_NO_THROW(f->dma_read(0, tmp.get_write(), tmp.size()).get());
}

TYPED_TEST(PersistenceTest, ThrowWrite) {
    auto f = this->create(this->path);
    auto tmp = this->fs.allocate(
      f->memory_dma_alignment(), f->disk_write_dma_alignment());

    f->fail_next_write(std::make_exception_ptr(fault_injection()));
    EXPECT_THROW(f->dma_write(0, tmp.get(), tmp.size()).get(), fault_injection);

    f->fail_next_write(fault_injection2());
    EXPECT_THROW(
      f->dma_write(0, tmp.get(), tmp.size()).get(), fault_injection2);

    EXPECT_NO_THROW(f->dma_write(0, tmp.get(), tmp.size()).get());
}

TYPED_TEST(PersistenceTest, ThrowClose) {
    auto f = this->fs.create(this->path).get();

    f->fail_next_close(std::make_exception_ptr(fault_injection()));
    EXPECT_THROW(f->close().get(), fault_injection);

    f->fail_next_close(fault_injection2());
    EXPECT_THROW(f->close().get(), fault_injection2);

    EXPECT_NO_THROW(f->close().get());
}

TEST(MemoryPersistenceTest, CustomAlignment) {
    std::vector<seastar::shared_ptr<io::persistence::file>> open_files;
    auto cleanup = seastar::defer([&open_files] {
        for (auto& file : open_files) {
            file->close().get();
        }
    });

    // default alignments
    io::memory_persistence dfs;
    auto df = dfs.create("file").get();
    open_files.push_back(df);

    // custom alignments
    io::memory_persistence cfs({1, 2, 3});
    auto cf = cfs.create("file").get();
    open_files.push_back(cf);

    EXPECT_NE(cf->disk_read_dma_alignment(), df->disk_read_dma_alignment());
    EXPECT_NE(cf->disk_write_dma_alignment(), df->disk_write_dma_alignment());
    EXPECT_NE(cf->memory_dma_alignment(), df->memory_dma_alignment());

    EXPECT_EQ(cf->disk_read_dma_alignment(), 1);
    EXPECT_EQ(cf->disk_write_dma_alignment(), 2);
    EXPECT_EQ(cf->memory_dma_alignment(), 3);
}
