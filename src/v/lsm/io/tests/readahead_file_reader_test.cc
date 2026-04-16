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

#include "bytes/ioarray.h"
#include "lsm/io/persistence.h"
#include "lsm/io/readahead_file_reader.h"

#include <seastar/core/coroutine.hh>

#include <gtest/gtest.h>

#include <random>

// NOLINTBEGIN(*magic-numbers*)

using namespace lsm::io;

namespace {

/// A fake file reader backed by an in-memory buffer with a known byte pattern.
/// Tracks the number of read calls and total bytes read so tests can verify
/// that readahead reduces I/O.
class fake_file_reader final : public random_access_file_reader {
public:
    explicit fake_file_reader(size_t size)
      : _size(size) {
        // Fill with a deterministic pattern: byte i = i & 0xFF
        iobuf b;
        for (size_t i = 0; i < size; ++i) {
            char c = static_cast<char>(i & 0xFF);
            b.append(&c, 1);
        }
        _data = ioarray::copy_from(b);
    }

    ss::future<ioarray> read(size_t offset, size_t n) override {
        ++_read_count;
        _bytes_read += n;
        co_return _data.share(offset, n);
    }

    ss::future<> close() override { co_return; }

    fmt::iterator format_to(fmt::iterator it) const override {
        return fmt::format_to(it, "{{fake_file_reader size={}}}", _size);
    }

    size_t file_size() const { return _size; }
    size_t read_count() const { return _read_count; }
    size_t bytes_read() const { return _bytes_read; }

    /// Verify that the given ioarray matches the expected pattern at offset.
    void verify(const ioarray& arr, size_t offset) const {
        for (size_t i = 0; i < arr.size(); ++i) {
            ASSERT_EQ(static_cast<char>((offset + i) & 0xFF), arr[i])
              << "mismatch at file offset " << (offset + i);
        }
    }

private:
    size_t _size;
    ioarray _data;
    size_t _read_count{0};
    size_t _bytes_read{0};
};

} // namespace

TEST(ReadaheadFileReader, FullBufferHit) {
    // A single read should populate the buffer. A subsequent read within the
    // buffered range should not hit the underlying reader.
    fake_file_reader inner(1_MiB);
    readahead_file_reader ra(&inner, inner.file_size(), 256_KiB);

    auto data = ra.read(0, 16_KiB).get();
    inner.verify(data, 0);
    EXPECT_EQ(16_KiB, data.size());

    size_t reads_after_first = inner.read_count();

    // This should be served entirely from the buffer.
    auto data2 = ra.read(16_KiB, 16_KiB).get();
    inner.verify(data2, 16_KiB);
    EXPECT_EQ(reads_after_first, inner.read_count());

    ra.close().get();
}

TEST(ReadaheadFileReader, SequentialReadsReduceIO) {
    // Reading 16_KiB blocks sequentially with 256_KiB readahead should issue
    // far fewer reads than one per block.
    fake_file_reader inner(1_MiB);
    readahead_file_reader ra(&inner, inner.file_size(), 256_KiB);

    size_t block_size = 16_KiB;
    size_t num_blocks = inner.file_size() / block_size;
    for (size_t i = 0; i < num_blocks; ++i) {
        auto data = ra.read(i * block_size, block_size).get();
        inner.verify(data, i * block_size);
    }

    // With 256_KiB readahead, ~1_MiB / (256_KiB + alignment overhead) reads.
    // Without readahead it would be 64 reads. We should be well under that.
    EXPECT_LT(inner.read_count(), num_blocks / 2);

    ra.close().get();
}

TEST(ReadaheadFileReader, PartialBufferHitExtends) {
    // A read that starts within the buffer but extends past it should only
    // read the missing portion (not re-read what's buffered).
    fake_file_reader inner(1_MiB);
    readahead_file_reader ra(&inner, inner.file_size(), 128_KiB);

    // First read populates the buffer
    auto d1 = ra.read(0, 16_KiB).get();
    inner.verify(d1, 0);
    size_t bytes_after_first = inner.bytes_read();

    // This read should extend past the initial buffer. The readahead reader
    // should concat rather than re-read from the start.
    auto d2 = ra.read(0, 512_KiB).get();
    inner.verify(d2, 0);

    // The additional bytes read should be roughly 512_KiB - buffer_already_had,
    // not a full re-read of 512_KiB + readahead from offset 0.
    size_t additional_bytes = inner.bytes_read() - bytes_after_first;
    EXPECT_LT(additional_bytes, 512_KiB + 256_KiB);

    ra.close().get();
}

TEST(ReadaheadFileReader, BufferMissResetsBuffer) {
    // A read that starts past the buffer should discard the old buffer and
    // issue a fresh aligned read.
    fake_file_reader inner(1_MiB);
    readahead_file_reader ra(&inner, inner.file_size(), 128_KiB);

    auto d1 = ra.read(0, 16_KiB).get();
    inner.verify(d1, 0);

    // Jump far ahead - this is a buffer miss.
    auto d2 = ra.read(800_KiB, 16_KiB).get();
    inner.verify(d2, 800_KiB);

    // The old buffer at offset 0 is gone. Reading from 0 again hits disk.
    size_t reads_before = inner.read_count();
    auto d3 = ra.read(0, 16_KiB).get();
    inner.verify(d3, 0);
    EXPECT_GT(inner.read_count(), reads_before);

    ra.close().get();
}

TEST(ReadaheadFileReader, ReadAtEndOfFile) {
    // Reads near the end of file should be clamped and not overshoot.
    size_t file_size = 500_KiB;
    fake_file_reader inner(file_size);
    readahead_file_reader ra(&inner, file_size, 256_KiB);

    auto data = ra.read(file_size - 100, 100).get();
    EXPECT_EQ(100, data.size());
    inner.verify(data, file_size - 100);

    ra.close().get();
}

TEST(ReadaheadFileReader, SingleByteReads) {
    // Reading one byte at a time should still work correctly and benefit
    // from readahead.
    fake_file_reader inner(64_KiB);
    readahead_file_reader ra(&inner, inner.file_size(), 64_KiB);

    for (size_t i = 0; i < inner.file_size(); ++i) {
        auto data = ra.read(i, 1).get();
        ASSERT_EQ(1, data.size());
        ASSERT_EQ(static_cast<char>(i & 0xFF), data[0]) << "i=" << i;
    }

    // Should need very few underlying reads for the whole file.
    EXPECT_LE(inner.read_count(), 3);

    ra.close().get();
}

TEST(ReadaheadFileReader, ExactFileSize) {
    // Reading the entire file in one go.
    size_t file_size = 300_KiB;
    fake_file_reader inner(file_size);
    readahead_file_reader ra(&inner, file_size, 128_KiB);

    auto data = ra.read(0, file_size).get();
    EXPECT_EQ(file_size, data.size());
    inner.verify(data, 0);

    ra.close().get();
}

TEST(ReadaheadFileReader, RandomAccessPattern) {
    // Random access should always return correct data, even if the buffer
    // is invalidated frequently.
    size_t file_size = 1_MiB;
    fake_file_reader inner(file_size);
    readahead_file_reader ra(&inner, file_size, 128_KiB);

    std::mt19937 rng(42);
    std::uniform_int_distribution<size_t> off_dist(0, file_size - 1);

    for (int i = 0; i < 200; ++i) {
        size_t offset = off_dist(rng);
        size_t max_n = file_size - offset;
        std::uniform_int_distribution<size_t> len_dist(
          1, std::min(max_n, 64_KiB));
        size_t n = len_dist(rng);

        auto data = ra.read(offset, n).get();
        ASSERT_EQ(n, data.size()) << "iter=" << i;
        inner.verify(data, offset);
    }

    ra.close().get();
}

TEST(ReadaheadFileReader, ZeroReadahead) {
    // With readahead_size=0, reads are still aligned to max_chunk_size so
    // some coalescing occurs. But each aligned region should be read at most
    // once. 256_KiB / 128_KiB = 2 aligned regions.
    fake_file_reader inner(256_KiB);
    readahead_file_reader ra(&inner, inner.file_size(), 0);

    for (size_t i = 0; i < 16; ++i) {
        auto data = ra.read(i * 16_KiB, 16_KiB).get();
        inner.verify(data, i * 16_KiB);
    }

    // 256_KiB file, 128_KiB alignment → 2 aligned reads
    EXPECT_EQ(2, inner.read_count());

    ra.close().get();
}

TEST(ReadaheadFileReader, SmallFile) {
    // A file smaller than the readahead size.
    fake_file_reader inner(100);
    readahead_file_reader ra(&inner, inner.file_size(), 256_KiB);

    auto d1 = ra.read(0, 50).get();
    inner.verify(d1, 0);

    auto d2 = ra.read(50, 50).get();
    inner.verify(d2, 50);

    // Entire file should be buffered after the first read.
    EXPECT_EQ(1, inner.read_count());

    ra.close().get();
}

TEST(ReadaheadFileReader, FullyConsumedBufferThenNewRead) {
    // When a read fully consumes the buffer, _buf_off must be updated so
    // the next read doesn't incorrectly match the stale buffer position.
    fake_file_reader inner(1_MiB);
    readahead_file_reader ra(&inner, inner.file_size(), 0);

    // Read exactly one aligned chunk — this fills then fully consumes the
    // buffer
    auto d1 = ra.read(0, 128_KiB).get();
    inner.verify(d1, 0);

    // A second read at a different offset must get correct data, not be
    // confused by stale _buf_off.
    auto d2 = ra.read(256_KiB, 128_KiB).get();
    inner.verify(d2, 256_KiB);

    ra.close().get();
}

TEST(ReadaheadFileReader, BufferDoesNotGrowUnbounded) {
    // Sequential reads should trim consumed data from the front, keeping
    // the buffer bounded to roughly the readahead size, not the whole file.
    size_t file_size = 4_MiB;
    size_t readahead = 256_KiB;
    fake_file_reader inner(file_size);
    readahead_file_reader ra(&inner, file_size, readahead);

    size_t block_size = 16_KiB;
    for (size_t off = 0; off + block_size <= file_size; off += block_size) {
        auto data = ra.read(off, block_size).get();
        inner.verify(data, off);
    }

    // The total bytes read from the underlying reader should be close to the
    // file size (each byte read ~once), not multiples of it.
    EXPECT_LE(
      inner.bytes_read(), file_size + readahead + ioarray::max_chunk_size);

    ra.close().get();
}

// NOLINTEND(*magic-numbers*)
