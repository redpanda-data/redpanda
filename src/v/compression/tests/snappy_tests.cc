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
#include "compression/internal/snappy_java_compressor.h"
#include "compression/snappy_standard_compressor.h"
#include "random/generators.h"
#include "snappy-sinksource.h"
#include "snappy.h"
#include "utils/file_io.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/temporary_buffer.hh>

#include <gtest/gtest.h>

#include <cstdlib>

TEST(SnappyTest, CompressAndDecompressSnappyStandardTest) {
    const auto data = random_generators::gen_alphanum_string(512);

    iobuf buf;
    buf.append(data.data(), data.size());
    auto compressed_buf = compression::snappy_standard_compressor::compress(
      buf);
    auto decompressed_buf = compression::snappy_standard_compressor::uncompress(
      compressed_buf);
    EXPECT_EQ(buf, decompressed_buf);
}

TEST(SnappyTest, CompressAndDecompressSnappyJavaTest) {
    const auto data = random_generators::gen_alphanum_string(512);

    iobuf buf;
    buf.append(data.data(), data.size());
    auto compressed_buf
      = compression::internal::snappy_java_compressor::compress(buf);
    auto decompressed_buf
      = compression::internal::snappy_java_compressor::uncompress(
        compressed_buf);
    EXPECT_EQ(buf, decompressed_buf);
}

TEST(SnappyTest, SnappyStandardIsValidCompressedTest) {
    const auto data = random_generators::gen_alphanum_string(512);

    iobuf buf;
    buf.append(data.data(), data.size());
    auto compressed_buf = compression::snappy_standard_compressor::compress(
      buf);

    EXPECT_EQ(
      snappy::IsValidCompressedBuffer(
        compressed_buf.begin()->get(), compressed_buf.size_bytes()),
      true);
}

TEST(SnappyTest, CompressedVersionHeadersSnappyJavaTest) {
    // snappy-java uses big-endian format to encode headers. See:
    // https://github.com/xerial/snappy-java/blob/65e1ec3de1a0d447b137c6dd6393629aa3d75b8b/src/main/java/org/xerial/snappy/SnappyOutputStream.java#L343-L349
    // https://github.com/xerial/snappy-java/blob/65e1ec3de1a0d447b137c6dd6393629aa3d75b8b/src/main/java/org/xerial/snappy/SnappyCodec.java#L78-L81
    const auto data = random_generators::gen_alphanum_string(512);

    iobuf buf;
    buf.append(data.data(), data.size());
    auto compressed_buf
      = compression::internal::snappy_java_compressor::compress(buf);
    auto magic_buf = ss::temporary_buffer<char>(
      compression::internal::snappy_java_compressor::snappy_magic::java_magic
        .size());
    auto compressed_frag = compressed_buf.begin();

    // Check the magic header
    EXPECT_EQ(
      std::memcmp(
        compressed_frag->get(),
        compression::internal::snappy_java_compressor::snappy_magic::java_magic
          .data(),
        compression::internal::snappy_java_compressor::snappy_magic::java_magic
          .size()),
      0);
    compressed_frag->trim_front(
      compression::internal::snappy_java_compressor::snappy_magic::java_magic
        .size());

    // Check the default version
    auto be_default_version = ss::cpu_to_be(
      compression::internal::snappy_java_compressor::snappy_magic::
        default_version);
    EXPECT_EQ(
      std::memcmp(
        compressed_frag->get(),
        reinterpret_cast<const char*>(&be_default_version),
        sizeof(compression::internal::snappy_java_compressor::snappy_magic::
                 default_version)),
      0);

    compressed_frag->trim_front(
      sizeof(compression::internal::snappy_java_compressor::snappy_magic::
               default_version));

    // Check the compat version
    auto be_compat_version = ss::cpu_to_be(
      compression::internal::snappy_java_compressor::snappy_magic::
        min_compatible_version);
    EXPECT_EQ(
      std::memcmp(
        compressed_frag->get(),
        reinterpret_cast<const char*>(&be_compat_version),
        sizeof(compression::internal::snappy_java_compressor::snappy_magic::
                 min_compatible_version)),
      0);

    compressed_frag->trim_front(
      sizeof(compression::internal::snappy_java_compressor::snappy_magic::
               min_compatible_version));

    // Check the size of the compressed payload
    int32_t be_compressed_size{};
    std::memcpy(
      &be_compressed_size, compressed_frag->get(), sizeof(be_compressed_size));
    int32_t compressed_size = ss::be_to_cpu(be_compressed_size);
    compressed_frag->trim_front(sizeof(compressed_size));
    EXPECT_EQ(compressed_size, compressed_frag->size());

    // Get the size of the decompressed payload
    snappy::ByteArraySource compressed_source(
      compressed_frag->get(), compressed_size);
    uint32_t decompressed_size;
    snappy::GetUncompressedLength(&compressed_source, &decompressed_size);
    EXPECT_EQ(decompressed_size, data.size());
    compressed_frag->trim_front(sizeof(decompressed_size));
}

namespace {

iobuf gen_iobuf(size_t size, bool compressible) {
    iobuf ret;
    if (compressible) {
        const auto data = random_generators::gen_alphanum_string(512);
        while (ret.size_bytes() < size) {
            ret.append(
              data.data(), std::min(data.size(), size - ret.size_bytes()));
        }
    } else {
        while (ret.size_bytes() < size) {
            const auto data = random_generators::gen_alphanum_string(
              std::min<size_t>(4096, size - ret.size_bytes()));
            ret.append(data.data(), data.size());
        }
    }
    return ret;
}

// input sizes covering the snappy::kBlockSize (64KiB) and iobuf max fragment
// (128KiB) boundaries
constexpr std::array<size_t, 9> boundary_sizes = {
  0,
  1,
  100,
  64_KiB - 1,
  64_KiB,
  64_KiB + 1,
  128_KiB,
  128_KiB + 1,
  1_MiB,
};

} // namespace

TEST(SnappyTest, RoundTripBlockBoundariesSnappyJavaTest) {
    for (size_t size : boundary_sizes) {
        for (bool compressible : {true, false}) {
            auto buf = gen_iobuf(size, compressible);
            auto compressed
              = compression::internal::snappy_java_compressor::compress(buf);
            auto decompressed
              = compression::internal::snappy_java_compressor::uncompress(
                compressed);
            EXPECT_EQ(buf, decompressed)
              << "size=" << size << " compressible=" << compressible;
        }
    }
}

// Differential check of the compressor's snappy-internal.h based
// implementation (a caller-owned WorkingMemory driving CompressFragment)
// against the public one-shot API: each java-framed block must be
// byte-identical to what snappy::RawCompress produces for the same input
// block, since both emit a varint32 uncompressed-length prefix followed by
// the same block compression. If this fails after a snappy version bump, the
// internal contracts drifted; see the static_assert in
// snappy_java_compressor.cc.
TEST(SnappyTest, DifferentialVsPublicApiSnappyJavaTest) {
    using compressor = compression::internal::snappy_java_compressor;
    for (size_t size : boundary_sizes) {
        for (bool compressible : {true, false}) {
            auto buf = gen_iobuf(size, compressible);

            iobuf expected;
            expected.append(
              compressor::snappy_magic::java_magic.data(),
              compressor::snappy_magic::java_magic.size());
            auto be_version = ss::cpu_to_be(
              compressor::snappy_magic::default_version);
            expected.append(
              reinterpret_cast<const char*>(&be_version), sizeof(be_version));
            auto be_compat = ss::cpu_to_be(
              compressor::snappy_magic::min_compatible_version);
            expected.append(
              reinterpret_cast<const char*>(&be_compat), sizeof(be_compat));
            ss::temporary_buffer<char> obuf(
              snappy::MaxCompressedLength(snappy::kBlockSize));
            for (const auto& frag : buf) {
                for (size_t offset = 0; offset < frag.size();
                     offset += snappy::kBlockSize) {
                    const size_t block_len = std::min(
                      snappy::kBlockSize, frag.size() - offset);
                    size_t compressed_len = obuf.size();
                    snappy::RawCompress(
                      frag.get() + offset,
                      block_len,
                      obuf.get_write(),
                      &compressed_len);
                    auto be_len = ss::cpu_to_be(int32_t(compressed_len));
                    expected.append(
                      reinterpret_cast<const char*>(&be_len), sizeof(be_len));
                    expected.append(obuf.get(), compressed_len);
                }
            }

            EXPECT_EQ(compressor::compress(buf), expected)
              << "size=" << size << " compressible=" << compressible;
        }
    }
}

TEST(SnappyTest, LittleEndianHeadersBackwardsCompatibilitySnappyJavaTest) {
    // Previously, version fields were erroneously written with
    // little-endian encoding. They are now corrected to be written and decoded
    // using big-endian, but we must retain backwards compatibility here with
    // the existing, improperly encoded batches (as version, min_version fields
    // with value 1 will decode to the value 16777216).
    // See: https://github.com/redpanda-data/redpanda/issues/25091
    auto snappy_payload_path = std::getenv("SNAPPY_PAYLOAD_PATH");
    vassert(snappy_payload_path, "expected value for payload path");
    auto root = std::filesystem::path(snappy_payload_path);

    // The original, uncompressed data.
    auto expected_decompressed_file = root / "uncompressed_data";
    EXPECT_TRUE(ss::file_exists(expected_decompressed_file.c_str()).get());
    auto expected_decompressed_buffer
      = read_fully(expected_decompressed_file.native()).get();

    // A payload that was previously compressed by redpanda, with version
    // headers in little-endian encoding.
    auto le_compressed_file = root / "little_endian_compressed_data.snappy";
    EXPECT_TRUE(ss::file_exists(le_compressed_file.c_str()).get());
    auto le_compressed_buffer = read_fully(le_compressed_file.native()).get();

    auto decompressed_buffer
      = compression::internal::snappy_java_compressor::uncompress(
        le_compressed_buffer);
    EXPECT_EQ(decompressed_buffer, expected_decompressed_buffer);
}
