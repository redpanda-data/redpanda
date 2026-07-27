// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "compression/internal/snappy_java_compressor.h"

#include "base/likely.h"
#include "bytes/details/io_iterator_consumer.h"
#include "compression/snappy_standard_compressor.h"

#include <seastar/core/temporary_buffer.hh>

#include <cstring>
#include <snappy-internal.h>
#include <snappy-stubs-internal.h>
#include <snappy.h>

namespace compression::internal {

// We drive snappy's internal block compressor directly instead of calling
// snappy::RawCompress per block: the public one-shot API allocates a ~`200KiB`
// `WorkingMemory` object internally on every call, tripping the
// large-allocation warning threshold on hot paths like produce-time
// The public `snappy` API currently offers no way to reuse this working memory:
// https://github.com/google/snappy/blob/1.2.2/snappy.cc#L1813.
// So, instead of using those public APIs, we reach into `snappy` internals,
// allocate our own thread local `WorkingMemory` object at startup, and reuse it
// across all compression calls. This also involves hand-rolling parts of
// `snappy::RawCompress()`.
//
// There are several contracts we must uphold for CompressFragment:
// https://github.com/google/snappy/blob/1.2.2/snappy-internal.h#L142-L157.
//   - input_length <= kBlockSize
//   - output buffer >= MaxCompressedLength(input_length)
//   - hash table zeroed, size a power of two (GetHashTable guarantees both)
static_assert(
  SNAPPY_MAJOR == 1 && SNAPPY_MINOR == 2 && SNAPPY_PATCHLEVEL == 2,
  "snappy version changed: re-audit the internal-header usage in this file "
  "(WorkingMemory, CompressFragment and Varint contracts) and the BUILD.bazel "
  "hunks in bazel/thirdparty/snappy-export-internal-h.patch before bumping");

namespace {

/// Scratch space for compression, reused across calls on this shard. Holds
/// snappy's working memory (hash table + scratch buffers, ~203KiB) and a
/// staging buffer for one compressed block (~75KiB). Allocated once at
/// startup via init_workspace().
class snappy_workspace {
private:
    /// Upper bound on one framed block in the staging buffer: a varint32
    /// uncompressed-length prefix, followed by the compressed block data for
    /// which CompressFragment's contract demands room for, which is
    /// `MaxCompressedLength(input)` bytes.
    /// `snappy::Compress()` composes the same bound, with the varint32 prefix
    /// in a separate stack buffer instead:
    /// https://github.com/google/snappy/blob/1.2.2/snappy.cc#L1808-L1810).
    static size_t max_framed_block_len() {
        return snappy::Varint::kMax32
               + snappy::MaxCompressedLength(snappy::kBlockSize);
    }

public:
    snappy_workspace()
      : _staging(max_framed_block_len()) {}

    /// Compress one block of at most `snappy::kBlockSize` bytes into the
    /// staging buffer. This mirrors the prologue and per-block body of
    /// `snappy::Compress()`, with the WorkingMemory hoisted out into this
    /// class: https://github.com/google/snappy/blob/1.2.2/snappy.cc#L1801-L1877
    ///
    /// Returns the framed size.
    size_t compress_block(const char* input, size_t len) {
        char* dst = _staging.get_write();
        char* op = snappy::Varint::Encode32(dst, static_cast<uint32_t>(len));
        int table_size = 0;
        uint16_t* table = _wmem.GetHashTable(len, &table_size);
        char* end = snappy::internal::CompressFragment(
          input, len, op, table, table_size);
        return end - dst;
    }

    const char* data() const { return _staging.get(); }

private:
    snappy::internal::WorkingMemory _wmem{snappy::kBlockSize};
    ss::temporary_buffer<char> _staging;
};

thread_local std::unique_ptr<snappy_workspace> shard_workspace;

snappy_workspace& get_workspace() {
    if (unlikely(!shard_workspace)) {
        shard_workspace = std::make_unique<snappy_workspace>();
    }
    return *shard_workspace;
}

} // namespace

void snappy_java_compressor::init_workspace() { get_workspace(); }

template<typename T, typename = std::enable_if_t<std::is_integral_v<T>, T>>
void append_be(iobuf& o, T t) {
    auto x = ss::cpu_to_be(t);
    // NOLINTNEXTLINE
    o.append((const char*)&x, sizeof(x));
}
template<typename T, typename = std::enable_if_t<std::is_integral_v<T>, T>>
void append_le(iobuf& o, T t) {
    auto x = ss::cpu_to_le(t);
    // NOLINTNEXTLINE
    o.append((const char*)&x, sizeof(x));
}
iobuf snappy_java_compressor::compress(const iobuf& x) {
    auto& wksp = get_workspace();
    iobuf ret;
    ret.append(
      snappy_magic::java_magic.data(), snappy_magic::java_magic.size());
    // versions in header are big-endian. See:
    // https://github.com/xerial/snappy-java/blob/65e1ec3de1a0d447b137c6dd6393629aa3d75b8b/src/main/java/org/xerial/snappy/SnappyCodec.java#L78-L81
    append_be(ret, snappy_magic::default_version);
    append_be(ret, snappy_magic::min_compatible_version);
    for (const auto& f : x) {
        // chop fragments at kBlockSize, as snappy::Compress does internally
        // (`CompressFragment()` rejects anything larger):
        // https://github.com/google/snappy/blob/1.2.2/snappy.cc#L1815-L1877
        for (size_t offset = 0; offset < f.size();
             offset += snappy::kBlockSize) {
            const size_t block_len = std::min(
              snappy::kBlockSize, f.size() - offset);
            const size_t framed = wksp.compress_block(
              f.get() + offset, block_len);
            // must be int32 to be compatible && in big endian
            append_be(ret, int32_t(framed));
            ret.append(wksp.data(), framed);
        }
    }
    return ret;
}
iobuf snappy_java_compressor::uncompress(const iobuf& x) {
    auto iter = details::io_iterator_consumer(x.cbegin(), x.cend());
    if (unlikely(x.size_bytes() < snappy_magic::header_len)) {
        return snappy_standard_compressor::uncompress(x);
    }
    std::array<uint8_t, snappy_magic::java_magic.size()> magic_compare{};
    iter.consume_to(magic_compare.size(), magic_compare.data());
    if (unlikely(snappy_magic::java_magic != magic_compare)) {
        return snappy_standard_compressor::uncompress(x);
    }
    // Previously, these version fields were erroneously written with
    // little-endian encoding. They are now corrected to be written and decoded
    // using big-endian. Additionally, there was previously a version check
    // here. It has been removed due to incorrect implementation, and because
    // most other snappy clients do not perform checks around these fields.
    [[maybe_unused]] const auto version = iter.consume_be_type<int32_t>();
    [[maybe_unused]] const auto min_version = iter.consume_be_type<int32_t>();
    // stream decoder next
    iobuf ret;
    const size_t input_bytes = x.size_bytes();
    while (iter.bytes_consumed() != input_bytes) {
        auto compressed_length = iter.consume_be_type<int32_t>();
        // iobuf doesn't have a const compatible share interface so we make a
        // copy here which is inefficient compared to a zero-copy approach.
        auto chunk = iobuf_copy(iter, compressed_length);
        auto output_size = snappy_standard_compressor::get_uncompressed_length(
          chunk);
        snappy_standard_compressor::uncompress_append(chunk, ret, output_size);
    }
    return ret;
}

} // namespace compression::internal
