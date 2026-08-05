/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

// Compares google/crc32c against the CRC32C abseil ships as
// absl::ExtendCrc32c and absl::MemcpyCrc32c.
//
// crc::crc32c is backed by abseil, so it is not benchmarked here; the google
// side calls crc32c::Extend directly. This is kept as the evidence for that
// choice and to re-evaluate it -- notably once abseil's CPU detection covers
// this host, which currently falls back to no PCLMULQDQ streams.
//
// Every case reports the cost of *one whole checksum*, not one byte: perf_tests
// never scales below nanoseconds, and per-byte figures round to two decimals
// where they are all noise. The buffer size is in the group name, so throughput
// is size/runtime.
//
// Successive iterations checksum independent copies of the same hot buffer, so
// the out-of-order engine overlaps them. These are throughput numbers, not the
// latency of a checksum in isolation.

#include "absl/crc/crc32c.h"
#include "base/units.h"
#include "base/vassert.h"
#include "bytes/bytes.h"
#include "bytes/iobuf.h"
#include "test_utils/random_bytes.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/testing/perf_tests.hh>

#include <crc32c/crc32c.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string_view>
#include <vector>

namespace {

// Bytes checksummed by a single test run. Only sets the granularity of the
// timing loop; the framework repeats runs until the measurement duration is up.
constexpr size_t bytes_per_run = 4_MiB;

/// perf_tests::do_not_optimize keeps a result alive, but it leaves the compiler
/// free to hoist the *pure* computation that produced it out of the loop. That
/// would rig this comparison: absl::ExtendCrc32c inlines for small buffers
/// while crc32c::Extend is always an opaque call, so only the abseil side could
/// be hoisted. Claiming memory may have changed forces both to be recomputed.
[[gnu::always_inline]] inline void clobber_memory() {
    asm volatile("" : : : "memory");
}

template<size_t Iters, typename Fn>
size_t measure(Fn checksum) {
    for (auto i = Iters; i--;) {
        clobber_memory();
        perf_tests::do_not_optimize(checksum());
    }
    return Iters;
}

std::string_view as_view(const bytes& b) {
    // NOLINTNEXTLINE(*-reinterpret-cast)
    return {reinterpret_cast<const char*>(b.data()), b.size()};
}

uint32_t crc32c_google(std::string_view buf) {
    // NOLINTNEXTLINE(*-reinterpret-cast)
    const auto* p = reinterpret_cast<const uint8_t*>(buf.data());
    return crc32c::Extend(0, p, buf.size());
}

uint32_t crc32c_abseil(std::string_view buf) {
    return static_cast<uint32_t>(absl::ComputeCrc32c(buf));
}

// ---------------------------------------------------------------------------
// contiguous buffer
// ---------------------------------------------------------------------------

template<size_t Size>
struct crc_bench {
    static constexpr size_t inner_iters = std::max<size_t>(
      1, bytes_per_run / Size);

    crc_bench()
      : _data(tests::random_bytes(Size)) {
        auto google = crc32c_google(data());
        auto abseil = crc32c_abseil(data());
        vassert(
          google == abseil,
          "crc32c disagreement at {} bytes: google={:#010x} abseil={:#010x}",
          Size,
          google,
          abseil);
    }

    std::string_view data() const { return as_view(_data); }

    [[gnu::noinline]] size_t run_crc32c_google() {
        return measure<inner_iters>([this] { return crc32c_google(data()); });
    }

    [[gnu::noinline]] size_t run_crc32c_abseil() {
        return measure<inner_iters>([this] { return crc32c_abseil(data()); });
    }

private:
    bytes _data;
};

// ---------------------------------------------------------------------------
// incremental integer extends
// ---------------------------------------------------------------------------

/// Field widths, and their order, that model::internal_header_only_crc feeds
/// into the checksum. Every replicated batch pays for this sequence, and unlike
/// the bulk cases it is dominated by per-call overhead: 57 bytes over 12 calls.
struct batch_header {
    int32_t size_bytes;
    int64_t base_offset;
    int8_t type;
    uint32_t crc;
    int16_t attrs;
    int32_t last_offset_delta;
    int64_t first_timestamp;
    int64_t max_timestamp;
    int64_t producer_id;
    int16_t producer_epoch;
    int32_t base_sequence;
    int32_t record_count;
};

template<typename Fn>
constexpr void for_each_field(const batch_header& h, Fn fn) {
    fn(h.size_bytes);
    fn(h.base_offset);
    fn(h.type);
    fn(h.crc);
    fn(h.attrs);
    fn(h.last_offset_delta);
    fn(h.first_timestamp);
    fn(h.max_timestamp);
    fn(h.producer_id);
    fn(h.producer_epoch);
    fn(h.base_sequence);
    fn(h.record_count);
}

// sizeof(batch_header) counts padding the checksum never sees, so sum the
// widths of the fields that are actually hashed.
constexpr size_t batch_header_bytes = [] {
    size_t n = 0;
    for_each_field(batch_header{}, [&n](auto field) { n += sizeof(field); });
    return n;
}();
static_assert(batch_header_bytes == 57);

uint32_t header_crc32c_google(const batch_header& h) {
    uint32_t crc = 0;
    for_each_field(h, [&crc](auto field) {
        auto le = ss::cpu_to_le(field);
        // NOLINTNEXTLINE(*-reinterpret-cast)
        const auto* p = reinterpret_cast<const uint8_t*>(&le);
        crc = crc32c::Extend(crc, p, sizeof(le));
    });
    return crc;
}

uint32_t header_crc32c_abseil(const batch_header& h) {
    absl::crc32c_t crc{0};
    for_each_field(h, [&crc](auto field) {
        auto le = ss::cpu_to_le(field);
        crc = absl::ExtendCrc32c(
          crc,
          // NOLINTNEXTLINE(*-reinterpret-cast)
          std::string_view{reinterpret_cast<const char*>(&le), sizeof(le)});
    });
    return static_cast<uint32_t>(crc);
}

struct crc_batch_header {
    static constexpr size_t inner_iters = std::max<size_t>(
      1, bytes_per_run / batch_header_bytes);

    crc_batch_header() {
        auto google = header_crc32c_google(_header);
        auto abseil = header_crc32c_abseil(_header);
        vassert(
          google == abseil,
          "crc32c disagreement over batch header: google={:#010x} "
          "abseil={:#010x}",
          google,
          abseil);
    }

    [[gnu::noinline]] size_t run_crc32c_google() {
        return measure<inner_iters>(
          [this] { return header_crc32c_google(_header); });
    }

    [[gnu::noinline]] size_t run_crc32c_abseil() {
        return measure<inner_iters>(
          [this] { return header_crc32c_abseil(_header); });
    }

private:
    batch_header _header{
      .size_bytes = 4096,
      .base_offset = 1'234'567,
      .type = 1,
      .crc = 0xdeadbeef,
      .attrs = 0,
      .last_offset_delta = 99,
      .first_timestamp = 1'700'000'000'000,
      .max_timestamp = 1'700'000'000'999,
      .producer_id = -1,
      .producer_epoch = -1,
      .base_sequence = -1,
      .record_count = 100,
    };
};

// ---------------------------------------------------------------------------
// fragmented iobuf
// ---------------------------------------------------------------------------

uint32_t iobuf_crc32c_google(const iobuf& buf) {
    uint32_t crc = 0;
    auto in = iobuf::iterator_consumer(buf.cbegin(), buf.cend());
    (void)in.consume(buf.size_bytes(), [&crc](const char* src, size_t sz) {
        // NOLINTNEXTLINE(*-reinterpret-cast)
        const auto* p = reinterpret_cast<const uint8_t*>(src);
        crc = crc32c::Extend(crc, p, sz);
        return ss::stop_iteration::no;
    });
    return crc;
}

uint32_t iobuf_crc32c_abseil(const iobuf& buf) {
    absl::crc32c_t crc{0};
    auto in = iobuf::iterator_consumer(buf.cbegin(), buf.cend());
    (void)in.consume(buf.size_bytes(), [&crc](const char* src, size_t sz) {
        crc = absl::ExtendCrc32c(crc, std::string_view{src, sz});
        return ss::stop_iteration::no;
    });
    return static_cast<uint32_t>(crc);
}

/// iobuf::append(temporary_buffer) linearizes small buffers into the tail
/// fragment, so append fragments explicitly to get an exact fragment size.
iobuf make_fragmented(std::string_view data, size_t frag_size) {
    iobuf buf;
    for (size_t off = 0; off < data.size(); off += frag_size) {
        auto sz = std::min(frag_size, data.size() - off);
        buf.append(
          std::make_unique<iobuf::fragment>(
            ss::temporary_buffer<char>(data.data() + off, sz)));
    }
    return buf;
}

template<size_t Size, size_t FragSize>
struct crc_iobuf_bench {
    static constexpr size_t inner_iters = std::max<size_t>(
      1, bytes_per_run / Size);

    crc_iobuf_bench()
      : _data(tests::random_bytes(Size))
      , _buf(make_fragmented(as_view(_data), FragSize)) {
        auto contiguous = crc32c_google(as_view(_data));
        auto google = iobuf_crc32c_google(_buf);
        auto abseil = iobuf_crc32c_abseil(_buf);
        vassert(
          google == contiguous && abseil == contiguous,
          "crc32c disagreement over {} bytes in {} byte fragments: "
          "google={:#010x} abseil={:#010x} contiguous={:#010x}",
          Size,
          FragSize,
          google,
          abseil,
          contiguous);
    }

    [[gnu::noinline]] size_t run_crc32c_google() {
        return measure<inner_iters>(
          [this] { return iobuf_crc32c_google(_buf); });
    }

    [[gnu::noinline]] size_t run_crc32c_abseil() {
        return measure<inner_iters>(
          [this] { return iobuf_crc32c_abseil(_buf); });
    }

private:
    bytes _data;
    iobuf _buf;
};

// ---------------------------------------------------------------------------
// checksum fused with a copy
// ---------------------------------------------------------------------------

// absl::MemcpyCrc32c is the one place abseil offers something structurally
// different: it interleaves the copy with the checksum instead of making two
// passes. Note that it switches to non-temporal stores for large buffers, which
// is a win here but bypasses the cache the copy's consumer may want.

uint32_t memcpy_crc32c_google(char* dst, std::string_view src) {
    std::memcpy(dst, src.data(), src.size());
    return crc32c_google({dst, src.size()});
}

uint32_t memcpy_crc32c_abseil(char* dst, std::string_view src) {
    return static_cast<uint32_t>(
      absl::MemcpyCrc32c(dst, src.data(), src.size()));
}

template<size_t Size>
struct crc_memcpy_bench {
    static constexpr size_t inner_iters = std::max<size_t>(
      1, bytes_per_run / Size);

    crc_memcpy_bench()
      : _src(tests::random_bytes(Size))
      , _dst(Size) {
        auto expected = crc32c_google(as_view(_src));
        auto google = memcpy_crc32c_google(_dst.data(), as_view(_src));
        auto abseil = memcpy_crc32c_abseil(_dst.data(), as_view(_src));
        vassert(
          google == expected && abseil == expected,
          "crc32c disagreement over a {} byte copy: google={:#010x} "
          "abseil={:#010x} expected={:#010x}",
          Size,
          google,
          abseil,
          expected);
    }

    [[gnu::noinline]] size_t run_google() {
        return measure<inner_iters>(
          [this] { return memcpy_crc32c_google(_dst.data(), as_view(_src)); });
    }

    [[gnu::noinline]] size_t run_abseil() {
        return measure<inner_iters>(
          [this] { return memcpy_crc32c_abseil(_dst.data(), as_view(_src)); });
    }

private:
    bytes _src;
    std::vector<char> _dst;
};

} // namespace

// NOLINTBEGIN(*-macro-*)

#define CRC_BENCH(size)                                                        \
    struct crc_##size##b : crc_bench<size> {};                                 \
    PERF_TEST_F(crc_##size##b, crc32c_google) { return run_crc32c_google(); }  \
    PERF_TEST_F(crc_##size##b, crc32c_abseil) { return run_crc32c_abseil(); }

// 57 is the batch header and 128 is just past the 64 byte buffer below which
// abseil takes an inlined fast path.
//
// The rest straddle google/crc32c's block tiers, which it enters only at
// 3*336, 3*1360 and 3*5440 bytes (crc32c_sse42.cc). Whatever does not fill a
// tier drops to a smaller one and finally to a serial 16-bytes-at-a-time loop,
// so its throughput sawtooths: 4032 and 16256 land just under a tier and are
// ~15% slower per byte than 4096 and 16384, which land just over one. Abseil
// splits the whole buffer into equal streams and has no such cliffs, so a
// single size can rank the two either way -- always measure both sides of a
// tier boundary.
CRC_BENCH(8)
CRC_BENCH(57)
CRC_BENCH(128)
CRC_BENCH(512)
CRC_BENCH(4032)
CRC_BENCH(4096)
CRC_BENCH(16256)
CRC_BENCH(16384)
CRC_BENCH(65536)

PERF_TEST_F(crc_batch_header, crc32c_google) { return run_crc32c_google(); }
PERF_TEST_F(crc_batch_header, crc32c_abseil) { return run_crc32c_abseil(); }

#define CRC_IOBUF_BENCH(size, frag_size)                                       \
    struct crc_iobuf_##size##b_##frag_size##b_frags                            \
      : crc_iobuf_bench<size, frag_size> {};                                   \
    PERF_TEST_F(crc_iobuf_##size##b_##frag_size##b_frags, crc32c_google) {     \
        return run_crc32c_google();                                            \
    }                                                                          \
    PERF_TEST_F(crc_iobuf_##size##b_##frag_size##b_frags, crc32c_abseil) {     \
        return run_crc32c_abseil();                                            \
    }

CRC_IOBUF_BENCH(65536, 512)
CRC_IOBUF_BENCH(65536, 16384)

#define CRC_MEMCPY_BENCH(size)                                                 \
    struct crc_memcpy_##size##b : crc_memcpy_bench<size> {};                   \
    PERF_TEST_F(crc_memcpy_##size##b, google_two_pass) {                       \
        return run_google();                                                   \
    }                                                                          \
    PERF_TEST_F(crc_memcpy_##size##b, abseil_fused) { return run_abseil(); }

// Paired around a tier boundary for the same reason: the two-pass side calls
// crc32c::Extend and inherits its sawtooth.
CRC_MEMCPY_BENCH(512)
CRC_MEMCPY_BENCH(4032)
CRC_MEMCPY_BENCH(4096)
CRC_MEMCPY_BENCH(65536)

// NOLINTEND(*-macro-*)
