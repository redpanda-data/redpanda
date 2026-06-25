/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iobuf.h"
#include "bytes/iostream.h"
#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/chunk_data_source.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/iostream.hh>

#include <gtest/gtest.h>

#include <expected>
#include <string>
#include <vector>

using namespace cloud_topics::l1;

namespace {

// A deterministic byte sequence so reads can be checked positionally.
std::string make_bytes(size_t n) {
    std::string s(n, '\0');
    for (size_t i = 0; i < n; ++i) {
        s[i] = static_cast<char>(i % 251);
    }
    return s;
}

iobuf to_iobuf(const std::string& s) {
    iobuf b;
    b.append(s.data(), s.size());
    return b;
}

// A fetch over an in-memory buffer that records every requested (pos, len) so
// tests can assert which chunks were fetched and in what granularity. The fetch
// is returned as a plain lambda -- chunk_data_source is generic over the fetch
// type (io::errc here is just this test's error type).
struct recording_fetcher {
    iobuf data;
    std::vector<std::pair<size_t, size_t>> calls;

    explicit recording_fetcher(const std::string& s)
      : data(to_iobuf(s)) {}

    auto fn() {
        return
          [this](this auto, size_t pos, size_t len, ss::abort_source*)
            -> ss::future<std::expected<ss::input_stream<char>, io::errc>> {
              calls.emplace_back(pos, len);
              co_return make_iobuf_input_stream(data.share(pos, len));
          };
    }

    static auto failing_fn() {
        return
          [](size_t, size_t, ss::abort_source*)
            -> ss::future<std::expected<ss::input_stream<char>, io::errc>> {
              co_return std::unexpected(io::errc::cloud_op_error);
          };
    }
};

template<typename Fetch>
ss::input_stream<char> make_stream(
  Fetch fetch,
  size_t start,
  size_t total,
  size_t chunk_size,
  ss::abort_source& as,
  use_chunk_aligned_reads aligned) {
    return ss::input_stream<char>{make_chunk_data_source(
      std::move(fetch), start, total, chunk_size, &as, aligned)};
}

std::string read_all(ss::input_stream<char>& in) {
    std::string out;
    while (true) {
        auto buf = in.read().get();
        if (buf.empty()) {
            break;
        }
        out.append(buf.get(), buf.size());
    }
    return out;
}

} // namespace

// A full read across many chunks yields exactly the requested byte range, i.e.
// chunking is transparent: the stream is byte-identical to a single download.
TEST(ChunkDataSourceTest, FullReadConcatenatesChunks) {
    auto bytes = make_bytes(1000);
    recording_fetcher f(bytes);
    ss::abort_source as;
    auto in = make_stream(
      f.fn(), 0, 1000, 256, as, use_chunk_aligned_reads::yes);
    auto out = read_all(in);
    in.close().get();

    EXPECT_EQ(out, bytes);
    // ceil(1000/256) = 4 chunks: (0,256)(256,256)(512,256)(768,232).
    ASSERT_EQ(f.calls.size(), 4u);
    EXPECT_EQ(f.calls[0], (std::pair<size_t, size_t>{0, 256}));
    EXPECT_EQ(f.calls[1], (std::pair<size_t, size_t>{256, 256}));
    EXPECT_EQ(f.calls[2], (std::pair<size_t, size_t>{512, 256}));
    EXPECT_EQ(f.calls[3], (std::pair<size_t, size_t>{768, 232}));
}

// A seek into the middle of a chunk: the first fetch is chunk-aligned and the
// leading bytes before the seek point are skipped.
TEST(ChunkDataSourceTest, MidChunkSeekSkipsHead) {
    auto bytes = make_bytes(1000);
    recording_fetcher f(bytes);
    ss::abort_source as;
    // start at 300 (mid second chunk), read to end.
    auto in = make_stream(
      f.fn(), 300, 700, 256, as, use_chunk_aligned_reads::yes);
    auto out = read_all(in);
    in.close().get();

    EXPECT_EQ(out, bytes.substr(300));
    // First fetch is the chunk containing 300: [256, 512).
    ASSERT_FALSE(f.calls.empty());
    EXPECT_EQ(f.calls[0], (std::pair<size_t, size_t>{256, 256}));
}

// use_chunk_aligned_reads=false: a mid-chunk start reads exactly from start_pos
// -- no chunk alignment and no skipped head, so no bytes before the start are
// fetched (contrast MidChunkSeekSkipsHead, which fetches the [256, 512) chunk
// and discards the leading 44 bytes). Chunks then advance by chunk_size
// from the start.
TEST(ChunkDataSourceTest, UnalignedStartReadsFromStartPos) {
    auto bytes = make_bytes(1000);
    recording_fetcher f(bytes);
    ss::abort_source as;
    auto in = make_stream(
      f.fn(), 300, 700, 256, as, use_chunk_aligned_reads::no);
    auto out = read_all(in);
    in.close().get();

    EXPECT_EQ(out, bytes.substr(300));
    // First fetch starts at 300 itself; subsequent chunks advance by 256.
    ASSERT_EQ(f.calls.size(), 3u);
    EXPECT_EQ(f.calls[0], (std::pair<size_t, size_t>{300, 256}));
    EXPECT_EQ(f.calls[1], (std::pair<size_t, size_t>{556, 256}));
    EXPECT_EQ(f.calls[2], (std::pair<size_t, size_t>{812, 188}));
}

// When start_pos already sits on a chunk boundary the flag makes no difference:
// use_chunk_aligned_reads::no fetches the same chunks as ::yes.
TEST(ChunkDataSourceTest, UnalignedStartOnBoundaryMatchesAligned) {
    auto bytes = make_bytes(1000);
    recording_fetcher f(bytes);
    ss::abort_source as;
    auto in = make_stream(
      f.fn(), 0, 1000, 256, as, use_chunk_aligned_reads::no);
    auto out = read_all(in);
    in.close().get();

    EXPECT_EQ(out, bytes);
    ASSERT_EQ(f.calls.size(), 4u);
    EXPECT_EQ(f.calls[0], (std::pair<size_t, size_t>{0, 256}));
    EXPECT_EQ(f.calls[1], (std::pair<size_t, size_t>{256, 256}));
    EXPECT_EQ(f.calls[2], (std::pair<size_t, size_t>{512, 256}));
    EXPECT_EQ(f.calls[3], (std::pair<size_t, size_t>{768, 232}));
}

// A read that stops early only fetches the chunks it touches; the tail is never
// downloaded. This is the win that a whole-suffix download cannot give, and the
// reason a naive length cap (which would EOF early) is unnecessary.
TEST(ChunkDataSourceTest, EarlyTerminationFetchesOnlyTouchedChunks) {
    auto bytes = make_bytes(10000);
    recording_fetcher f(bytes);
    ss::abort_source as;
    auto in = make_stream(
      f.fn(), 0, 10000, 256, as, use_chunk_aligned_reads::yes);

    auto head = in.read_exactly(50).get();
    in.close().get();

    EXPECT_EQ(std::string(head.get(), head.size()), bytes.substr(0, 50));
    // Only the first chunk was needed; a whole-suffix download would have
    // pulled all ceil(10000/256)=40 chunks.
    EXPECT_EQ(f.calls.size(), 1u);
}

// A segment smaller than one chunk is a single fetch.
TEST(ChunkDataSourceTest, SingleChunkSegment) {
    auto bytes = make_bytes(100);
    recording_fetcher f(bytes);
    ss::abort_source as;
    auto in = make_stream(
      f.fn(), 0, 100, 256, as, use_chunk_aligned_reads::yes);
    auto out = read_all(in);
    in.close().get();

    EXPECT_EQ(out, bytes);
    ASSERT_EQ(f.calls.size(), 1u);
    EXPECT_EQ(f.calls[0], (std::pair<size_t, size_t>{0, 100}));
}

// When the length is an exact multiple of the chunk size, EOF lands exactly at
// the end -- it is not mistaken for a chunk boundary.
TEST(ChunkDataSourceTest, ExactMultipleEndsAtTotal) {
    auto bytes = make_bytes(512);
    recording_fetcher f(bytes);
    ss::abort_source as;
    auto in = make_stream(
      f.fn(), 0, 512, 256, as, use_chunk_aligned_reads::yes);
    auto out = read_all(in);
    in.close().get();

    EXPECT_EQ(out.size(), 512u);
    EXPECT_EQ(out, bytes);
    ASSERT_EQ(f.calls.size(), 2u);
    EXPECT_EQ(f.calls[1], (std::pair<size_t, size_t>{256, 256}));
}

// A fetch failure surfaces as an exception from the stream read.
TEST(ChunkDataSourceTest, FetchFailurePropagates) {
    ss::abort_source as;
    auto in = make_stream(
      recording_fetcher::failing_fn(),
      0,
      1000,
      256,
      as,
      use_chunk_aligned_reads::yes);
    EXPECT_THROW(in.read().get(), std::runtime_error);
    in.close().get();
}

// A mid-stream abort surfaces as an exception rather than a clean EOF: the
// source checks the abort source on every get(), so a cancelled read fails
// loudly instead of looking like a (truncated) end-of-stream -- otherwise a
// cancelled caller could commit a partial read as if it had read everything.
TEST(ChunkDataSourceTest, AbortSurfacesAsError) {
    auto bytes = make_bytes(1000);
    recording_fetcher f(bytes);
    ss::abort_source as;
    auto in = make_stream(
      f.fn(), 0, 1000, 256, as, use_chunk_aligned_reads::yes);

    // First read succeeds, then abort with bytes still unread.
    auto first = in.read().get();
    EXPECT_FALSE(first.empty());
    as.request_abort();

    EXPECT_THROW(
      {
          while (!in.read().get().empty()) {
          }
      },
      ss::abort_requested_exception);
    in.close().get();
}

// An abort before the first read fails the very first get(), before any chunk
// is fetched -- the abort check sits at the top of get(), ahead of the fetch.
TEST(ChunkDataSourceTest, AbortBeforeFirstReadFetchesNothing) {
    auto bytes = make_bytes(1000);
    recording_fetcher f(bytes);
    ss::abort_source as;
    auto in = make_stream(
      f.fn(), 0, 1000, 256, as, use_chunk_aligned_reads::yes);

    as.request_abort();
    EXPECT_THROW(in.read().get(), ss::abort_requested_exception);
    in.close().get();

    EXPECT_TRUE(f.calls.empty());
}
