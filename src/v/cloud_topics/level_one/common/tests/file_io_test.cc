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
#include "cloud_io/tests/cache_test_fixture.h"
#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/file_io.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "model/fundamental.h"
#include "test_utils/test.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>

using namespace cloud_topics;
using namespace std::chrono_literals;

namespace cloud_topics::l1 {

// White-box fixture combining cloud_io::cache_test_fixture (real cache,
// no remote) with seastar_test (gtest + coroutine support). file_io is
// constructed with a null remote so tests can exercise the byte-range
// read-merge path without touching object storage. The cold-miss
// download path is bench-validated, not unit-tested here.
class file_io_test_fixture
  : public cloud_io::cache_test_fixture
  , public seastar_test {
public:
    file_io_test_fixture() {
        _staging_dir = test_dir.get_path() / "staging";
        std::filesystem::create_directories(_staging_dir);
        _bucket = cloud_storage_clients::bucket_name("test-bucket");
        _file_io = std::make_unique<file_io>(
          _staging_dir, /*remote*/ nullptr, _bucket, &sharded_cache.local());
    }

    ss::future<> TearDownAsync() override {
        if (_file_io) {
            co_await _file_io->stop();
        }
    }

    auto& inflight_downloads() { return _file_io->_inflight_downloads; }
    file_io& io() { return *_file_io; }

    void put_byte_range(
      const object_id& id, size_t pos, size_t size, char fill_char) {
        auto key = byte_range_key(id, pos, size);
        put_into_cache(create_data_string(fill_char, size), key);
    }

    static std::filesystem::path
    byte_range_key(const object_id& id, size_t pos, size_t size) {
        return file_io::cache_key(
          object_extent{.id = id, .position = pos, .size = size});
    }

private:
    std::filesystem::path _staging_dir;
    cloud_storage_clients::bucket_name _bucket;
    std::unique_ptr<file_io> _file_io;
};

namespace {

// Read the first byte from the stream and close it. Helps assert which
// of two same-sized payloads we got.
ss::future<char>
read_first_byte_and_close(ss::input_stream<char> stream, size_t total_size) {
    auto buf = co_await read_iobuf_exactly(stream, total_size);
    co_await stream.close();
    iobuf::iterator_consumer cons{buf.cbegin(), buf.cend()};
    char first = 0;
    cons.consume_to(1, &first);
    co_return first;
}

} // namespace

// A read that finds an in-flight download for its (oid, pos, size)
// joins it, awaits the shared promise, and on resolve returns the
// cached bytes.
TEST_F_CORO(file_io_test_fixture, MergedReadHitsCacheAfterSuccess) {
    auto id = create_object_id();
    constexpr size_t pos = 100;
    constexpr size_t size = 256;
    auto br_key = byte_range_key(id, pos, size);

    // Synthesize an in-flight download by inserting into the map
    // before the merged read starts; we'll resolve the promise by
    // hand below.
    auto [it, inserted] = inflight_downloads().emplace(
      br_key, ss::shared_promise<std::optional<io::errc>>{});
    ASSERT_TRUE_CORO(inserted);

    object_extent extent{.id = id, .position = pos, .size = size};
    ss::abort_source as;
    auto fut = io().read_object(
      extent, &as, cloud_io::group_id::consumer_fetch);

    // Stand in for the in-flight download: warm the cache, then
    // resolve.
    put_byte_range(id, pos, size, 'X');
    co_await ss::sleep(5ms);
    it->second.set_value(std::nullopt);
    inflight_downloads().erase(it);

    auto result = co_await std::move(fut);
    ASSERT_TRUE_CORO(result.has_value());
    auto first = co_await read_first_byte_and_close(
      std::move(result.value()), extent.size);
    ASSERT_EQ_CORO(first, 'X');
}

// A merged read whose in-flight download fails must propagate that
// errc rather than racing on a fresh attempt.
TEST_F_CORO(file_io_test_fixture, MergedReadPropagatesError) {
    auto id = create_object_id();
    constexpr size_t pos = 100;
    constexpr size_t size = 256;
    auto br_key = byte_range_key(id, pos, size);

    auto [it, inserted] = inflight_downloads().emplace(
      br_key, ss::shared_promise<std::optional<io::errc>>{});
    ASSERT_TRUE_CORO(inserted);

    object_extent extent{.id = id, .position = pos, .size = size};
    ss::abort_source as;
    auto fut = io().read_object(
      extent, &as, cloud_io::group_id::consumer_fetch);

    // Download failed; merged reads propagate the same errc.
    co_await ss::sleep(5ms);
    it->second.set_value(io::errc::cloud_op_error);
    inflight_downloads().erase(it);

    auto result = co_await std::move(fut);
    ASSERT_FALSE_CORO(result.has_value());
    ASSERT_EQ_CORO(result.error(), io::errc::cloud_op_error);
}

// Aborting a merged read while it's suspended on the shared promise
// must surface as cloud_op_timeout (the abort_source-aware contract
// on the shared_future), not hang.
TEST_F_CORO(file_io_test_fixture, MergedReadAborted) {
    auto id = create_object_id();
    constexpr size_t pos = 100;
    constexpr size_t size = 256;
    auto br_key = byte_range_key(id, pos, size);

    auto [it, inserted] = inflight_downloads().emplace(
      br_key, ss::shared_promise<std::optional<io::errc>>{});
    ASSERT_TRUE_CORO(inserted);

    object_extent extent{.id = id, .position = pos, .size = size};
    ss::abort_source as;
    auto fut = io().read_object(
      extent, &as, cloud_io::group_id::consumer_fetch);

    co_await ss::sleep(5ms);
    as.request_abort();

    auto result = co_await std::move(fut);
    ASSERT_FALSE_CORO(result.has_value());
    ASSERT_EQ_CORO(result.error(), io::errc::cloud_op_timeout);

    // Clean up so the synthesized in-flight entry doesn't outlive the
    // fixture.
    it->second.set_value(std::nullopt);
    inflight_downloads().erase(it);
}

} // namespace cloud_topics::l1
