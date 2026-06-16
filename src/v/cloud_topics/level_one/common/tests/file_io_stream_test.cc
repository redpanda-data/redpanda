/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/units.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "cloud_io/basic_cache_service_api.h"
#include "cloud_io/cache_service.h"
#include "cloud_io/remote.h"
#include "cloud_io/tests/s3_imposter.h"
#include "cloud_io/tests/scoped_remote.h"
#include "cloud_topics/level_one/common/file_io.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/common/object_utils.h"
#include "config/property.h"
#include "storage/disk.h"
#include "test_utils/scoped_config.h"
#include "test_utils/test.h"
#include "test_utils/tmp_dir.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/sharded.hh>

#include <gtest/gtest.h>

using namespace cloud_topics;
using namespace std::chrono_literals;

namespace {

ss::abort_source never_abort;

ss::future<ss::sstring> drain(ss::input_stream<char> stream) {
    iobuf buf;
    while (true) {
        auto chunk = co_await stream.read();
        if (chunk.empty()) {
            break;
        }
        buf.append(std::move(chunk));
    }
    co_await stream.close();
    iobuf_parser p(std::move(buf));
    co_return p.read_string(p.bytes_left());
}

} // namespace

// Exercises file_io's skip_cache (streaming) read path against a real remote
// and cache backed by the S3 imposter. The streaming path bridges
// download_stream (push) into a pull input_stream without ever populating the
// local cache.
class l1_file_io_stream_test
  : public s3_imposter_fixture
  , public seastar_test {
public:
    void SetUp() override {
        set_expectations_and_listen({});
        _scoped = cloud_io::scoped_remote::create(1, get_configuration());

        _cache_dir = _tmp.get_path() / "cache";
        cloud_io::cache::initialize(_cache_dir).get();
        _cache
          .start(
            _cache_dir,
            30_GiB,
            config::mock_binding<double>(0.0),
            config::mock_binding<uint64_t>(1_MiB + 500_KiB),
            config::mock_binding<std::optional<double>>(std::nullopt),
            config::mock_binding<uint32_t>(100000),
            config::mock_binding<uint16_t>(3))
          .get();
        _cache.invoke_on_all([](cloud_io::cache& c) { return c.start(); })
          .get();
        _cache
          .invoke_on(
            ss::shard_id{0},
            [](cloud_io::cache& c) {
                c.notify_disk_status(
                  100_GiB, 50_GiB, storage::disk_space_alert::ok);
            })
          .get();

        _io = std::make_unique<l1::file_io>(
          _tmp.get_path() / "staging",
          &_scoped->remote.local(),
          bucket_name,
          &_cache.local());
    }

    void TearDown() override {
        _io.reset();
        _cache.stop().get();
        _scoped->request_stop();
        _scoped.reset();
    }

    void upload(l1::object_id oid, std::string_view body) {
        retry_chain_node rtc(never_abort, 5s, 100ms);
        auto res
          = _scoped->remote.local()
              .upload_object(
                {.transfer_details
                 = {.bucket = bucket_name, .key = l1::object_path_factory::level_one_path(oid), .parent_rtc = rtc},
                 .display_str = "test-upload",
                 .payload = iobuf::from(body)})
              .get();
        ASSERT_EQ(res, cloud_io::upload_result::success);
    }

    // The cache key file_io derives for a given extent.
    std::filesystem::path cache_key(const l1::object_extent& e) {
        return fmt::format(
          "l1_{}_position_{}_size_{}.partial", e.id, e.position, e.size);
    }

    cloud_io::cache_element_status is_cached(const l1::object_extent& e) {
        return _cache.local().is_cached(cache_key(e)).get();
    }

    temporary_dir _tmp{"l1_file_io_stream_test"};
    std::filesystem::path _cache_dir;
    std::unique_ptr<cloud_io::scoped_remote> _scoped;
    ss::sharded<cloud_io::cache> _cache;
    std::unique_ptr<l1::file_io> _io;
};

TEST_F(l1_file_io_stream_test, streaming_read_returns_full_object) {
    auto oid = l1::create_object_id();
    const ss::sstring body = "hello level one streaming read";
    upload(oid, body);

    l1::object_extent extent{.id = oid, .position = 0, .size = body.size()};
    auto stream_res = _io
                        ->read_object(
                          extent,
                          &never_abort,
                          cloud_io::group_id::default_group,
                          /*skip_cache=*/true)
                        .get();
    ASSERT_TRUE(stream_res.has_value());
    auto contents = drain(std::move(stream_res).value()).get();

    EXPECT_EQ(contents, body);
    // The streaming path must not populate the cache.
    EXPECT_EQ(is_cached(extent), cloud_io::cache_element_status::not_available);
}

TEST_F(l1_file_io_stream_test, cached_read_populates_cache) {
    auto oid = l1::create_object_id();
    const ss::sstring body = "this read goes through the cache";
    upload(oid, body);

    l1::object_extent extent{.id = oid, .position = 0, .size = body.size()};
    auto stream_res = _io
                        ->read_object(
                          extent,
                          &never_abort,
                          cloud_io::group_id::default_group,
                          /*skip_cache=*/false)
                        .get();
    ASSERT_TRUE(stream_res.has_value());
    auto contents = drain(std::move(stream_res).value()).get();

    EXPECT_EQ(contents, body);
    // The cached path, in contrast, leaves the extent in the cache.
    EXPECT_EQ(is_cached(extent), cloud_io::cache_element_status::available);
}

TEST_F(l1_file_io_stream_test, skip_cache_read_served_from_cache_when_present) {
    auto oid = l1::create_object_id();
    const ss::sstring body = "skip_cache still reads an already-cached object";
    upload(oid, body);

    l1::object_extent extent{.id = oid, .position = 0, .size = body.size()};

    // Populate the cache via a normal (non-skip) read.
    {
        auto res = _io
                     ->read_object(
                       extent,
                       &never_abort,
                       cloud_io::group_id::default_group,
                       /*skip_cache=*/false)
                     .get();
        ASSERT_TRUE(res.has_value());
        EXPECT_EQ(drain(std::move(res).value()).get(), body);
    }
    ASSERT_EQ(is_cached(extent), cloud_io::cache_element_status::available);

    auto count_gets = [this] {
        return get_requests([](const http_test_utils::request_info& r) {
                   return r.method == "GET";
               })
          .size();
    };
    const auto gets_before = count_gets();

    // skip_cache only suppresses cache *insertion*, not lookup: an extent
    // already on disk is served from the cache, so this read issues no
    // additional GET to object storage and leaves the cache populated.
    auto res = _io
                 ->read_object(
                   extent,
                   &never_abort,
                   cloud_io::group_id::default_group,
                   /*skip_cache=*/true)
                 .get();
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(drain(std::move(res).value()).get(), body);

    EXPECT_EQ(count_gets(), gets_before);
    EXPECT_EQ(is_cached(extent), cloud_io::cache_element_status::available);
}

TEST_F(l1_file_io_stream_test, streaming_read_partial_extent) {
    auto oid = l1::create_object_id();
    const ss::sstring body = "0123456789abcdef";
    upload(oid, body);

    // Read a sub-range of the object; the bridge passes the byte range through
    // to download_stream.
    l1::object_extent extent{.id = oid, .position = 4, .size = 6};
    auto stream_res = _io
                        ->read_object(
                          extent,
                          &never_abort,
                          cloud_io::group_id::default_group,
                          /*skip_cache=*/true)
                        .get();
    ASSERT_TRUE(stream_res.has_value());
    auto contents = drain(std::move(stream_res).value()).get();

    EXPECT_EQ(contents, "456789");
    EXPECT_EQ(is_cached(extent), cloud_io::cache_element_status::not_available);
}

TEST_F(l1_file_io_stream_test, streaming_read_early_close_does_not_hang) {
    auto oid = l1::create_object_id();
    // Spans several get() reads so a buffered chunk remains after the first
    // read; close() must drop it and return without hanging.
    const ss::sstring body(4_MiB, 'x');
    upload(oid, body);

    l1::object_extent extent{.id = oid, .position = 0, .size = body.size()};
    auto stream_res = _io
                        ->read_object(
                          extent,
                          &never_abort,
                          cloud_io::group_id::default_group,
                          /*skip_cache=*/true)
                        .get();
    ASSERT_TRUE(stream_res.has_value());
    auto stream = std::move(stream_res).value();

    // Read once, then close with bytes still buffered. close() must drop the
    // buffered remainder and return rather than hang.
    auto chunk = stream.read().get();
    EXPECT_FALSE(chunk.empty());
    stream.close().get();
}

TEST_F(l1_file_io_stream_test, streaming_read_issues_single_ranged_get) {
    // read_object is a single-range primitive: a skip_cache read serves the
    // whole requested extent as ONE ranged GET, reassembled in order.
    // Chunking (splitting a read into several bounded GETs) is applied a layer
    // up in open_object, which calls read_object once per chunk -- so it is
    // covered end-to-end there, not here.
    const size_t total = 2_MiB + 512_KiB;

    auto oid = l1::create_object_id();
    // Position-dependent (ASCII, so the UTF8 drain() accepts it) content so a
    // misordered or truncated read is caught by the comparison below.
    ss::sstring body(total, '\0');
    for (size_t i = 0; i < total; ++i) {
        body[i] = static_cast<char>('a' + (i % 26));
    }
    upload(oid, body);

    l1::object_extent extent{.id = oid, .position = 0, .size = total};
    auto stream_res = _io
                        ->read_object(
                          extent,
                          &never_abort,
                          cloud_io::group_id::default_group,
                          /*skip_cache=*/true)
                        .get();
    ASSERT_TRUE(stream_res.has_value());
    auto contents = drain(std::move(stream_res).value()).get();

    EXPECT_EQ(contents.size(), body.size());
    EXPECT_EQ(contents, body);
    EXPECT_EQ(is_cached(extent), cloud_io::cache_element_status::not_available);

    // Exactly one ranged GET for the whole extent.
    auto gets = get_requests(
      [](const http_test_utils::request_info& r) { return r.method == "GET"; });
    EXPECT_EQ(gets.size(), 1);
}
