/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "cloud_storage/offset_translation_layer.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_segment.h"
#include "cloud_storage/tests/common_def.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "cloud_storage/types.h"
#include "model/metadata.h"
#include "s3/client.h"
#include "s3/client_probe.h"
#include "seastarx.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/future.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <chrono>
#include <exception>

using namespace std::chrono_literals;
using namespace cloud_storage;

inline ss::logger test_log("test"); // NOLINT

static constexpr std::string_view manifest_payload = R"json({
    "version": 1,
    "namespace": "test-ns",
    "topic": "test-topic",
    "partition": 42,
    "revision": 0,
    "last_offset": 1004,
    "segments": {
        "1-2-v1.log": {
            "is_compacted": false,
            "size_bytes": 100,
            "committed_offset": 2,
            "base_offset": 1
        }
    }
})json";

static partition_manifest load_manifest_from_str(std::string_view v) {
    partition_manifest m;
    iobuf i;
    i.append(v.data(), v.size());
    auto s = make_iobuf_input_stream(std::move(i));
    m.update(std::move(s)).get();
    return m;
}

FIXTURE_TEST(test_download_manifest, s3_imposter_fixture) { // NOLINT
    set_expectations_and_listen({expectation{
      .url = "/" + manifest_url, .body = ss::sstring(manifest_payload)}});
    auto conf = get_configuration();
    remote remote(s3_connection_limit(10), conf);
    partition_manifest actual(manifest_ntp, manifest_revision);
    auto action = ss::defer([&remote] { remote.stop().get(); });
    retry_chain_node fib(100ms, 20ms);
    auto res = remote
                 .download_manifest(
                   s3::bucket_name("bucket"),
                   remote_manifest_path(std::filesystem::path(manifest_url)),
                   actual,
                   fib)
                 .get();
    BOOST_REQUIRE(res == download_result::success);
    auto expected = load_manifest_from_str(manifest_payload);
    BOOST_REQUIRE(expected == actual); // NOLINT
}

FIXTURE_TEST(test_download_manifest_timeout, s3_imposter_fixture) { // NOLINT
    auto conf = get_configuration();
    remote remote(s3_connection_limit(10), conf);
    partition_manifest actual(manifest_ntp, manifest_revision);
    auto action = ss::defer([&remote] { remote.stop().get(); });
    retry_chain_node fib(100ms, 20ms);
    auto res = remote
                 .download_manifest(
                   s3::bucket_name("bucket"),
                   remote_manifest_path(std::filesystem::path(manifest_url)),
                   actual,
                   fib)
                 .get();
    BOOST_REQUIRE(res == download_result::timedout);
}

FIXTURE_TEST(test_upload_segment, s3_imposter_fixture) { // NOLINT
    set_expectations_and_listen({});
    auto conf = get_configuration();
    remote remote(s3_connection_limit(10), conf);
    auto name = segment_name("1-2-v1.log");
    auto path = generate_remote_segment_path(
      manifest_ntp, manifest_revision, name, model::term_id{123});
    uint64_t clen = manifest_payload.size();
    auto action = ss::defer([&remote] { remote.stop().get(); });
    auto reset_stream = []() -> ss::future<storage::segment_reader_handle> {
        iobuf out;
        out.append(manifest_payload.data(), manifest_payload.size());
        co_return storage::segment_reader_handle(
          make_iobuf_input_stream(std::move(out)));
    };
    retry_chain_node fib(100ms, 20ms);
    auto res = remote
                 .upload_segment(
                   s3::bucket_name("bucket"), path, clen, reset_stream, fib)
                 .get();
    BOOST_REQUIRE(res == upload_result::success);
    const auto& req = get_requests().front();
    BOOST_REQUIRE_EQUAL(req.content_length, clen);
    BOOST_REQUIRE_EQUAL(req.content, ss::sstring(manifest_payload));
}

FIXTURE_TEST(test_upload_segment_timeout, s3_imposter_fixture) { // NOLINT
    auto conf = get_configuration();
    remote remote(s3_connection_limit(10), conf);
    auto name = segment_name("1-2-v1.log");
    auto path = generate_remote_segment_path(
      manifest_ntp, manifest_revision, name, model::term_id{123});
    uint64_t clen = manifest_payload.size();
    auto action = ss::defer([&remote] { remote.stop().get(); });
    auto reset_stream = []() -> ss::future<storage::segment_reader_handle> {
        iobuf out;
        out.append(manifest_payload.data(), manifest_payload.size());
        co_return storage::segment_reader_handle(
          make_iobuf_input_stream(std::move(out)));
    };
    retry_chain_node fib(100ms, 20ms);
    auto res = remote
                 .upload_segment(
                   s3::bucket_name("bucket"), path, clen, reset_stream, fib)
                 .get();
    BOOST_REQUIRE(res == upload_result::timedout);
}

FIXTURE_TEST(test_download_segment, s3_imposter_fixture) { // NOLINT
    set_expectations_and_listen({});
    auto conf = get_configuration();
    auto bucket = s3::bucket_name("bucket");
    remote remote(s3_connection_limit(10), conf);
    auto name = segment_name("1-2-v1.log");
    auto path = generate_remote_segment_path(
      manifest_ntp, manifest_revision, name, model::term_id{123});
    uint64_t clen = manifest_payload.size();
    auto action = ss::defer([&remote] { remote.stop().get(); });
    auto reset_stream = []() -> ss::future<storage::segment_reader_handle> {
        iobuf out;
        out.append(manifest_payload.data(), manifest_payload.size());
        co_return storage::segment_reader_handle(
          make_iobuf_input_stream(std::move(out)));
    };
    retry_chain_node fib(100ms, 20ms);
    auto upl_res
      = remote.upload_segment(bucket, path, clen, reset_stream, fib).get();
    BOOST_REQUIRE(upl_res == upload_result::success);

    iobuf downloaded;
    auto try_consume = [&downloaded](
                         uint64_t len,
                         ss::input_stream<char> is) -> ss::future<uint64_t> {
        downloaded.clear();
        auto rds = make_iobuf_ref_output_stream(downloaded);
        co_await ss::copy(is, rds);
        co_return downloaded.size_bytes();
    };
    auto dnl_res
      = remote.download_segment(bucket, path, try_consume, fib).get();

    BOOST_REQUIRE(dnl_res == download_result::success);
    iobuf_parser p(std::move(downloaded));
    auto actual = p.read_string(p.bytes_left());
    BOOST_REQUIRE(actual == manifest_payload);
}

FIXTURE_TEST(test_download_segment_timeout, s3_imposter_fixture) { // NOLINT
    auto conf = get_configuration();
    auto bucket = s3::bucket_name("bucket");
    remote remote(s3_connection_limit(10), conf);
    auto name = segment_name("1-2-v1.log");
    auto path = generate_remote_segment_path(
      manifest_ntp, manifest_revision, name, model::term_id{123});

    iobuf downloaded;
    auto try_consume = [&downloaded](uint64_t, ss::input_stream<char>) {
        return ss::make_ready_future<uint64_t>(0);
    };

    retry_chain_node fib(100ms, 20ms);
    auto dnl_res
      = remote.download_segment(bucket, path, try_consume, fib).get();
    BOOST_REQUIRE(dnl_res == download_result::timedout);
}

FIXTURE_TEST(test_segment_exists, s3_imposter_fixture) { // NOLINT
    set_expectations_and_listen({});
    auto conf = get_configuration();
    auto bucket = s3::bucket_name("bucket");
    remote remote(s3_connection_limit(10), conf);
    auto name = segment_name("1-2-v1.log");
    auto path = generate_remote_segment_path(
      manifest_ntp, manifest_revision, name, model::term_id{123});
    uint64_t clen = manifest_payload.size();
    auto action = ss::defer([&remote] { remote.stop().get(); });
    auto reset_stream = []() -> ss::future<storage::segment_reader_handle> {
        iobuf out;
        out.append(manifest_payload.data(), manifest_payload.size());
        co_return storage::segment_reader_handle(
          make_iobuf_input_stream(std::move(out)));
    };

    retry_chain_node fib(100ms, 20ms);

    auto expected_notfound = remote.segment_exists(bucket, path, fib).get();
    BOOST_REQUIRE(expected_notfound == download_result::notfound);

    auto upl_res
      = remote.upload_segment(bucket, path, clen, reset_stream, fib).get();
    BOOST_REQUIRE(upl_res == upload_result::success);

    auto expected_success = remote.segment_exists(bucket, path, fib).get();

    BOOST_REQUIRE(expected_success == download_result::success);
}

FIXTURE_TEST(test_segment_exists_timeout, s3_imposter_fixture) { // NOLINT
    auto conf = get_configuration();
    auto bucket = s3::bucket_name("bucket");
    remote remote(s3_connection_limit(10), conf);
    auto name = segment_name("1-2-v1.log");
    auto path = generate_remote_segment_path(
      manifest_ntp, manifest_revision, name, model::term_id{123});

    retry_chain_node fib(100ms, 20ms);
    auto expect_timeout = remote.segment_exists(bucket, path, fib).get();
    BOOST_REQUIRE(expect_timeout == download_result::timedout);
}
