/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "cloud_storage/types.h"
#include "model/metadata.h"
#include "s3/client.h"
#include "seastarx.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/future.hh>
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
static const auto manifest_namespace = model::ns("test-ns");    // NOLINT
static const auto manifest_topic = model::topic("test-topic");  // NOLINT
static const auto manifest_partition = model::partition_id(42); // NOLINT
static const auto manifest_ntp = model::ntp(                    // NOLINT
  manifest_namespace,
  manifest_topic,
  manifest_partition);
static const auto manifest_revision = model::revision_id(0); // NOLINT
static const ss::sstring manifest_url = ssx::sformat(        // NOLINT
  "20000000/meta/{}_{}/manifest.json",
  manifest_ntp.path(),
  manifest_revision());
// NOLINTNEXTLINE
static const ss::sstring segment_url
  = "ce4fd1a3/test-ns/test-topic/42_0/1-2-v1.log";

static const std::vector<s3_imposter_fixture::expectation>
  default_expectations({
    s3_imposter_fixture::expectation{
      .url = "/" + manifest_url, .body = ss::sstring(manifest_payload)},
    s3_imposter_fixture::expectation{
      .url = "/" + segment_url, .body = "segment1"},
  });

static manifest load_manifest_from_str(std::string_view v) {
    manifest m;
    iobuf i;
    i.append(v.data(), v.size());
    auto s = make_iobuf_input_stream(std::move(i));
    m.update(std::move(s)).get();
    return std::move(m);
}

FIXTURE_TEST(test_download_manifest, s3_imposter_fixture) { // NOLINT
    set_expectations_and_listen(default_expectations);
    auto conf = get_configuration();
    service_probe probe;
    remote remote(s3_connection_limit(10), conf, probe);
    manifest actual(manifest_ntp, manifest_revision);
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
    service_probe probe;
    remote remote(s3_connection_limit(10), conf, probe);
    manifest actual(manifest_ntp, manifest_revision);
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
    set_expectations_and_listen(default_expectations);
    auto conf = get_configuration();
    service_probe probe;
    remote remote(s3_connection_limit(10), conf, probe);
    manifest m(manifest_ntp, manifest_revision);
    auto name = segment_name("1-2-v1.log");
    uint64_t clen = manifest_payload.size();
    auto action = ss::defer([&remote] { remote.stop().get(); });
    auto reset_stream = [] {
        iobuf out;
        out.append(manifest_payload.data(), manifest_payload.size());
        return make_iobuf_input_stream(std::move(out));
    };
    retry_chain_node fib(100ms, 20ms);
    auto res = remote
                 .upload_segment(
                   s3::bucket_name("bucket"), name, clen, reset_stream, m, fib)
                 .get();
    BOOST_REQUIRE(res == upload_result::success);
    const auto& req = get_requests().front();
    BOOST_REQUIRE_EQUAL(req.content_length, clen);
    BOOST_REQUIRE_EQUAL(req.content, ss::sstring(manifest_payload));
}

FIXTURE_TEST(test_upload_segment_timeout, s3_imposter_fixture) { // NOLINT
    auto conf = get_configuration();
    service_probe probe;
    remote remote(s3_connection_limit(10), conf, probe);
    manifest m(manifest_ntp, manifest_revision);
    auto name = segment_name("1-2-v1.log");
    uint64_t clen = manifest_payload.size();
    auto action = ss::defer([&remote] { remote.stop().get(); });
    auto reset_stream = [] {
        iobuf out;
        out.append(manifest_payload.data(), manifest_payload.size());
        return make_iobuf_input_stream(std::move(out));
    };
    retry_chain_node fib(100ms, 20ms);
    auto res = remote
                 .upload_segment(
                   s3::bucket_name("bucket"), name, clen, reset_stream, m, fib)
                 .get();
    BOOST_REQUIRE(res == upload_result::timedout);
}

FIXTURE_TEST(test_download_segment, s3_imposter_fixture) { // NOLINT
    set_expectations_and_listen(default_expectations);
    auto conf = get_configuration();
    auto bucket = s3::bucket_name("bucket");
    service_probe probe;
    remote remote(s3_connection_limit(10), conf, probe);
    manifest m(manifest_ntp, manifest_revision);
    auto name = segment_name("1-2-v1.log");
    uint64_t clen = manifest_payload.size();
    auto action = ss::defer([&remote] { remote.stop().get(); });
    auto reset_stream = [] {
        iobuf out;
        out.append(manifest_payload.data(), manifest_payload.size());
        return make_iobuf_input_stream(std::move(out));
    };
    retry_chain_node fib(100ms, 20ms);
    auto upl_res
      = remote.upload_segment(bucket, name, clen, reset_stream, m, fib).get();
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
      = remote.download_segment(bucket, name, m, try_consume, fib).get();

    BOOST_REQUIRE(dnl_res == download_result::success);
    iobuf_parser p(std::move(downloaded));
    auto actual = p.read_string(p.bytes_left());
    BOOST_REQUIRE(actual == manifest_payload);
}

FIXTURE_TEST(test_download_segment_timeout, s3_imposter_fixture) { // NOLINT
    auto conf = get_configuration();
    auto bucket = s3::bucket_name("bucket");
    service_probe probe;
    remote remote(s3_connection_limit(10), conf, probe);
    manifest m(manifest_ntp, manifest_revision);
    auto name = segment_name("1-2-v1.log");

    iobuf downloaded;
    auto try_consume = [&downloaded](uint64_t, ss::input_stream<char>) {
        return ss::make_ready_future<uint64_t>(0);
    };

    retry_chain_node fib(100ms, 20ms);
    auto dnl_res
      = remote.download_segment(bucket, name, m, try_consume, fib).get();
    BOOST_REQUIRE(dnl_res == download_result::timedout);
}
