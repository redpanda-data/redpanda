/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "bytes/iostream.h"
#include "cloud_storage/base_manifest.h"
#include "cloud_storage/materialized_resources.h"
#include "cloud_storage/offset_translation_layer.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/partition_manifest_downloader.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_path_provider.h"
#include "cloud_storage/remote_segment.h"
#include "cloud_storage/segment_path_utils.h"
#include "cloud_storage/tests/common_def.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/client_pool.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "model/metadata.h"
#include "storage/directories.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"
#include "test_utils/tmp_dir.h"
#include "utils/lazy_abort_source.h"
#include "utils/retry_chain_node.h"
#include "utils/stream_provider.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/resource.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <chrono>
#include <exception>
#include <iterator>

using namespace std::chrono_literals;
using namespace cloud_storage;

inline ss::logger test_log("test"); // NOLINT

static ss::abort_source never_abort;

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

static constexpr std::string_view plural_delete_error = R"json(
<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Error>
        <Key>0</Key>
        <Code>TestFailure</Code>
    </Error>
</DeleteResult>)json";

static lazy_abort_source always_continue{[]() { return std::nullopt; }};

static constexpr model::cloud_credentials_source config_file{
  model::cloud_credentials_source::config_file};

static remote::event_filter allow_all;

static iobuf make_iobuf_from_string(std::string_view s) {
    iobuf b;
    b.append(s.data(), s.size());
    return b;
}

struct noop_mixin_t {};

template<model::cloud_storage_backend backend>
struct backend_override_mixin_t {
    backend_override_mixin_t()
      : _default_backend(
          config::shard_local_cfg().cloud_storage_backend.value()) {
        config::shard_local_cfg().cloud_storage_backend.set_value(
          model::cloud_storage_backend::google_s3_compat);
    }
    ~backend_override_mixin_t() {
        config::shard_local_cfg().cloud_storage_backend.set_value(
          _default_backend);
    }
    model::cloud_storage_backend _default_backend;
};
template<class Mixin>
class remote_fixture_base
  : public s3_imposter_fixture
  , Mixin {
public:
    remote_fixture_base(cloud_storage_clients::s3_url_style url_style)
      : s3_imposter_fixture(url_style) {
        pool.start(10, ss::sharded_parameter([this] { return conf; })).get();
        io.start(
            std::ref(pool),
            ss::sharded_parameter([this] { return conf; }),
            ss::sharded_parameter([] { return config_file; }))
          .get();
        remote
          .start(std::ref(io), ss::sharded_parameter([this] { return conf; }))
          .get();
    }
    ~remote_fixture_base() {
        pool.local().shutdown_connections();
        io.local().request_stop();
        remote.stop().get();
        io.stop().get();
        pool.stop().get();
    }

    ss::sharded<cloud_storage_clients::client_pool> pool;
    ss::sharded<cloud_io::remote> io;
    ss::sharded<remote> remote;
};

struct remote_test_parameters {
    cloud_storage_clients::s3_url_style url_style;
};

using remote_fixture = remote_fixture_base<noop_mixin_t>;
using gcs_remote_fixture = remote_fixture_base<
  backend_override_mixin_t<model::cloud_storage_backend::google_s3_compat>>;

class all_types_remote_fixture
  : public remote_fixture
  , public testing::TestWithParam<remote_test_parameters> {
public:
    all_types_remote_fixture()
      : remote_fixture(GetParam().url_style) {}
};

class all_types_gcs_remote_fixture
  : public gcs_remote_fixture
  , public testing::TestWithParam<remote_test_parameters> {
public:
    all_types_gcs_remote_fixture()
      : gcs_remote_fixture(GetParam().url_style) {}
};

TEST_P(all_types_remote_fixture, test_download_manifest_timeout) { // NOLINT
    partition_manifest actual(manifest_ntp, manifest_revision);
    auto subscription = remote.local().subscribe(allow_all);
    retry_chain_node fib(never_abort, 100ms, 20ms);
    auto res = remote.local()
                 .download_manifest(
                   bucket_name, json_manifest_format_path, actual, fib)
                 .get();
    ASSERT_TRUE(res == download_result::timedout);
    ASSERT_TRUE(subscription.available());
    ASSERT_TRUE(
      subscription.get().type == api_activity_type::manifest_download);
}

TEST_P(all_types_remote_fixture, test_upload_segment) { // NOLINT
    set_expectations_and_listen({});
    auto subscription = remote.local().subscribe(allow_all);
    auto name = segment_name("1-2-v1.log");
    auto path = remote_segment_path{prefixed_segment_path(
      manifest_ntp, manifest_revision, name, model::term_id{123})};
    uint64_t clen = manifest_payload.size();
    auto reset_stream = []() -> ss::future<std::unique_ptr<stream_provider>> {
        iobuf out;
        out.append(manifest_payload.data(), manifest_payload.size());
        co_return std::make_unique<storage::segment_reader_handle>(
          make_iobuf_input_stream(std::move(out)));
    };
    retry_chain_node fib(never_abort, 100ms, 20ms);
    auto res = remote.local()
                 .upload_segment(
                   bucket_name, path, clen, reset_stream, fib, always_continue)
                 .get();
    ASSERT_TRUE(res == upload_result::success);
    const auto& req = get_requests().front();
    ASSERT_EQ(req.content_length, clen);
    ASSERT_EQ(req.content, ss::sstring(manifest_payload));
    ASSERT_TRUE(subscription.available());
    ASSERT_TRUE(subscription.get().type == api_activity_type::segment_upload);
}

TEST_P(
  all_types_remote_fixture, test_upload_segment_lost_leadership) { // NOLINT
    set_expectations_and_listen({});
    auto subscription = remote.local().subscribe(allow_all);
    auto name = segment_name("1-2-v1.log");
    auto path = remote_segment_path{prefixed_segment_path(
      manifest_ntp, manifest_revision, name, model::term_id{123})};
    uint64_t clen = manifest_payload.size();
    auto reset_stream = []() -> ss::future<std::unique_ptr<stream_provider>> {
        iobuf out;
        out.append(manifest_payload.data(), manifest_payload.size());
        co_return std::make_unique<storage::segment_reader_handle>(
          make_iobuf_input_stream(std::move(out)));
    };
    retry_chain_node fib(never_abort, 100ms, 20ms);
    static ss::abort_source never_abort;
    auto lost_leadership = lazy_abort_source{
      []() { return "lost leadership"; }};
    auto res = remote.local()
                 .upload_segment(
                   bucket_name, path, clen, reset_stream, fib, lost_leadership)
                 .get();
    ASSERT_EQ(res, upload_result::cancelled);
    ASSERT_TRUE(get_requests().empty());
    ASSERT_TRUE(subscription.available());
    ASSERT_TRUE(subscription.get().type == api_activity_type::segment_upload);
}

TEST_P(all_types_remote_fixture, test_upload_segment_timeout) { // NOLINT
    auto subscription = remote.local().subscribe(allow_all);
    auto name = segment_name("1-2-v1.log");
    auto path = remote_segment_path{prefixed_segment_path(
      manifest_ntp, manifest_revision, name, model::term_id{123})};
    uint64_t clen = manifest_payload.size();
    auto reset_stream = []() -> ss::future<std::unique_ptr<stream_provider>> {
        iobuf out;
        out.append(manifest_payload.data(), manifest_payload.size());
        co_return std::make_unique<storage::segment_reader_handle>(
          make_iobuf_input_stream(std::move(out)));
    };
    retry_chain_node fib(never_abort, 100ms, 20ms);
    auto res = remote.local()
                 .upload_segment(
                   bucket_name, path, clen, reset_stream, fib, always_continue)
                 .get();
    ASSERT_TRUE(res == upload_result::timedout);
    ASSERT_TRUE(subscription.available());
    ASSERT_TRUE(subscription.get().type == api_activity_type::segment_upload);
}

TEST_P(all_types_remote_fixture, test_download_segment) { // NOLINT
    set_expectations_and_listen({});
    auto subscription = remote.local().subscribe(allow_all);
    auto name = segment_name("1-2-v1.log");
    auto path = remote_segment_path{prefixed_segment_path(
      manifest_ntp, manifest_revision, name, model::term_id{123})};
    uint64_t clen = manifest_payload.size();
    auto reset_stream = []() -> ss::future<std::unique_ptr<stream_provider>> {
        iobuf out;
        out.append(manifest_payload.data(), manifest_payload.size());
        co_return std::make_unique<storage::segment_reader_handle>(
          make_iobuf_input_stream(std::move(out)));
    };
    retry_chain_node fib(never_abort, 100ms, 20ms);
    auto upl_res
      = remote.local()
          .upload_segment(
            bucket_name, path, clen, reset_stream, fib, always_continue)
          .get();
    ASSERT_TRUE(upl_res == upload_result::success);

    iobuf downloaded;
    auto try_consume = [&downloaded](uint64_t len, ss::input_stream<char> is) {
        downloaded.clear();
        auto rds = make_iobuf_ref_output_stream(downloaded);
        return ss::do_with(
          std::move(rds), std::move(is), [&downloaded](auto& rds, auto& is) {
              return ss::copy(is, rds).then(
                [&downloaded] { return downloaded.size_bytes(); });
          });
    };
    auto dnl_res = remote.local()
                     .download_segment(bucket_name, path, try_consume, fib)
                     .get();

    ASSERT_TRUE(dnl_res == download_result::success);
    iobuf_parser p(std::move(downloaded));
    auto actual = p.read_string(p.bytes_left());
    ASSERT_TRUE(actual == manifest_payload);
    ASSERT_TRUE(subscription.available());
    ASSERT_TRUE(subscription.get().type == api_activity_type::segment_upload);
}

TEST_P(all_types_remote_fixture, test_download_segment_timeout) { // NOLINT
    auto subscription = remote.local().subscribe(allow_all);
    auto name = segment_name("1-2-v1.log");
    auto path = remote_segment_path{prefixed_segment_path(
      manifest_ntp, manifest_revision, name, model::term_id{123})};

    auto try_consume = [](uint64_t, ss::input_stream<char>) {
        return ss::make_ready_future<uint64_t>(0);
    };

    retry_chain_node fib(never_abort, 100ms, 20ms);

    auto dnl_res = remote.local()
                     .download_segment(bucket_name, path, try_consume, fib)
                     .get();
    ASSERT_TRUE(dnl_res == download_result::timedout);
    ASSERT_TRUE(subscription.available());
    ASSERT_TRUE(subscription.get().type == api_activity_type::segment_download);
}

TEST_P(all_types_remote_fixture, test_download_segment_range) {
    auto subscription = remote.local().subscribe(allow_all);

    auto path = remote_segment_path{prefixed_segment_path(
      manifest_ntp,
      manifest_revision,
      segment_name("1-2-v1.log"),
      model::term_id{123})};

    retry_chain_node fib(never_abort, 100ms, 20ms);

    set_expectations_and_listen({}, {{"Range"}});

    auto upl_res
      = remote.local()
          .upload_segment(
            bucket_name,
            path,
            manifest_payload.size(),
            []() -> ss::future<std::unique_ptr<stream_provider>> {
                iobuf out;
                out.append(manifest_payload.data(), manifest_payload.size());
                co_return std::make_unique<storage::segment_reader_handle>(
                  make_iobuf_input_stream(std::move(out)));
            },
            fib,
            always_continue)
          .get();
    ASSERT_EQ(upl_res, upload_result::success);

    iobuf downloaded;
    auto dnl_res = remote.local()
                     .download_segment(
                       bucket_name,
                       path,
                       [&downloaded](uint64_t len, ss::input_stream<char> is) {
                           downloaded.clear();
                           auto rds = make_iobuf_ref_output_stream(downloaded);
                           return ss::do_with(
                             std::move(rds),
                             std::move(is),
                             [&downloaded](auto& rds, auto& is) {
                                 return ss::copy(is, rds).then([&downloaded] {
                                     return downloaded.size_bytes();
                                 });
                             });
                       },
                       fib,
                       {{0, 1}})
                     .get();
    ASSERT_EQ(dnl_res, download_result::success);

    iobuf_parser p(std::move(downloaded));
    auto actual = p.read_string(p.bytes_left());

    auto expected = ss::sstring{
      manifest_payload.begin(), manifest_payload.begin() + 2};
    ASSERT_EQ(actual, expected);
    ASSERT_TRUE(subscription.available());
    ASSERT_TRUE(subscription.get().type == api_activity_type::segment_upload);

    const auto& req = get_requests()[1];
    ASSERT_EQ(req.method, "GET");
    ASSERT_EQ(req.header("Range"), "bytes=0-1");
}

TEST_P(all_types_remote_fixture, test_segment_exists) { // NOLINT
    set_expectations_and_listen({});
    auto name = segment_name("1-2-v1.log");
    auto path = remote_segment_path{prefixed_segment_path(
      manifest_ntp, manifest_revision, name, model::term_id{123})};
    uint64_t clen = manifest_payload.size();
    auto reset_stream = []() -> ss::future<std::unique_ptr<stream_provider>> {
        iobuf out;
        out.append(manifest_payload.data(), manifest_payload.size());
        co_return std::make_unique<storage::segment_reader_handle>(
          make_iobuf_input_stream(std::move(out)));
    };

    retry_chain_node fib(never_abort, 100ms, 20ms);

    auto expected_notfound
      = remote.local().segment_exists(bucket_name, path, fib).get();
    ASSERT_TRUE(expected_notfound == download_result::notfound);
    auto upl_res
      = remote.local()
          .upload_segment(
            bucket_name, path, clen, reset_stream, fib, always_continue)
          .get();
    ASSERT_TRUE(upl_res == upload_result::success);

    auto expected_success
      = remote.local().segment_exists(bucket_name, path, fib).get();

    ASSERT_TRUE(expected_success == download_result::success);
}

TEST_P(all_types_remote_fixture, test_segment_exists_timeout) { // NOLINT
    auto name = segment_name("1-2-v1.log");
    auto path = remote_segment_path{prefixed_segment_path(
      manifest_ntp, manifest_revision, name, model::term_id{123})};

    retry_chain_node fib(never_abort, 100ms, 20ms);
    auto expect_timeout
      = remote.local().segment_exists(bucket_name, path, fib).get();
    ASSERT_TRUE(expect_timeout == download_result::timedout);
}

TEST_P(all_types_remote_fixture, test_segment_delete) { // NOLINT
    set_expectations_and_listen({});
    auto name = segment_name("0-1-v1.log");
    auto path = remote_segment_path{prefixed_segment_path(
      manifest_ntp, manifest_revision, name, model::term_id{1})};

    retry_chain_node fib(never_abort, 100ms, 20ms);
    uint64_t clen = manifest_payload.size();
    auto reset_stream = []() -> ss::future<std::unique_ptr<stream_provider>> {
        iobuf out;
        out.append(manifest_payload.data(), manifest_payload.size());
        co_return std::make_unique<storage::segment_reader_handle>(
          make_iobuf_input_stream(std::move(out)));
    };
    auto upl_res
      = remote.local()
          .upload_segment(
            bucket_name, path, clen, reset_stream, fib, always_continue)
          .get();
    ASSERT_TRUE(upl_res == upload_result::success);

    // NOTE: we have to upload something as segment in order for the
    // mock to work correctly.

    auto subscription = remote.local().subscribe(allow_all);

    auto expected_success = remote.local()
                              .delete_object(
                                bucket_name,
                                cloud_storage_clients::object_key(path),
                                fib)
                              .get();
    ASSERT_TRUE(expected_success == upload_result::success);

    auto expected_notfound
      = remote.local().segment_exists(bucket_name, path, fib).get();
    ASSERT_TRUE(expected_notfound == download_result::notfound);
    ASSERT_TRUE(subscription.available());
    ASSERT_TRUE(subscription.get().type == api_activity_type::segment_delete);
}

TEST_P(all_types_remote_fixture, test_concat_segment_upload) {
    temporary_dir tmp_dir("concat_segment_read");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    model::ntp test_ntp{"test_ns", "test_tpc", 0};
    disk_log_builder b{log_config{
      data_path.string(),
      1024,
      ss::default_priority_class(),
      storage::make_sanitized_file_config()}};
    b | start(ntp_config{test_ntp, {data_path}});

    auto defer = ss::defer([&b]() { b.stop().get(); });

    model::offset start_offset{0};
    for (auto i = 0; i < 4; ++i) {
        b | add_segment(start_offset) | add_random_batch(start_offset, 10);
        auto& segment = b.get_segment(i);
        start_offset = segment.offsets().get_dirty_offset() + model::offset{1};
    }

    auto path = remote_segment_path{prefixed_segment_path(
      test_ntp,
      manifest_revision,
      segment_name("1-2-v1.log"),
      model::term_id{123})};

    auto start_pos = 20;
    auto end_pos = b.get_log_segments().back()->file_size() - 20;

    auto reset_stream = [&b, start_pos, end_pos]() {
        return ss::make_ready_future<std::unique_ptr<stream_provider>>(
          std::make_unique<concat_segment_reader_view>(
            std::vector<ss::lw_shared_ptr<segment>>{
              b.get_log_segments().begin(), b.get_log_segments().end()},
            start_pos,
            end_pos,
            ss::default_priority_class()));
    };

    retry_chain_node fib(never_abort, 100ms, 20ms);
    auto upload_size = b.get_disk_log_impl().size_bytes() - 40;

    set_expectations_and_listen({});
    auto upl_res
      = remote.local()
          .upload_segment(
            bucket_name, path, upload_size, reset_stream, fib, always_continue)
          .get();
    ASSERT_EQ(upl_res, upload_result::success);
}

TEST_P(all_types_remote_fixture, test_list_bucket) {
    set_expectations_and_listen({});
    retry_chain_node fib(never_abort, 10s, 20ms);

    int first = 2;
    int second = 3;
    int third = 4;
    for (int i = 0; i < first; i++) {
        for (int j = 0; j < second; j++) {
            for (int k = 0; k < third; k++) {
                cloud_storage_clients::object_key path{
                  fmt::format("{}/{}/{}", i, j, k)};
                auto result
                  = remote.local()
                      .upload_object(
                        {.transfer_details
                         = {.bucket = bucket_name, .key = path, .parent_rtc = fib},
                         .payload = iobuf{}})
                      .get();
                ASSERT_EQ(cloud_storage::upload_result::success, result);
            }
        }
    }
    {
        auto result = remote.local().list_objects(bucket_name, fib).get();
        ASSERT_TRUE(result.has_value());
        ASSERT_EQ(result.value().contents.size(), first * second * third);
        ASSERT_TRUE(result.value().common_prefixes.empty());
    }
    {
        auto result = remote.local()
                        .list_objects(
                          bucket_name,
                          fib,
                          cloud_storage_clients::object_key{url_base()},
                          '/')
                        .get();

        ASSERT_TRUE(result.has_value());
        ASSERT_TRUE(result.value().contents.empty());
        ASSERT_EQ(result.value().common_prefixes.size(), first);
    }
    {
        cloud_storage_clients::object_key prefix{url_base() + "1/"};
        auto result
          = remote.local().list_objects(bucket_name, fib, prefix).get();
        ASSERT_TRUE(result.has_value());
        ASSERT_EQ(result.value().contents.size(), second * third);
        ASSERT_TRUE(result.value().common_prefixes.empty());
    }
    {
        cloud_storage_clients::object_key prefix{url_base() + "1/"};
        auto result
          = remote.local().list_objects(bucket_name, fib, prefix, '/').get();
        ASSERT_TRUE(result.has_value());
        ASSERT_TRUE(result.value().contents.empty());
        ASSERT_EQ(result.value().common_prefixes.size(), second);
    }
}

TEST_P(all_types_remote_fixture, test_list_bucket_with_max_keys) {
    set_expectations_and_listen({});
    retry_chain_node fib(never_abort, 10s, 20ms);

    const auto s3_imposter_max_keys = s3_imposter_fixture::default_max_keys;
    const auto size = s3_imposter_max_keys + 50;
    for (int i = 0; i < size; i++) {
        cloud_storage_clients::object_key path{fmt::format("{}", i)};
        auto result
          = remote.local()
              .upload_object(
                {.transfer_details
                 = {.bucket = bucket_name, .key = path, .parent_rtc = fib},
                 .payload = iobuf{}})
              .get();
        ASSERT_EQ(cloud_storage::upload_result::success, result);
    }

    {
        // Passing max_keys indicates we, as a user, will handle truncation
        // results. Here, we know that that size > s3_imposter_max_keys, and the
        // result will end up truncated.
        auto max_keys = s3_imposter_max_keys;
        auto result = remote.local()
                        .list_objects(
                          bucket_name,
                          fib,
                          std::nullopt,
                          std::nullopt,
                          std::nullopt,
                          max_keys)
                        .get();

        ASSERT_TRUE(result.has_value());
        ASSERT_TRUE(result.value().is_truncated);
        // This continuation token is 54 because objects are sorted
        // lexicographically.
        ASSERT_EQ(
          result.value().next_continuation_token, "/" + url_base() + "54");
        ASSERT_EQ(result.value().contents.size(), s3_imposter_max_keys);
        ASSERT_TRUE(result.value().common_prefixes.empty());

        // Now, we can use the next_continuation_token from the previous,
        // truncated result in order to query for the rest of the objects. We
        // should expect to get the rest of the objects in "storage", and that
        // this request is not truncated.
        auto next_result = remote.local()
                             .list_objects(
                               bucket_name,
                               fib,
                               std::nullopt,
                               std::nullopt,
                               std::nullopt,
                               max_keys,
                               result.value().next_continuation_token)
                             .get();
        ASSERT_TRUE(next_result.has_value());
        ASSERT_TRUE(!next_result.value().is_truncated);
        ASSERT_EQ(
          next_result.value().contents.size(), size - s3_imposter_max_keys);
        ASSERT_TRUE(next_result.value().common_prefixes.empty());
    }
    {
        // On the other hand, passing max_keys as std::nullopt means
        // truncation will be handled by the remote API, (all object keys will
        // be read in a loop, we should expect no truncation in the return
        // value), and the result contents should be full.
        auto max_keys = std::nullopt;
        auto result = remote.local()
                        .list_objects(
                          bucket_name,
                          fib,
                          std::nullopt,
                          std::nullopt,
                          std::nullopt,
                          max_keys)
                        .get();
        ASSERT_TRUE(result.has_value());
        ASSERT_TRUE(!result.value().is_truncated);
        ASSERT_EQ(result.value().contents.size(), size);
        ASSERT_TRUE(result.value().common_prefixes.empty());
    }
    {
        auto max_keys = 2;
        auto result = remote.local()
                        .list_objects(
                          bucket_name,
                          fib,
                          std::nullopt,
                          std::nullopt,
                          std::nullopt,
                          max_keys)
                        .get();

        ASSERT_TRUE(result.has_value());
        ASSERT_TRUE(result.value().is_truncated);
        // This continuation token is /10 because objects are sorted
        // lexicographically.
        ASSERT_EQ(
          result.value().next_continuation_token, "/" + url_base() + "10");
        const auto& contents = result.value().contents;
        ASSERT_EQ(contents.size(), max_keys);
        ASSERT_EQ(contents[0].key, url_base() + "0");
        ASSERT_EQ(contents[1].key, url_base() + "1");
        ASSERT_TRUE(result.value().common_prefixes.empty());
    }
    {
        // This will also be truncated, since size > s3_imposter_max_keys.
        auto max_keys = size;
        auto result = remote.local()
                        .list_objects(
                          bucket_name,
                          fib,
                          std::nullopt,
                          std::nullopt,
                          std::nullopt,
                          max_keys)
                        .get();

        ASSERT_TRUE(result.has_value());
        ASSERT_TRUE(result.value().is_truncated);
        ASSERT_EQ(result.value().contents.size(), s3_imposter_max_keys);
        // This continuation token is 54 because objects are sorted
        // lexicographically.
        ASSERT_EQ(
          result.value().next_continuation_token, "/" + url_base() + "54");
        ASSERT_TRUE(result.value().common_prefixes.empty());

        // Reissue another request with continuation-token. This should capture
        // the rest of the object keys, we expect a non-truncated result.
        auto next_result = remote.local()
                             .list_objects(
                               bucket_name,
                               fib,
                               std::nullopt,
                               std::nullopt,
                               std::nullopt,
                               max_keys,
                               result.value().next_continuation_token)
                             .get();
        ASSERT_TRUE(next_result.has_value());
        ASSERT_TRUE(!next_result.value().is_truncated);
        ASSERT_EQ(
          next_result.value().contents.size(), size - s3_imposter_max_keys);
        ASSERT_TRUE(next_result.value().common_prefixes.empty());
    }
}

TEST_P(all_types_remote_fixture, test_list_bucket_with_prefix) {
    set_expectations_and_listen({});
    retry_chain_node fib(never_abort, 100ms, 20ms);
    for (const char first : {'x', 'y'}) {
        for (const char second : {'a', 'b'}) {
            cloud_storage_clients::object_key path{
              fmt::format("{}/{}", first, second)};
            auto result
              = remote.local()
                  .upload_object(
                    {.transfer_details
                     = {.bucket = bucket_name, .key = path, .parent_rtc = fib},
                     .payload = iobuf{}})
                  .get();
            ASSERT_EQ(cloud_storage::upload_result::success, result);
        }
    }

    auto result = remote.local()
                    .list_objects(
                      bucket_name,
                      fib,
                      cloud_storage_clients::object_key{url_base() + "x/"})
                    .get();
    ASSERT_TRUE(result.has_value());
    auto items = result.value().contents;
    ASSERT_EQ(items.size(), 2);
    ASSERT_EQ(items[0].key, url_base() + "x/a");
    ASSERT_EQ(items[1].key, url_base() + "x/b");
    auto request = get_requests().back();
    ASSERT_EQ(request.method, "GET");
    ASSERT_EQ(request.q_list_type, "2");
    ASSERT_EQ(request.q_prefix, url_base() + "x/");
}

TEST_P(all_types_remote_fixture, test_list_bucket_with_filter) {
    set_expectations_and_listen({});
    retry_chain_node fib(never_abort, 100ms, 20ms);
    cloud_storage_clients::object_key path{"b"};
    auto upl_result
      = remote.local()
          .upload_object(
            {.transfer_details
             = {.bucket = bucket_name, .key = path, .parent_rtc = fib},
             .payload = iobuf{}})
          .get();
    ASSERT_EQ(cloud_storage::upload_result::success, upl_result);

    auto path_with_prefix = ss::sstring{url_base() + "b"};
    auto result = remote.local()
                    .list_objects(
                      bucket_name,
                      fib,
                      cloud_storage_clients::object_key{url_base()},
                      std::nullopt,
                      [path_with_prefix](const auto& item) {
                          return item.key == path_with_prefix;
                      })
                    .get();
    ASSERT_TRUE(result.has_value());
    auto items = result.value().contents;
    ASSERT_EQ(items.size(), 1);
    ASSERT_EQ(items[0].key, path_with_prefix);
}

TEST_P(all_types_remote_fixture, test_put_string) {
    set_expectations_and_listen({});
    auto conf = get_configuration();

    retry_chain_node fib(never_abort, 100ms, 20ms);

    cloud_storage_clients::object_key path{"p"};
    auto subscription = remote.local().subscribe(allow_all);
    auto result
      = remote.local()
          .upload_object(
            {.transfer_details
             = {.bucket = bucket_name, .key = path, .parent_rtc = fib},
             .payload = make_iobuf_from_string("p")})
          .get();
    ASSERT_EQ(cloud_storage::upload_result::success, result);

    auto request = get_requests()[0];
    ASSERT_TRUE(request.method == "PUT");
    ASSERT_TRUE(request.content == "p");

    ASSERT_TRUE(subscription.available());
    ASSERT_TRUE(subscription.get().type == api_activity_type::object_upload);
}

TEST_P(all_types_remote_fixture, test_delete_objects) {
    set_expectations_and_listen({});

    retry_chain_node fib(never_abort, 100ms, 20ms);

    std::vector<cloud_storage_clients::object_key> to_delete{
      cloud_storage_clients::object_key{"a"},
      cloud_storage_clients::object_key{"b"}};
    auto result
      = remote.local().delete_objects(bucket_name, to_delete, fib).get();
    ASSERT_EQ(cloud_storage::upload_result::success, result);
    auto request = get_requests()[0];
    ASSERT_EQ(request.method, "POST");
    ASSERT_EQ(request.url, "/" + url_base() + "?delete");
    ASSERT_TRUE(request.has_q_delete);
}

TEST_P(all_types_remote_fixture, test_delete_objects_multiple_batches) {
    set_expectations_and_listen({});

    retry_chain_node fib(never_abort, 500ms, 20ms);

    std::vector<cloud_storage_clients::object_key> to_delete;
    for (auto k :
         boost::irange(remote.local().delete_objects_max_keys() * 2.6)) {
        to_delete.emplace_back(fmt::format("{}", k));
    }

    auto result
      = remote.local().delete_objects(bucket_name, to_delete, fib).get();
    ASSERT_EQ(cloud_storage::upload_result::success, result);
    auto requests = get_requests();
    ASSERT_EQ(requests.size(), 3);

    std::vector<cloud_storage_clients::object_key> deleted_keys;

    for (const auto& request : requests) {
        ASSERT_EQ(request.method, "POST");
        ASSERT_EQ(request.url, "/" + url_base() + "?delete");
        ASSERT_TRUE(request.has_q_delete);

        auto request_keys = keys_from_delete_objects_request(request);
        deleted_keys.insert(
          deleted_keys.begin(),
          std::make_move_iterator(request_keys.begin()),
          std::make_move_iterator(request_keys.end()));
    }

    std::sort(to_delete.begin(), to_delete.end());
    std::sort(deleted_keys.begin(), deleted_keys.end());

    ASSERT_EQ(to_delete, deleted_keys);
}

TEST_P(
  all_types_remote_fixture,
  test_delete_objects_multiple_batches_single_failure) {
    set_expectations_and_listen({expectation{
      .url = "?delete", .body = ss::sstring(plural_delete_error)}});

    retry_chain_node fib(never_abort, 500ms, 20ms);

    std::vector<cloud_storage_clients::object_key> to_delete;
    for (auto k :
         boost::irange(remote.local().delete_objects_max_keys() * 2.6)) {
        to_delete.emplace_back(fmt::format("{}", k));
    }

    auto result
      = remote.local().delete_objects(bucket_name, to_delete, fib).get();
    ASSERT_EQ(cloud_storage::upload_result::failed, result);
    auto requests = get_requests();
    ASSERT_EQ(requests.size(), 3);

    std::vector<cloud_storage_clients::object_key> deleted_keys;

    for (const auto& request : requests) {
        ASSERT_EQ(request.method, "POST");
        ASSERT_EQ(request.url, "/" + url_base() + "?delete");
        ASSERT_TRUE(request.has_q_delete);

        auto request_keys = keys_from_delete_objects_request(request);
        deleted_keys.insert(
          deleted_keys.begin(),
          std::make_move_iterator(request_keys.begin()),
          std::make_move_iterator(request_keys.end()));
    }

    std::sort(to_delete.begin(), to_delete.end());
    std::sort(deleted_keys.begin(), deleted_keys.end());

    ASSERT_EQ(to_delete, deleted_keys);
}

TEST_P(all_types_remote_fixture, test_delete_objects_failure_handling) {
    // Test that the failure to delete one key via the plural form
    // fails the entire operation.
    set_expectations_and_listen({expectation{
      .url = "?delete", .body = ss::sstring(plural_delete_error)}});

    retry_chain_node fib(never_abort, 100ms, 20ms);

    std::vector<cloud_storage_clients::object_key> to_delete{
      cloud_storage_clients::object_key{"0"},
      cloud_storage_clients::object_key{"1"}};
    auto result
      = remote.local().delete_objects(bucket_name, to_delete, fib).get();
    ASSERT_EQ(cloud_storage::upload_result::failed, result);

    auto request = get_requests()[0];
    ASSERT_EQ(request.method, "POST");
    ASSERT_EQ(request.url, "/" + url_base() + "?delete");
    ASSERT_TRUE(request.has_q_delete);
}

TEST_P(all_types_gcs_remote_fixture, test_delete_objects_on_unknown_backend) {
    set_expectations_and_listen({});

    retry_chain_node fib(never_abort, 60s, 20ms);

    ASSERT_EQ(
      cloud_storage::upload_result::success,
      remote.local()
        .upload_object(
          {.transfer_details
           = {.bucket = bucket_name, .key = cloud_storage_clients::object_key{"p"}, .parent_rtc = fib},
           .payload = make_iobuf_from_string("p")})
        .get());
    ASSERT_EQ(
      cloud_storage::upload_result::success,
      remote.local()
        .upload_object(
          {.transfer_details
           = {.bucket = bucket_name, .key = cloud_storage_clients::object_key{"q"}, .parent_rtc = fib},
           .payload = make_iobuf_from_string("q")})
        .get());

    std::vector<cloud_storage_clients::object_key> to_delete{
      cloud_storage_clients::object_key{"p"},
      cloud_storage_clients::object_key{"q"}};
    auto result
      = remote.local().delete_objects(bucket_name, to_delete, fib).get();
    ASSERT_EQ(cloud_storage::upload_result::success, result);

    ASSERT_EQ(get_requests().size(), 4);
    auto first_delete = get_requests()[2];

    std::unordered_set<ss::sstring> expected_urls{
      "/" + url_base() + "p", "/" + url_base() + "q"};
    ASSERT_EQ(first_delete.method, "DELETE");
    ASSERT_TRUE(expected_urls.contains(first_delete.url));

    expected_urls.erase(first_delete.url);
    auto second_delete = get_requests()[3];
    ASSERT_EQ(second_delete.method, "DELETE");
    ASSERT_TRUE(expected_urls.contains(second_delete.url));
}

TEST_P(
  all_types_gcs_remote_fixture,
  test_delete_objects_on_unknown_backend_result_reduction) {
    set_expectations_and_listen({});

    retry_chain_node fib(never_abort, 5s, 20ms);

    ASSERT_EQ(
      cloud_storage::upload_result::success,
      remote.local()
        .upload_object(
          {.transfer_details
           = {.bucket = bucket_name, .key = cloud_storage_clients::object_key{"p"}, .parent_rtc = fib},
           .payload = make_iobuf_from_string("p")})
        .get());

    std::vector<cloud_storage_clients::object_key> to_delete{
      // can be deleted
      cloud_storage_clients::object_key{"p"},
      // will time out
      cloud_storage_clients::object_key{"failme"}};

    auto result
      = remote.local().delete_objects(bucket_name, to_delete, fib).get();
    if (conf.url_style == cloud_storage_clients::s3_url_style::virtual_host) {
        // Due to virtual-host style addressing, this will timeout as DNS tries
        // to resolve the request with the provided bucket name.
        ASSERT_EQ(cloud_storage::upload_result::timedout, result);
    } else {
        // But, if we have path style addressing, the object won't be found, a
        // warning will be issued, and the request will return success instead.
        ASSERT_EQ(cloud_storage::upload_result::success, result);
    }
}

TEST_P(all_types_remote_fixture, test_filter_by_source) { // NOLINT
    set_expectations_and_listen({expectation{
      .url = manifest_url, .body = ss::sstring(manifest_payload)}});
    auto conf = get_configuration();
    retry_chain_node root_rtc(never_abort, 100ms, 20ms);
    remote::event_filter flt;
    flt.add_source_to_ignore(&root_rtc);

    auto subscription = remote.local().subscribe(flt);
    partition_manifest actual(manifest_ntp, manifest_revision);

    // We shouldn't receive notification here because the rtc node
    // is ignored by the filter
    retry_chain_node child_rtc(&root_rtc);
    auto res = remote.local()
                 .download_manifest(
                   bucket_name, json_manifest_format_path, actual, child_rtc)
                 .get();
    ASSERT_TRUE(res == download_result::success);
    ASSERT_TRUE(!subscription.available());

    // In this case the caller is different and the manifest download
    // shold trigger notification.
    retry_chain_node other_rtc(never_abort, 100ms, 20ms);
    res = remote.local()
            .download_manifest(
              bucket_name, json_manifest_format_path, actual, other_rtc)
            .get();
    ASSERT_TRUE(res == download_result::success);
    ASSERT_TRUE(subscription.available());
    ASSERT_TRUE(
      subscription.get().type == api_activity_type::manifest_download);

    // Reuse filter for the next event
    subscription = remote.local().subscribe(flt);
    res = remote.local()
            .download_manifest(
              bucket_name, json_manifest_format_path, actual, other_rtc)
            .get();
    ASSERT_TRUE(res == download_result::success);
    ASSERT_TRUE(subscription.available());
    ASSERT_TRUE(
      subscription.get().type == api_activity_type::manifest_download);

    // Remove the rtc node from the filter and re-subscribe. This time we should
    // receive the notification.
    flt.remove_source_to_ignore(&root_rtc);
    subscription = remote.local().subscribe(flt);
    res = remote.local()
            .download_manifest(
              bucket_name, json_manifest_format_path, actual, child_rtc)
            .get();
    ASSERT_TRUE(res == download_result::success);
    ASSERT_TRUE(subscription.available());
    ASSERT_TRUE(
      subscription.get().type == api_activity_type::manifest_download);
}

TEST_P(all_types_remote_fixture, test_filter_by_type) { // NOLINT
    set_expectations_and_listen({expectation{
      .url = manifest_url, .body = ss::sstring(manifest_payload)}});
    retry_chain_node root_rtc(never_abort, 100ms, 20ms);
    partition_manifest actual(manifest_ntp, manifest_revision);

    remote::event_filter flt1({api_activity_type::manifest_download});
    remote::event_filter flt2({api_activity_type::manifest_upload});
    auto subscription1 = remote.local().subscribe(flt1);
    auto subscription2 = remote.local().subscribe(flt2);

    auto dl_res = remote.local()
                    .download_manifest(
                      bucket_name, json_manifest_format_path, actual, root_rtc)
                    .get();

    ASSERT_TRUE(dl_res == download_result::success);
    ASSERT_TRUE(!subscription1.available());
    ASSERT_TRUE(subscription2.available());
    ASSERT_TRUE(
      subscription2.get().type == api_activity_type::manifest_download);

    auto upl_res
      = remote.local()
          .upload_manifest(
            bucket_name, actual, json_manifest_format_path.second, root_rtc)
          .get();
    ASSERT_TRUE(upl_res == upload_result::success);
    ASSERT_TRUE(subscription1.available());
    ASSERT_TRUE(subscription1.get().type == api_activity_type::manifest_upload);
}

TEST_P(all_types_remote_fixture, test_filter_lifetime_1) { // NOLINT
    set_expectations_and_listen({expectation{
      .url = manifest_url, .body = ss::sstring(manifest_payload)}});
    retry_chain_node root_rtc(never_abort, 100ms, 20ms);
    partition_manifest actual(manifest_ntp, manifest_revision);

    std::optional<remote::event_filter> flt;
    flt.emplace();
    auto subscription = remote.local().subscribe(*flt);
    retry_chain_node child_rtc(&root_rtc);
    auto res = remote.local()
                 .download_manifest(
                   bucket_name, json_manifest_format_path, actual, child_rtc)
                 .get();
    flt.reset();
    // Notification should be received despite the fact that the filter object
    // is destroyed.
    ASSERT_TRUE(res == download_result::success);
    ASSERT_TRUE(subscription.available());
    ASSERT_TRUE(
      subscription.get().type == api_activity_type::manifest_download);
}

TEST_P(all_types_remote_fixture, test_filter_lifetime_2) { // NOLINT
    std::optional<remote::event_filter> flt;
    flt.emplace();
    auto subscription = remote.local().subscribe(*flt);
    flt.reset();
    ASSERT_THROW(subscription.get(), ss::broken_promise);
}

struct throttle_low_limit {
    throttle_low_limit() {
        config::shard_local_cfg()
          .cloud_storage_max_throughput_per_shard.set_value(
            manifest_payload.size());
        vlog(
          test_log.info,
          "CONF throughput: {}",
          config::shard_local_cfg().cloud_storage_max_throughput_per_shard());
    }
    ~throttle_low_limit() {
        config::shard_local_cfg()
          .cloud_storage_max_throughput_per_shard.reset();
    }
};

using throttle_remote_fixture = remote_fixture_base<throttle_low_limit>;

class all_types_throttle_remote_fixture
  : public throttle_remote_fixture
  , public testing::TestWithParam<remote_test_parameters> {
public:
    all_types_throttle_remote_fixture()
      : throttle_remote_fixture(GetParam().url_style) {}
};

TEST_P(
  all_types_throttle_remote_fixture,
  test_download_segment_throttle) { // NOLINT
    set_expectations_and_listen({});
    auto subscription = remote.local().subscribe(allow_all);
    auto name = segment_name("1-2-v1.log");
    auto path = remote_segment_path{prefixed_segment_path(
      manifest_ntp, manifest_revision, name, model::term_id{123})};
    uint64_t clen = manifest_payload.size();
    auto reset_stream = []() -> ss::future<std::unique_ptr<stream_provider>> {
        iobuf out;
        out.append(manifest_payload.data(), manifest_payload.size());
        co_return std::make_unique<storage::segment_reader_handle>(
          make_iobuf_input_stream(std::move(out)));
    };
    retry_chain_node fib(never_abort, 100ms, 20ms);
    auto upl_res
      = remote.local()
          .upload_segment(
            bucket_name, path, clen, reset_stream, fib, always_continue)
          .get();
    ASSERT_TRUE(upl_res == upload_result::success);

    auto download_one = [](cloud_storage::remote& api, auto path, auto bucket) {
        retry_chain_node fib(never_abort, 100ms, 20ms);
        iobuf downloaded;
        auto try_consume = [&downloaded](
                             uint64_t len, ss::input_stream<char> is) {
            downloaded.clear();
            auto rds = make_iobuf_ref_output_stream(downloaded);
            return ss::do_with(
              std::move(rds),
              std::move(is),
              [&downloaded](auto& rds, auto& is) {
                  return ss::copy(is, rds).then(
                    [&downloaded] { return downloaded.size_bytes(); });
              });
        };
        auto dnl_res
          = api.download_segment(bucket, path, try_consume, fib).get();

        ASSERT_TRUE(dnl_res == download_result::success);
        iobuf_parser p(std::move(downloaded));
        auto actual = p.read_string(p.bytes_left());
        // segment and manifest has the same synthetic payload in this test
        ASSERT_TRUE(actual == manifest_payload);
    };
    download_one(remote.local(), path, bucket_name);
    auto s1 = remote.local()
                .materialized()
                .get_read_path_probe()
                .get_downloads_throttled_sum();
    ASSERT_TRUE(s1 == 0);
    download_one(remote.local(), path, bucket_name);
    auto s2 = remote.local()
                .materialized()
                .get_read_path_probe()
                .get_downloads_throttled_sum();
    ASSERT_TRUE(s2 > 0);
}

struct no_throttle {
    no_throttle() {
        config::shard_local_cfg()
          .cloud_storage_throughput_limit_percent.set_value(0);
        config::shard_local_cfg()
          .cloud_storage_max_throughput_per_shard.set_value(
            manifest_payload.size());
    }
    ~no_throttle() {
        config::shard_local_cfg()
          .cloud_storage_throughput_limit_percent.reset();
        config::shard_local_cfg()
          .cloud_storage_max_throughput_per_shard.reset();
    }
};

using no_throttle_remote_fixture = remote_fixture_base<no_throttle>;

class all_types_no_throttle_remote_fixture
  : public no_throttle_remote_fixture
  , public testing::TestWithParam<remote_test_parameters> {
public:
    all_types_no_throttle_remote_fixture()
      : no_throttle_remote_fixture(GetParam().url_style) {}
};

// This test checks that the throttling can actually be disabled
TEST_P(
  all_types_no_throttle_remote_fixture,
  test_download_segment_no_throttle) { // NOLINT
    set_expectations_and_listen({});
    auto subscription = remote.local().subscribe(allow_all);
    auto name = segment_name("1-2-v1.log");
    auto path = remote_segment_path{prefixed_segment_path(
      manifest_ntp, manifest_revision, name, model::term_id{123})};
    uint64_t clen = manifest_payload.size();
    auto reset_stream = []() -> ss::future<std::unique_ptr<stream_provider>> {
        iobuf out;
        out.append(manifest_payload.data(), manifest_payload.size());
        co_return std::make_unique<storage::segment_reader_handle>(
          make_iobuf_input_stream(std::move(out)));
    };
    static const auto timeout = 1s;
    static const auto backoff = 10ms;
    retry_chain_node fib(never_abort, timeout, backoff);
    auto upl_res
      = remote.local()
          .upload_segment(
            bucket_name, path, clen, reset_stream, fib, always_continue)
          .get();
    ASSERT_TRUE(upl_res == upload_result::success);

    auto download_one =
      [](cloud_storage::remote& api, auto path, auto bucket) { // NOLINT
          retry_chain_node fib(never_abort, timeout, backoff);
          auto downloaded = ss::make_lw_shared<iobuf>();
          auto try_consume = [downloaded](uint64_t, ss::input_stream<char> is) {
              auto rds = make_iobuf_ref_output_stream(*downloaded);
              return ss::do_with(
                std::move(rds),
                std::move(is),
                downloaded,
                [](auto& rds, auto& is, auto dl) { // NOLINT
                    return ss::copy(is, rds).then(
                      [dl] { return dl->size_bytes(); });
                });
          };
          auto dnl_res
            = api.download_segment(bucket, path, try_consume, fib).get();

          ASSERT_TRUE(dnl_res == download_result::success);
          iobuf_parser p(std::move(*downloaded));
          auto actual = p.read_string(p.bytes_left());
          ASSERT_TRUE(actual == manifest_payload);
      };
    for (int i = 0; i < 100; i++) {
        download_one(remote.local(), path, bucket_name);
        auto times_throttled = remote.local()
                                 .materialized()
                                 .get_read_path_probe()
                                 .get_downloads_throttled_sum();
        ASSERT_TRUE(times_throttled == 0);
    }
}

TEST_P(all_types_remote_fixture, test_notification_retry_meta) {
    set_expectations_and_listen(
      {expectation{.url = manifest_serde_url, .slowdown = true}});

    retry_chain_node fib(never_abort, 500ms, 10ms);
    partition_manifest actual(manifest_ntp, manifest_revision);
    auto filter = remote::event_filter{};

    remote_path_provider path_provider(std::nullopt, std::nullopt);
    partition_manifest_downloader dl(
      bucket_name,
      path_provider,
      manifest_ntp,
      manifest_revision,
      remote.local());

    auto fut = dl.download_manifest(fib, &actual);

    RPTEST_REQUIRE_EVENTUALLY(2s, [&] {
        auto sub = remote.local().subscribe(filter);
        return sub.then([](api_activity_notification event) {
            return ss::make_ready_future<bool>(event.is_retry);
        });
    });

    auto res = fut.get();
    EXPECT_TRUE(res.has_error());
    EXPECT_TRUE(res.error() == error_outcome::manifest_download_error);
}

TEST_P(all_types_remote_fixture, test_get_object) {
    set_expectations_and_listen({});
    auto conf = get_configuration();

    retry_chain_node fib(never_abort, 1s, 20ms);

    cloud_storage_clients::object_key path{"p"};

    ASSERT_EQ(
      cloud_storage::upload_result::success,
      remote.local()
        .upload_object({
          .transfer_details
          = {.bucket = bucket_name, .key = path, .parent_rtc = fib},
          .payload = make_iobuf_from_string("p"),
        })
        .get());

    auto subscription = remote.local().subscribe(allow_all);
    iobuf buf;
    auto dl_res
      = remote.local()
          .download_object(
            {.transfer_details
             = {.bucket = bucket_name, .key = path, .parent_rtc = fib},
             .payload = buf})
          .get();

    const auto requests = get_requests();
    ASSERT_EQ(requests.size(), 2);

    const auto last_request = requests.back();
    ASSERT_EQ(last_request.method, "GET");
    ASSERT_EQ(last_request.url, "/" + url_base() + "p");

    ASSERT_TRUE(dl_res == download_result::success);
    ASSERT_EQ(iobuf_to_bytes(buf), bytes::from_string("p"));
    ASSERT_TRUE(subscription.available());
    ASSERT_TRUE(subscription.get().type == api_activity_type::object_download);
}

INSTANTIATE_TEST_SUITE_P(
  test_with_all_types_remote_fixture,
  all_types_remote_fixture,
  testing::Values(
    remote_test_parameters{
      .url_style = cloud_storage_clients::s3_url_style::virtual_host},
    remote_test_parameters{
      .url_style = cloud_storage_clients::s3_url_style::path}));

INSTANTIATE_TEST_SUITE_P(
  test_with_all_types_gcs_remote_fixture,
  all_types_gcs_remote_fixture,
  testing::Values(
    remote_test_parameters{
      .url_style = cloud_storage_clients::s3_url_style::virtual_host},
    remote_test_parameters{
      .url_style = cloud_storage_clients::s3_url_style::path}));

INSTANTIATE_TEST_SUITE_P(
  test_with_all_types_throttle_remote_fixture,
  all_types_throttle_remote_fixture,
  testing::Values(
    remote_test_parameters{
      .url_style = cloud_storage_clients::s3_url_style::virtual_host},
    remote_test_parameters{
      .url_style = cloud_storage_clients::s3_url_style::path}));

INSTANTIATE_TEST_SUITE_P(
  test_with_all_types_no_throttle_remote_fixture,
  all_types_no_throttle_remote_fixture,
  testing::Values(
    remote_test_parameters{
      .url_style = cloud_storage_clients::s3_url_style::virtual_host},
    remote_test_parameters{
      .url_style = cloud_storage_clients::s3_url_style::path}));
