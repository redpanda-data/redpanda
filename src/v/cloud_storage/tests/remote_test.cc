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
#include "bytes/iostream.h"
#include "cloud_storage/offset_translation_layer.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_segment.h"
#include "cloud_storage/tests/common_def.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/client_pool.h"
#include "config/configuration.h"
#include "model/metadata.h"
#include "seastarx.h"
#include "storage/directories.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"
#include "test_utils/tmp_dir.h"
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

static constexpr std::string_view list_response = R"XML(
<ListBucketResult>
   <IsTruncated>false</IsTruncated>
   <Contents>
      <Key>a</Key>
   </Contents>
  <Contents>
      <Key>b</Key>
   </Contents>
   <NextContinuationToken>n</NextContinuationToken>
</ListBucketResult>
)XML";

static cloud_storage::lazy_abort_source always_continue{
  []() { return std::nullopt; }};

static constexpr model::cloud_credentials_source config_file{
  model::cloud_credentials_source::config_file};

static partition_manifest load_manifest_from_str(std::string_view v) {
    partition_manifest m;
    iobuf i;
    i.append(v.data(), v.size());
    auto s = make_iobuf_input_stream(std::move(i));
    m.update(std::move(s)).get();
    return m;
}

static remote::event_filter allow_all;

static iobuf make_iobuf_from_string(std::string_view s) {
    iobuf b;
    b.append(s.data(), s.size());
    return b;
}

static const cloud_storage_clients::object_tag_formatter upload_tags{{}};

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
    remote_fixture_base() {
        auto conf = get_configuration();
        pool
          .start(
            10, ss::sharded_parameter([this] { return get_configuration(); }))
          .get();
        remote
          .start(
            std::ref(pool),
            ss::sharded_parameter([this] { return get_configuration(); }),
            ss::sharded_parameter([] { return config_file; }))
          .get();
    }
    ~remote_fixture_base() {
        pool.local().shutdown_connections();
        remote.stop().get();
        pool.stop().get();
    }
    ss::sharded<cloud_storage_clients::client_pool> pool;
    ss::sharded<remote> remote;
};

using remote_fixture = remote_fixture_base<noop_mixin_t>;
using gcs_remote_fixture = remote_fixture_base<
  backend_override_mixin_t<model::cloud_storage_backend::google_s3_compat>>;

FIXTURE_TEST(test_download_manifest, remote_fixture) { // NOLINT
    set_expectations_and_listen({expectation{
      .url = "/" + manifest_url, .body = ss::sstring(manifest_payload)}});
    auto subscription = remote.local().subscribe(allow_all);
    partition_manifest actual(manifest_ntp, manifest_revision);
    retry_chain_node fib(never_abort, 100ms, 20ms);
    auto res = remote.local()
                 .download_manifest(
                   cloud_storage_clients::bucket_name("bucket"),
                   remote_manifest_path(std::filesystem::path(manifest_url)),
                   actual,
                   fib)
                 .get();
    BOOST_REQUIRE(res == download_result::success);
    BOOST_REQUIRE(subscription.available());
    BOOST_REQUIRE(
      subscription.get() == api_activity_notification::manifest_download);
    auto expected = load_manifest_from_str(manifest_payload);
    BOOST_REQUIRE(expected == actual); // NOLINT
}

FIXTURE_TEST(test_download_manifest_timeout, remote_fixture) { // NOLINT
    partition_manifest actual(manifest_ntp, manifest_revision);
    auto subscription = remote.local().subscribe(allow_all);
    retry_chain_node fib(never_abort, 100ms, 20ms);
    auto res = remote.local()
                 .download_manifest(
                   cloud_storage_clients::bucket_name("bucket"),
                   remote_manifest_path(std::filesystem::path(manifest_url)),
                   actual,
                   fib)
                 .get();
    BOOST_REQUIRE(res == download_result::timedout);
    BOOST_REQUIRE(subscription.available());
    BOOST_REQUIRE(
      subscription.get() == api_activity_notification::manifest_download);
}

FIXTURE_TEST(test_upload_segment, remote_fixture) { // NOLINT
    set_expectations_and_listen({});
    auto subscription = remote.local().subscribe(allow_all);
    auto name = segment_name("1-2-v1.log");
    auto path = generate_remote_segment_path(
      manifest_ntp, manifest_revision, name, model::term_id{123});
    uint64_t clen = manifest_payload.size();
    auto reset_stream =
      []() -> ss::future<std::unique_ptr<storage::stream_provider>> {
        iobuf out;
        out.append(manifest_payload.data(), manifest_payload.size());
        co_return std::make_unique<storage::segment_reader_handle>(
          make_iobuf_input_stream(std::move(out)));
    };
    retry_chain_node fib(never_abort, 100ms, 20ms);
    auto res = remote.local()
                 .upload_segment(
                   cloud_storage_clients::bucket_name("bucket"),
                   path,
                   clen,
                   reset_stream,
                   fib,
                   always_continue)
                 .get();
    BOOST_REQUIRE(res == upload_result::success);
    const auto& req = get_requests().front();
    BOOST_REQUIRE_EQUAL(req.content_length, clen);
    BOOST_REQUIRE_EQUAL(req.content, ss::sstring(manifest_payload));
    BOOST_REQUIRE(subscription.available());
    BOOST_REQUIRE(
      subscription.get() == api_activity_notification::segment_upload);
}

FIXTURE_TEST(test_upload_segment_lost_leadership, remote_fixture) { // NOLINT
    set_expectations_and_listen({});
    auto subscription = remote.local().subscribe(allow_all);
    auto name = segment_name("1-2-v1.log");
    auto path = generate_remote_segment_path(
      manifest_ntp, manifest_revision, name, model::term_id{123});
    uint64_t clen = manifest_payload.size();
    auto reset_stream =
      []() -> ss::future<std::unique_ptr<storage::stream_provider>> {
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
                   cloud_storage_clients::bucket_name("bucket"),
                   path,
                   clen,
                   reset_stream,
                   fib,
                   lost_leadership)
                 .get();
    BOOST_REQUIRE_EQUAL(res, upload_result::cancelled);
    BOOST_REQUIRE(get_requests().empty());
    BOOST_REQUIRE(subscription.available());
    BOOST_REQUIRE(
      subscription.get() == api_activity_notification::segment_upload);
}

FIXTURE_TEST(test_upload_segment_timeout, remote_fixture) { // NOLINT
    auto subscription = remote.local().subscribe(allow_all);
    auto name = segment_name("1-2-v1.log");
    auto path = generate_remote_segment_path(
      manifest_ntp, manifest_revision, name, model::term_id{123});
    uint64_t clen = manifest_payload.size();
    auto reset_stream =
      []() -> ss::future<std::unique_ptr<storage::stream_provider>> {
        iobuf out;
        out.append(manifest_payload.data(), manifest_payload.size());
        co_return std::make_unique<storage::segment_reader_handle>(
          make_iobuf_input_stream(std::move(out)));
    };
    retry_chain_node fib(never_abort, 100ms, 20ms);
    auto res = remote.local()
                 .upload_segment(
                   cloud_storage_clients::bucket_name("bucket"),
                   path,
                   clen,
                   reset_stream,
                   fib,
                   always_continue)
                 .get();
    BOOST_REQUIRE(res == upload_result::timedout);
    BOOST_REQUIRE(subscription.available());
    BOOST_REQUIRE(
      subscription.get() == api_activity_notification::segment_upload);
}

FIXTURE_TEST(test_download_segment, remote_fixture) { // NOLINT
    set_expectations_and_listen({});
    auto bucket = cloud_storage_clients::bucket_name("bucket");
    auto subscription = remote.local().subscribe(allow_all);
    auto name = segment_name("1-2-v1.log");
    auto path = generate_remote_segment_path(
      manifest_ntp, manifest_revision, name, model::term_id{123});
    uint64_t clen = manifest_payload.size();
    auto reset_stream =
      []() -> ss::future<std::unique_ptr<storage::stream_provider>> {
        iobuf out;
        out.append(manifest_payload.data(), manifest_payload.size());
        co_return std::make_unique<storage::segment_reader_handle>(
          make_iobuf_input_stream(std::move(out)));
    };
    retry_chain_node fib(never_abort, 100ms, 20ms);
    auto upl_res = remote.local()
                     .upload_segment(
                       bucket, path, clen, reset_stream, fib, always_continue)
                     .get();
    BOOST_REQUIRE(upl_res == upload_result::success);

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
    auto dnl_res
      = remote.local().download_segment(bucket, path, try_consume, fib).get();

    BOOST_REQUIRE(dnl_res == download_result::success);
    iobuf_parser p(std::move(downloaded));
    auto actual = p.read_string(p.bytes_left());
    BOOST_REQUIRE(actual == manifest_payload);
    BOOST_REQUIRE(subscription.available());
    BOOST_REQUIRE(
      subscription.get() == api_activity_notification::segment_upload);
}

FIXTURE_TEST(test_download_segment_timeout, remote_fixture) { // NOLINT
    auto bucket = cloud_storage_clients::bucket_name("bucket");
    auto subscription = remote.local().subscribe(allow_all);
    auto name = segment_name("1-2-v1.log");
    auto path = generate_remote_segment_path(
      manifest_ntp, manifest_revision, name, model::term_id{123});

    auto try_consume = [](uint64_t, ss::input_stream<char>) {
        return ss::make_ready_future<uint64_t>(0);
    };

    retry_chain_node fib(never_abort, 100ms, 20ms);
    auto dnl_res
      = remote.local().download_segment(bucket, path, try_consume, fib).get();
    BOOST_REQUIRE(dnl_res == download_result::timedout);
    BOOST_REQUIRE(subscription.available());
    BOOST_REQUIRE(
      subscription.get() == api_activity_notification::segment_download);
}

FIXTURE_TEST(test_segment_exists, remote_fixture) { // NOLINT
    set_expectations_and_listen({});
    auto bucket = cloud_storage_clients::bucket_name("bucket");
    auto name = segment_name("1-2-v1.log");
    auto path = generate_remote_segment_path(
      manifest_ntp, manifest_revision, name, model::term_id{123});
    uint64_t clen = manifest_payload.size();
    auto reset_stream =
      []() -> ss::future<std::unique_ptr<storage::stream_provider>> {
        iobuf out;
        out.append(manifest_payload.data(), manifest_payload.size());
        co_return std::make_unique<storage::segment_reader_handle>(
          make_iobuf_input_stream(std::move(out)));
    };

    retry_chain_node fib(never_abort, 100ms, 20ms);

    auto expected_notfound
      = remote.local().segment_exists(bucket, path, fib).get();
    BOOST_REQUIRE(expected_notfound == download_result::notfound);
    auto upl_res = remote.local()
                     .upload_segment(
                       bucket, path, clen, reset_stream, fib, always_continue)
                     .get();
    BOOST_REQUIRE(upl_res == upload_result::success);

    auto expected_success
      = remote.local().segment_exists(bucket, path, fib).get();

    BOOST_REQUIRE(expected_success == download_result::success);
}

FIXTURE_TEST(test_segment_exists_timeout, remote_fixture) { // NOLINT
    auto bucket = cloud_storage_clients::bucket_name("bucket");
    auto name = segment_name("1-2-v1.log");
    auto path = generate_remote_segment_path(
      manifest_ntp, manifest_revision, name, model::term_id{123});

    retry_chain_node fib(never_abort, 100ms, 20ms);
    auto expect_timeout
      = remote.local().segment_exists(bucket, path, fib).get();
    BOOST_REQUIRE(expect_timeout == download_result::timedout);
}

FIXTURE_TEST(test_segment_delete, remote_fixture) { // NOLINT
    set_expectations_and_listen({});
    auto bucket = cloud_storage_clients::bucket_name("bucket");
    auto name = segment_name("0-1-v1.log");
    auto path = generate_remote_segment_path(
      manifest_ntp, manifest_revision, name, model::term_id{1});

    retry_chain_node fib(never_abort, 100ms, 20ms);
    uint64_t clen = manifest_payload.size();
    auto reset_stream =
      []() -> ss::future<std::unique_ptr<storage::stream_provider>> {
        iobuf out;
        out.append(manifest_payload.data(), manifest_payload.size());
        co_return std::make_unique<storage::segment_reader_handle>(
          make_iobuf_input_stream(std::move(out)));
    };
    auto upl_res = remote.local()
                     .upload_segment(
                       bucket, path, clen, reset_stream, fib, always_continue)
                     .get();
    BOOST_REQUIRE(upl_res == upload_result::success);

    // NOTE: we have to upload something as segment in order for the
    // mock to work correctly.

    auto subscription = remote.local().subscribe(allow_all);

    auto expected_success
      = remote.local()
          .delete_object(bucket, cloud_storage_clients::object_key(path), fib)
          .get();
    BOOST_REQUIRE(expected_success == upload_result::success);

    auto expected_notfound
      = remote.local().segment_exists(bucket, path, fib).get();
    BOOST_REQUIRE(expected_notfound == download_result::notfound);
    BOOST_REQUIRE(subscription.available());
    BOOST_REQUIRE(
      subscription.get() == api_activity_notification::segment_delete);
}

FIXTURE_TEST(test_concat_segment_upload, remote_fixture) {
    temporary_dir tmp_dir("concat_segment_read");
    auto data_path = tmp_dir.get_path();
    using namespace storage;

    model::ntp test_ntp{"test_ns", "test_tpc", 0};
    disk_log_builder b{log_config{
      data_path.string(),
      1024,
      debug_sanitize_files::yes,
    }};
    b | start(ntp_config{test_ntp, {data_path}});

    auto defer = ss::defer([&b]() { b.stop().get(); });

    model::offset start_offset{0};
    for (auto i = 0; i < 4; ++i) {
        b | add_segment(start_offset) | add_random_batch(start_offset, 10);
        auto& segment = b.get_segment(i);
        start_offset = segment.offsets().dirty_offset + model::offset{1};
    }

    auto bucket = cloud_storage_clients::bucket_name("bucket");

    auto path = generate_remote_segment_path(
      test_ntp,
      manifest_revision,
      segment_name("1-2-v1.log"),
      model::term_id{123});

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
            bucket, path, upload_size, reset_stream, fib, always_continue)
          .get();
    BOOST_REQUIRE_EQUAL(upl_res, upload_result::success);
}

FIXTURE_TEST(test_list_bucket, remote_fixture) {
    set_expectations_and_listen(
      {{.url = "/?list-type=2",
        .body = ss::sstring{list_response.data(), list_response.size()}}});

    cloud_storage_clients::bucket_name bucket{"test"};
    retry_chain_node fib(never_abort, 100ms, 20ms);
    auto result = remote.local().list_objects(bucket, fib).get();
    BOOST_REQUIRE(result.has_value());

    auto items = result.value();
    BOOST_REQUIRE_EQUAL(items.contents.size(), 2);
}

FIXTURE_TEST(test_list_bucket_with_prefix, remote_fixture) {
    set_expectations_and_listen(
      {{.url = "/?list-type=2&prefix=x",
        .body = ss::sstring{list_response.data(), list_response.size()}}});

    cloud_storage_clients::bucket_name bucket{"test"};
    retry_chain_node fib(never_abort, 100ms, 20ms);
    auto result = remote.local()
                    .list_objects(
                      bucket, fib, cloud_storage_clients::object_key{"x"})
                    .get();
    BOOST_REQUIRE(result.has_value());
    auto items = result.value().contents;
    BOOST_REQUIRE_EQUAL(items.size(), 2);
    BOOST_REQUIRE_EQUAL(items[0].key, "a");
    BOOST_REQUIRE_EQUAL(items[1].key, "b");
    auto request = get_requests()[0];
    BOOST_REQUIRE_EQUAL(request.method, "GET");
    BOOST_REQUIRE_EQUAL(request.q_list_type, "2");
    BOOST_REQUIRE_EQUAL(request.q_prefix, "x");
    BOOST_REQUIRE_EQUAL(request.h_prefix, "x");
}

FIXTURE_TEST(test_list_bucket_with_filter, remote_fixture) {
    set_expectations_and_listen(
      {{.url = "/?list-type=2",
        .body = ss::sstring{list_response.data(), list_response.size()}}});

    cloud_storage_clients::bucket_name bucket{"test"};
    retry_chain_node fib(never_abort, 100ms, 20ms);
    auto result = remote.local()
                    .list_objects(
                      bucket,
                      fib,
                      std::nullopt,
                      std::nullopt,
                      [](const auto& item) { return item.key == "b"; })
                    .get();
    BOOST_REQUIRE(result.has_value());
    auto items = result.value().contents;
    BOOST_REQUIRE_EQUAL(items.size(), 1);
    BOOST_REQUIRE_EQUAL(items[0].key, "b");
}

FIXTURE_TEST(test_put_string, remote_fixture) {
    set_expectations_and_listen({});
    auto conf = get_configuration();

    cloud_storage_clients::bucket_name bucket{"test"};
    retry_chain_node fib(never_abort, 100ms, 20ms);

    cloud_storage_clients::object_key path{"p"};
    auto result
      = remote.local()
          .upload_object(
            bucket, path, make_iobuf_from_string("p"), fib, upload_tags)
          .get();
    BOOST_REQUIRE_EQUAL(cloud_storage::upload_result::success, result);

    auto request = get_requests()[0];
    BOOST_REQUIRE(request.method == "PUT");
    BOOST_REQUIRE(request.content == "p");
}

FIXTURE_TEST(test_delete_objects, remote_fixture) {
    set_expectations_and_listen({});

    cloud_storage_clients::bucket_name bucket{"test"};
    retry_chain_node fib(never_abort, 100ms, 20ms);

    std::vector<cloud_storage_clients::object_key> to_delete{
      cloud_storage_clients::object_key{"a"},
      cloud_storage_clients::object_key{"b"}};
    auto result = remote.local().delete_objects(bucket, to_delete, fib).get();
    BOOST_REQUIRE_EQUAL(cloud_storage::upload_result::success, result);
    auto request = get_requests()[0];
    BOOST_REQUIRE_EQUAL(request.method, "POST");
    BOOST_REQUIRE_EQUAL(request.url, "/?delete");
    BOOST_REQUIRE(request.has_q_delete);
}

FIXTURE_TEST(test_delete_objects_on_unknown_backend, gcs_remote_fixture) {
    set_expectations_and_listen({});

    cloud_storage_clients::bucket_name bucket{"test"};
    retry_chain_node fib(never_abort, 60s, 20ms);

    BOOST_REQUIRE_EQUAL(
      cloud_storage::upload_result::success,
      remote.local()
        .upload_object(
          bucket,
          cloud_storage_clients::object_key{"p"},
          make_iobuf_from_string("p"),
          fib,
          upload_tags)
        .get());
    BOOST_REQUIRE_EQUAL(
      cloud_storage::upload_result::success,
      remote.local()
        .upload_object(
          bucket,
          cloud_storage_clients::object_key{"q"},
          make_iobuf_from_string("q"),
          fib,
          upload_tags)
        .get());

    std::vector<cloud_storage_clients::object_key> to_delete{
      cloud_storage_clients::object_key{"p"},
      cloud_storage_clients::object_key{"q"}};
    auto result = remote.local().delete_objects(bucket, to_delete, fib).get();
    BOOST_REQUIRE_EQUAL(cloud_storage::upload_result::success, result);

    BOOST_REQUIRE_EQUAL(get_requests().size(), 4);
    auto first_delete = get_requests()[2];

    std::unordered_set<ss::sstring> expected_urls{"/p", "/q"};
    BOOST_REQUIRE_EQUAL(first_delete.method, "DELETE");
    BOOST_REQUIRE(expected_urls.contains(first_delete.url));

    expected_urls.erase(first_delete.url);
    auto second_delete = get_requests()[3];
    BOOST_REQUIRE_EQUAL(second_delete.method, "DELETE");
    BOOST_REQUIRE(expected_urls.contains(second_delete.url));
}

FIXTURE_TEST(
  test_delete_objects_on_unknown_backend_result_reduction, gcs_remote_fixture) {
    set_expectations_and_listen({});

    cloud_storage_clients::bucket_name bucket{"test"};
    retry_chain_node fib(never_abort, 5s, 20ms);

    BOOST_REQUIRE_EQUAL(
      cloud_storage::upload_result::success,
      remote.local()
        .upload_object(
          bucket,
          cloud_storage_clients::object_key{"p"},
          make_iobuf_from_string("p"),
          fib,
          upload_tags)
        .get());

    std::vector<cloud_storage_clients::object_key> to_delete{
      // can be deleted
      cloud_storage_clients::object_key{"p"},
      // will time out
      cloud_storage_clients::object_key{"failme"}};

    auto result = remote.local().delete_objects(bucket, to_delete, fib).get();
    BOOST_REQUIRE_EQUAL(cloud_storage::upload_result::timedout, result);
}

FIXTURE_TEST(test_filter_by_source, remote_fixture) { // NOLINT
    set_expectations_and_listen({expectation{
      .url = "/" + manifest_url, .body = ss::sstring(manifest_payload)}});
    auto conf = get_configuration();
    retry_chain_node root_rtc(never_abort, 100ms, 20ms);
    remote::event_filter flt(root_rtc);
    auto subscription = remote.local().subscribe(flt);
    partition_manifest actual(manifest_ntp, manifest_revision);

    // We shouldn't receive notification here because the rtc node
    // is ignored by the filter
    retry_chain_node child_rtc(&root_rtc);
    auto res = remote.local()
                 .download_manifest(
                   cloud_storage_clients::bucket_name("bucket"),
                   remote_manifest_path(std::filesystem::path(manifest_url)),
                   actual,
                   child_rtc)
                 .get();
    BOOST_REQUIRE(res == download_result::success);
    BOOST_REQUIRE(!subscription.available());

    // In this case the caller is different and the manifest download
    // shold trigger notification.
    retry_chain_node other_rtc(never_abort, 100ms, 20ms);
    res = remote.local()
            .download_manifest(
              cloud_storage_clients::bucket_name("bucket"),
              remote_manifest_path(std::filesystem::path(manifest_url)),
              actual,
              other_rtc)
            .get();
    BOOST_REQUIRE(res == download_result::success);
    BOOST_REQUIRE(subscription.available());
    BOOST_REQUIRE(
      subscription.get() == api_activity_notification::manifest_download);

    // Reuse filter for the next event
    subscription = remote.local().subscribe(flt);
    res = remote.local()
            .download_manifest(
              cloud_storage_clients::bucket_name("bucket"),
              remote_manifest_path(std::filesystem::path(manifest_url)),
              actual,
              other_rtc)
            .get();
    BOOST_REQUIRE(res == download_result::success);
    BOOST_REQUIRE(subscription.available());
    BOOST_REQUIRE(
      subscription.get() == api_activity_notification::manifest_download);
}

FIXTURE_TEST(test_filter_by_type, remote_fixture) { // NOLINT
    set_expectations_and_listen({expectation{
      .url = "/" + manifest_url, .body = ss::sstring(manifest_payload)}});
    retry_chain_node root_rtc(never_abort, 100ms, 20ms);
    partition_manifest actual(manifest_ntp, manifest_revision);

    remote::event_filter flt1({api_activity_notification::manifest_download});
    remote::event_filter flt2({api_activity_notification::manifest_upload});
    auto subscription1 = remote.local().subscribe(flt1);
    auto subscription2 = remote.local().subscribe(flt2);

    auto dl_res = remote.local()
                    .download_manifest(
                      cloud_storage_clients::bucket_name("bucket"),
                      remote_manifest_path(std::filesystem::path(manifest_url)),
                      actual,
                      root_rtc)
                    .get();

    BOOST_REQUIRE(dl_res == download_result::success);
    BOOST_REQUIRE(!subscription1.available());
    BOOST_REQUIRE(subscription2.available());
    BOOST_REQUIRE(
      subscription2.get() == api_activity_notification::manifest_download);

    auto upl_res = remote.local()
                     .upload_manifest(
                       cloud_storage_clients::bucket_name("bucket"),
                       actual,
                       root_rtc)
                     .get();
    BOOST_REQUIRE(upl_res == upload_result::success);
    BOOST_REQUIRE(subscription1.available());
    BOOST_REQUIRE(
      subscription1.get() == api_activity_notification::manifest_upload);
}

FIXTURE_TEST(test_filter_lifetime_1, remote_fixture) { // NOLINT
    set_expectations_and_listen({expectation{
      .url = "/" + manifest_url, .body = ss::sstring(manifest_payload)}});
    retry_chain_node root_rtc(never_abort, 100ms, 20ms);
    partition_manifest actual(manifest_ntp, manifest_revision);

    std::optional<remote::event_filter> flt;
    flt.emplace();
    auto subscription = remote.local().subscribe(*flt);
    retry_chain_node child_rtc(&root_rtc);
    auto res = remote.local()
                 .download_manifest(
                   cloud_storage_clients::bucket_name("bucket"),
                   remote_manifest_path(std::filesystem::path(manifest_url)),
                   actual,
                   child_rtc)
                 .get();
    flt.reset();
    // Notification should be received despite the fact that the filter object
    // is destroyed.
    BOOST_REQUIRE(res == download_result::success);
    BOOST_REQUIRE(subscription.available());
    BOOST_REQUIRE(
      subscription.get() == api_activity_notification::manifest_download);
}

FIXTURE_TEST(test_filter_lifetime_2, remote_fixture) { // NOLINT
    std::optional<remote::event_filter> flt;
    flt.emplace();
    auto subscription = remote.local().subscribe(*flt);
    flt.reset();
    BOOST_REQUIRE_THROW(subscription.get(), ss::broken_promise);
}
