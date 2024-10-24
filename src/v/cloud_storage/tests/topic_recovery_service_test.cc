/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/recovery_request.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "cluster/cloud_metadata/tests/manual_mixin.h"
#include "cluster/topic_recovery_service.h"
#include "redpanda/tests/fixture.h"
#include "test_utils/fixture.h"
#include "utils/memory_data_source.h"

#include <seastar/core/iostream.hh>
#include <seastar/core/simple-stream.hh>
#include <seastar/core/sstring.hh>
#include <seastar/http/request.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <memory>
#include <utility>

inline ss::logger test_log("test"); // NOLINT

namespace {
const ss::sstring no_manifests = R"XML(
    <ListBucketResult>
      <IsTruncated>false</IsTruncated>
      <NextContinuationToken>n</NextContinuationToken>
    </ListBucketResult>
    )XML";

const ss::sstring top_level_result = R"XML(
    <ListBucketResult>
      <IsTruncated>false</IsTruncated>
      <Contents>
      </Contents>
      <NextContinuationToken>n</NextContinuationToken>
      <CommonPrefixes>
        <Prefix>b0000000/</Prefix>
      </CommonPrefixes>
    </ListBucketResult>
    )XML";

const ss::sstring valid_manifest_list = R"XML(
    <ListBucketResult>
      <IsTruncated>false</IsTruncated>
      <Contents>
          <Key>b0000000/meta/kafka/test/topic_manifest.json</Key>
      </Contents>
      <NextContinuationToken>n</NextContinuationToken>
    </ListBucketResult>
    )XML";

const ss::sstring recovery_results = R"XML(
    <ListBucketResult>
      <IsTruncated>false</IsTruncated>
      <Contents>
          <Key>recovery_state/kafka/test/0_9c7bc334-a669-4f04-b8c3-81c30b6ef5bf.false</Key>
      </Contents>
      <NextContinuationToken>n</NextContinuationToken>
    </ListBucketResult>
    )XML";

const ss::sstring topic_manifest_json = R"JSON({
      "version": 1,
      "namespace": "kafka",
      "topic": "test",
      "partition_count": 1,
      "replication_factor": 1,
      "revision_id": 1
    })JSON";

const model::topic_namespace tp_ns{model::ns{"kafka"}, model::topic{"test"}};

const s3_imposter_fixture::expectation root_level{
  .url = "?list-type=2&delimiter=/",
  .body = top_level_result,
};

const s3_imposter_fixture::expectation meta_level{
  .url = "?list-type=2&prefix=b0000000/",
  .body = valid_manifest_list,
};

const s3_imposter_fixture::expectation manifest{
  .url = "b0000000/meta/kafka/test/topic_manifest.json",
  .body = topic_manifest_json,
};

const s3_imposter_fixture::expectation recovery_state{
  .url = "?list-type=2&prefix=recovery_state",
  .body = recovery_results,
};

// Generates expectations such that listing on a manifest prefix will result in
// a response that contains no manifests.
std::vector<s3_imposter_fixture::expectation>
generate_no_manifests_expectations(
  std::vector<s3_imposter_fixture::expectation> additional_expectations) {
    const char hex_chars[] = "0123456789abcdef";
    std::vector<s3_imposter_fixture::expectation> expectations;
    for (int i = 0; i < 16; ++i) {
        expectations.emplace_back(s3_imposter_fixture::expectation{
          .url = fmt::format("?list-type=2&prefix={}0000000/", hex_chars[i]),
          .body = no_manifests,
        });
    }
    expectations.emplace_back(s3_imposter_fixture::expectation{
      .url = fmt::format(
        "?list-type=2&prefix=meta/{}/{}/", tp_ns.ns(), tp_ns.tp()),
      .body = no_manifests,
    });
    expectations.emplace_back(s3_imposter_fixture::expectation{
      .url = "?list-type=2&prefix=meta/",
      .body = no_manifests,
    });
    for (auto& e : additional_expectations) {
        expectations.emplace_back(std::move(e));
    }
    return expectations;
}

bool is_manifest_list_request(const http_test_utils::request_info& req) {
    return req.method == "GET" && req.url.contains("?list-type=2&prefix=")
           && req.url.ends_with("0000000/");
}

} // namespace

class fixture
  : public s3_imposter_fixture
  , public manual_metadata_upload_mixin
  , public redpanda_thread_fixture
  , public enable_cloud_storage_fixture {
public:
    fixture()
      : redpanda_thread_fixture(
          redpanda_thread_fixture::init_cloud_storage_tag{},
          httpd_port_number()) {
        // This test will manually set expectations for list requests.
        set_search_on_get_list(false);
    }

    void wait_for_topic(model::topic_namespace tp_ns) {
        RPTEST_REQUIRE_EVENTUALLY(10s, [this, tn = std::move(tp_ns)] {
            const auto& topics
              = app.controller->get_topics_state().local().all_topics();
            const auto has_topic = std::find_if(
                                     topics.cbegin(),
                                     topics.cend(),
                                     [&tn](const auto& tp_ns) {
                                         return tp_ns == tn;
                                     })
                                   != topics.cend();
            return ss::make_ready_future<bool>(has_topic);
        });
    }

    using equals = ss::bool_class<struct equals_tag>;
    void wait_for_n_requests(
      size_t n,
      equals e = equals::no,
      std::optional<req_pred_t> predicate = std::nullopt) {
        RPTEST_REQUIRE_EVENTUALLY(10s, [this, n, e, predicate] {
            const auto matching_requests_size
              = predicate ? get_requests(predicate.value()).size()
                          : get_requests().size();
            return e ? matching_requests_size == n
                     : matching_requests_size >= n;
        });
    }

    cloud_storage::init_recovery_result
    start_recovery(const ss::sstring& payload = "") {
        return app.topic_recovery_service.local()
          .start_recovery(make_request(payload))
          .get();
    }

private:
    ss::http::request make_request(const ss::sstring& payload) {
        ss::http::request r;
        memory_data_source::vector_type v;
        v.emplace_back(payload.data(), payload.size());
        ss::data_source ds(std::make_unique<memory_data_source>(std::move(v)));
        _request_streams.emplace_back(std::move(ds));
        r.content_stream = &_request_streams.back();
        r.content_length = payload.size();
        r._headers["Content-Type"] = "application/json";
        return r;
    }
    // These are here to be cleaned up properly at the end of the test.
    //
    // We use a std::list to keep pointer stability, so streams don't move and
    // pointers stay valid.
    std::list<ss::input_stream<char>> _request_streams;
};

FIXTURE_TEST(start_with_bad_request, fixture) {
    auto result = start_recovery("++");
    auto expected = cloud_storage::init_recovery_result{
      .status_code = ss::http::reply::status_type::bad_request,
      .message = "bad recovery request payload: Invalid value."};
    BOOST_REQUIRE_EQUAL(result, expected);
}

FIXTURE_TEST(start_with_good_request, fixture) {
    auto result = start_recovery();
    auto expected = cloud_storage::init_recovery_result{
      .status_code = ss::http::reply::status_type::accepted,
      .message = "recovery started"};
    BOOST_REQUIRE_EQUAL(result, expected);
}

FIXTURE_TEST(recovery_with_no_topics_exits_early, fixture) {
    set_search_on_get_list(true);
    set_expectations_and_listen({});

    auto& service = app.topic_recovery_service;
    auto result = start_recovery();

    auto expected = cloud_storage::init_recovery_result{
      .status_code = ss::http::reply::status_type::accepted,
      .message = "recovery started"};

    BOOST_REQUIRE_EQUAL(result, expected);

    // Wait until one request is received, to list bucket for manifest files
    wait_for_n_requests(16, equals::yes, is_manifest_list_request);

    const auto& list_topics_req = get_requests()[0];
    BOOST_REQUIRE_EQUAL(
      list_topics_req.url, "/" + url_base() + "?list-type=2&prefix=meta/");

    // Wait until recovery exits after finding no topics to create
    tests::cooperative_spin_wait_with_timeout(10s, [&service] {
        return service.local().is_active() == false;
    }).get();

    // No other calls were made
    BOOST_REQUIRE_EQUAL(get_requests().size(), 17);
}

void do_test(fixture& f) {
    auto& service = f.app.topic_recovery_service;
    auto result = f.start_recovery();

    auto expected = cloud_storage::init_recovery_result{
      .status_code = ss::http::reply::status_type::accepted,
      .message = "recovery started"};

    BOOST_REQUIRE_EQUAL(result, expected);

    // Wait until three requests are received:
    // 1. meta/kafka for labeled topic manifests
    // 2..17. to list bucket for topic meta prefixes
    // 18..20. to download manifest, which now takes three requests
    f.wait_for_n_requests(20, fixture::equals::yes);

    const auto& get_manifest_req = f.get_requests()[19];
    BOOST_REQUIRE_EQUAL(
      get_manifest_req.url, "/" + f.url_base() + manifest.url);

    // Wait until recovery exits after finding no topics to create
    RPTEST_REQUIRE_EVENTUALLY(
      10s, [&service] { return service.local().is_active() == false; });

    BOOST_REQUIRE_EQUAL(f.get_requests().size(), 20);
}

FIXTURE_TEST(recovery_with_unparseable_topic_manifest, fixture) {
    set_expectations_and_listen(generate_no_manifests_expectations(
      {meta_level, {.url = manifest.url, .body = "bad json"}}));
    do_test(*this);
}

FIXTURE_TEST(recovery_with_missing_topic_manifest, fixture) {
    set_expectations_and_listen(generate_no_manifests_expectations({
      meta_level,
    }));
    do_test(*this);
}

FIXTURE_TEST(recovery_with_existing_topic, fixture) {
    cluster::custom_assignable_topic_configuration_vector topic_cfg{
      {cluster::custom_assignable_topic_configuration{
        {model::ns{"kafka"}, model::topic{"test"}, 1, 1}}}};
    auto topic_create_result = app.controller->get_topics_frontend()
                                 .local()
                                 .create_topics(
                                   std::move(topic_cfg), model::no_timeout)
                                 .get();
    wait_for_topics(std::move(topic_create_result)).get();
    set_expectations_and_listen(
      generate_no_manifests_expectations({meta_level}));

    auto& service = app.topic_recovery_service;
    auto result = start_recovery();

    auto expected = cloud_storage::init_recovery_result{
      .status_code = ss::http::reply::status_type::accepted,
      .message = "recovery started"};

    BOOST_REQUIRE_EQUAL(result, expected);
    wait_for_n_requests(16, equals::yes, is_manifest_list_request);

    tests::cooperative_spin_wait_with_timeout(10s, [&service] {
        return service.local().is_active() == false;
    }).get();

    BOOST_REQUIRE_GE(get_requests().size(), 16);
}

FIXTURE_TEST(recovery_where_topic_is_created, fixture) {
    set_expectations_and_listen(generate_no_manifests_expectations(
      {meta_level, manifest, recovery_state}));

    auto& service = app.topic_recovery_service;
    auto result = start_recovery();

    auto expected = cloud_storage::init_recovery_result{
      .status_code = ss::http::reply::status_type::accepted,
      .message = "recovery started"};

    BOOST_REQUIRE_EQUAL(result, expected);
    wait_for_n_requests(16);

    // Wait for the topic to appear
    wait_for_topic(tp_ns);

    // Wait for the topic recovery service to settle into recovery mode
    tests::cooperative_spin_wait_with_timeout(10s, [&service] {
        return service.local().current_state()
               == cloud_storage::topic_recovery_service::state::recovering_data;
    }).get();

    auto topic = app.controller->get_topics_state().local().get_topic_cfg(
      tp_ns);
    BOOST_REQUIRE(topic.has_value());
    BOOST_REQUIRE_EQUAL(topic->partition_count, 1);
    BOOST_REQUIRE_EQUAL(topic->replication_factor, 1);
    BOOST_REQUIRE(topic->is_recovery_enabled());
    BOOST_REQUIRE_EQUAL(
      topic->properties.shadow_indexing, model::shadow_indexing_mode::full);
    BOOST_REQUIRE_EQUAL(
      topic->properties.retention_local_target_bytes.value(),
      config::shard_local_cfg()
        .cloud_storage_recovery_temporary_retention_bytes_default.value());
    // We will have at least three requests, there could be more depending on
    // partition recovery manager:
    // 1. list prefixes
    // 2. list prefix content
    // 3. download manifest
    // 4. try to clear recovery results from previous runs
    BOOST_REQUIRE_GE(get_requests().size(), 18);
}

FIXTURE_TEST(recovery_result_clear_before_start, fixture) {
    set_expectations_and_listen(generate_no_manifests_expectations(
      {meta_level, manifest, recovery_state}));

    start_recovery();
    wait_for_n_requests(22);

    // 1 to check the labeled root, 16 to check each manifest prefix, 3 to
    // download the JSON topic manifest, 1 to check recovery results, 1 to
    // delete.
    const auto& delete_request = get_requests()[21];
    BOOST_REQUIRE_EQUAL(delete_request.url, "/" + url_base() + "?delete");
    BOOST_REQUIRE_EQUAL(delete_request.method, "POST");
}

FIXTURE_TEST(recovery_download_tracking, fixture) {
    set_expectations_and_listen(generate_no_manifests_expectations(
      {meta_level, manifest, recovery_state}));

    start_recovery();
    wait_for_n_requests(3);
    wait_for_topic(tp_ns);

    auto& service = app.topic_recovery_service;
    tests::cooperative_spin_wait_with_timeout(10s, [&service] {
        return service.local().current_state()
               == cloud_storage::topic_recovery_service::state::recovering_data;
    }).get();

    auto download_counts
      = service.local().current_recovery_status().download_counts;
    BOOST_REQUIRE_EQUAL(
      download_counts[tp_ns].failed_downloads
        + download_counts[tp_ns].pending_downloads
        + download_counts[tp_ns].successful_downloads,
      1);
}

FIXTURE_TEST(recovery_with_topic_name_pattern_without_match, fixture) {
    set_expectations_and_listen(generate_no_manifests_expectations({
      meta_level,
    }));

    start_recovery(R"JSON({"topic_names_pattern": "abc*"})JSON");

    wait_for_n_requests(16, equals::yes, is_manifest_list_request);

    auto& service = app.topic_recovery_service;
    tests::cooperative_spin_wait_with_timeout(10s, [&service] {
        return !service.local().is_active();
    }).get();

    BOOST_REQUIRE_EQUAL(get_requests().size(), 17);
}

FIXTURE_TEST(recovery_with_topic_name_pattern_with_match, fixture) {
    set_expectations_and_listen(generate_no_manifests_expectations(
      {meta_level, manifest, recovery_state}));

    start_recovery(R"JSON({"topic_names_pattern": ".*es*"})JSON");

    wait_for_n_requests(19);
    wait_for_topic(tp_ns);
}

FIXTURE_TEST(recovery_with_retention_ms_override, fixture) {
    set_expectations_and_listen(generate_no_manifests_expectations(
      {meta_level, manifest, recovery_state}));

    start_recovery(
      R"JSON({"topic_names_pattern": ".*es*", "retention_ms": 10000})JSON");

    wait_for_n_requests(19);
    wait_for_topic(tp_ns);
    auto topic = app.controller->get_topics_state().local().get_topic_cfg(
      tp_ns);
    BOOST_REQUIRE(topic.has_value());
    BOOST_REQUIRE(
      topic->properties.retention_local_target_ms.has_optional_value());
    BOOST_REQUIRE_EQUAL(
      topic->properties.retention_local_target_ms.value().count(), 10000);
}

FIXTURE_TEST(recovery_with_retention_bytes_override, fixture) {
    set_expectations_and_listen(generate_no_manifests_expectations(
      {meta_level, manifest, recovery_state}));
    start_recovery(
      R"JSON({"topic_names_pattern": ".*es*", "retention_bytes": 10000})JSON");

    wait_for_n_requests(5);
    wait_for_topic(tp_ns);
    auto topic = app.controller->get_topics_state().local().get_topic_cfg(
      tp_ns);
    BOOST_REQUIRE(topic.has_value());
    BOOST_REQUIRE(
      topic->properties.retention_local_target_bytes.has_optional_value());
    BOOST_REQUIRE_EQUAL(
      topic->properties.retention_local_target_bytes.value(), 10000);
}

FIXTURE_TEST(recovery_status, fixture) {
    set_expectations_and_listen(generate_no_manifests_expectations(
      {meta_level, manifest, recovery_state}));

    start_recovery(
      R"JSON({"topic_names_pattern": ".*es*", "retention_bytes": 10000})JSON");

    // capture a status where the recovery is running. if the recovery finishes
    // too fast the test may miss it.
    cloud_storage::topic_recovery_service::recovery_status status;
    auto& service = app.topic_recovery_service;
    tests::cooperative_spin_wait_with_timeout(60s, [&service, &status] {
        status = service.local().current_recovery_status();
        return service.local().current_recovery_status().request.has_value();
    }).get();

    BOOST_REQUIRE_NE(
      status.state, cloud_storage::topic_recovery_service::state::inactive);

    BOOST_REQUIRE(status.request.has_value());
    BOOST_REQUIRE_EQUAL(status.request.value().topic_names_pattern(), ".*es*");
    BOOST_REQUIRE_EQUAL(status.request.value().retention_bytes(), 10000);
    BOOST_REQUIRE(!status.request.value().retention_ms().has_value());
}

FIXTURE_TEST(recovery_status_default, fixture) {
    auto state
      = app.topic_recovery_service.local().current_recovery_status().state;
    BOOST_REQUIRE_EQUAL(
      state, cloud_storage::topic_recovery_service::state::inactive);
}
