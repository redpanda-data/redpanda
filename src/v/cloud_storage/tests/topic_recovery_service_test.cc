/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/tests/s3_imposter.h"
#include "cluster/topic_recovery_service.h"
#include "redpanda/tests/fixture.h"
#include "test_utils/fixture.h"

#include <seastar/http/request.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <utility>

inline ss::logger test_log("test"); // NOLINT

namespace {
const ss::sstring no_manifests = R"XML(
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
  .url = "/?list-type=2&delimiter=/",
  .body = top_level_result,
};

const s3_imposter_fixture::expectation meta_level{
  .url = "/?list-type=2&prefix=b0000000/",
  .body = valid_manifest_list,
};

const s3_imposter_fixture::expectation manifest{
  .url = "/b0000000/meta/kafka/test/topic_manifest.json",
  .body = topic_manifest_json,
};

const s3_imposter_fixture::expectation recovery_state{
  .url = "/?list-type=2&prefix=recovery_state",
  .body = recovery_results,
};

} // namespace

class fixture
  : public s3_imposter_fixture
  , public redpanda_thread_fixture
  , public enable_cloud_storage_fixture {
public:
    fixture()
      : redpanda_thread_fixture(
        redpanda_thread_fixture::init_cloud_storage_tag{},
        httpd_port_number()) {}

    void wait_for_topic(model::topic_namespace tp_ns) {
        tests::cooperative_spin_wait_with_timeout(
          10s,
          [this, tn = std::move(tp_ns)] {
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
          })
          .get();
    }

    using equals = ss::bool_class<struct equals_tag>;
    void wait_for_n_requests(size_t n, equals e = equals::no) {
        tests::cooperative_spin_wait_with_timeout(10s, [this, n, e] {
            if (e) {
                return get_requests().size() == n;
            } else {
                return get_requests().size() >= n;
            }
        }).get();
    }
};

FIXTURE_TEST(start_with_bad_request, fixture) {
    ss::http::request r;
    r.content = "++";
    r.content_length = 2;
    r._headers["Content-Type"] = "application/json";

    auto result = app.topic_recovery_service.local().start_recovery(r);
    auto expected = cloud_storage::init_recovery_result{
      .status_code = ss::http::reply::status_type::bad_request,
      .message = "bad recovery request payload: Invalid value."};
    BOOST_REQUIRE_EQUAL(result, expected);
}

FIXTURE_TEST(start_with_good_request, fixture) {
    auto result = app.topic_recovery_service.local().start_recovery({});
    auto expected = cloud_storage::init_recovery_result{
      .status_code = ss::http::reply::status_type::accepted,
      .message = "recovery started"};
    BOOST_REQUIRE_EQUAL(result, expected);
}

FIXTURE_TEST(recovery_with_no_topics_exits_early, fixture) {
    set_expectations_and_listen(
      {{.url = root_level.url, .body = no_manifests}});

    auto& service = app.topic_recovery_service;
    auto result = service.local().start_recovery({});

    auto expected = cloud_storage::init_recovery_result{
      .status_code = ss::http::reply::status_type::accepted,
      .message = "recovery started"};

    BOOST_REQUIRE_EQUAL(result, expected);

    // Wait until one request is received, to list bucket for manifest files
    wait_for_n_requests(1, equals::yes);

    const auto& list_topics_req = get_requests()[0];
    BOOST_REQUIRE_EQUAL(list_topics_req.url, root_level.url);

    // Wait until recovery exits after finding no topics to create
    tests::cooperative_spin_wait_with_timeout(10s, [&service] {
        return service.local().is_active() == false;
    }).get();

    // No other calls were made
    BOOST_REQUIRE_EQUAL(get_requests().size(), 1);
}

void do_test(fixture& f) {
    auto& service = f.app.topic_recovery_service;
    auto result = service.local().start_recovery({});

    auto expected = cloud_storage::init_recovery_result{
      .status_code = ss::http::reply::status_type::accepted,
      .message = "recovery started"};

    BOOST_REQUIRE_EQUAL(result, expected);

    // Wait until three requests are received:
    // 1. to list bucket for topic meta prefixes
    // 2. to list the topic meta prefix itself
    // 3. to download manifest
    f.wait_for_n_requests(3, fixture::equals::yes);

    const auto& list_topics_req = f.get_requests()[0];
    BOOST_REQUIRE_EQUAL(list_topics_req.url, root_level.url);

    const auto& list_prefix_req = f.get_requests()[1];
    BOOST_REQUIRE_EQUAL(list_prefix_req.url, meta_level.url);

    const auto& get_manifest_req = f.get_requests()[2];
    BOOST_REQUIRE_EQUAL(get_manifest_req.url, manifest.url);

    // Wait until recovery exits after finding no topics to create
    tests::cooperative_spin_wait_with_timeout(10s, [&service] {
        return service.local().is_active() == false;
    }).get();

    BOOST_REQUIRE_EQUAL(f.get_requests().size(), 3);
}

FIXTURE_TEST(recovery_with_unparseable_topic_manifest, fixture) {
    set_expectations_and_listen(
      {root_level, meta_level, {.url = manifest.url, .body = "bad json"}});
    do_test(*this);
}

FIXTURE_TEST(recovery_with_missing_topic_manifest, fixture) {
    set_expectations_and_listen({
      root_level,
      meta_level,
    });
    do_test(*this);
}

FIXTURE_TEST(recovery_with_existing_topic, fixture) {
    cluster::topic_configuration cfg{
      model::ns{"kafka"}, model::topic{"test"}, 1, 1};
    std::vector<cluster::custom_assignable_topic_configuration> topic_cfg = {
      cluster::custom_assignable_topic_configuration{std::move(cfg)}};
    auto topic_create_result = app.controller->get_topics_frontend()
                                 .local()
                                 .create_topics(topic_cfg, model::no_timeout)
                                 .get();
    wait_for_topics(std::move(topic_create_result)).get();
    set_expectations_and_listen({root_level, meta_level});

    auto& service = app.topic_recovery_service;
    auto result = service.local().start_recovery({});

    auto expected = cloud_storage::init_recovery_result{
      .status_code = ss::http::reply::status_type::accepted,
      .message = "recovery started"};

    BOOST_REQUIRE_EQUAL(result, expected);
    wait_for_n_requests(2, equals::yes);

    const auto& list_topics_req = get_requests()[0];
    BOOST_REQUIRE_EQUAL(list_topics_req.url, root_level.url);

    const auto& prefix_req = get_requests()[1];
    BOOST_REQUIRE_EQUAL(prefix_req.url, meta_level.url);

    tests::cooperative_spin_wait_with_timeout(10s, [&service] {
        return service.local().is_active() == false;
    }).get();

    BOOST_REQUIRE_EQUAL(get_requests().size(), 2);
}

FIXTURE_TEST(recovery_where_topic_is_created, fixture) {
    set_expectations_and_listen(
      {root_level, meta_level, manifest, recovery_state});

    auto& service = app.topic_recovery_service;
    auto result = service.local().start_recovery({});

    auto expected = cloud_storage::init_recovery_result{
      .status_code = ss::http::reply::status_type::accepted,
      .message = "recovery started"};

    BOOST_REQUIRE_EQUAL(result, expected);
    wait_for_n_requests(3);

    const auto& list_topics_req = get_requests()[0];
    BOOST_REQUIRE_EQUAL(list_topics_req.url, root_level.url);

    const auto& prefix_req = get_requests()[1];
    BOOST_REQUIRE_EQUAL(prefix_req.url, meta_level.url);

    const auto& get_manifest = get_requests()[2];
    BOOST_REQUIRE_EQUAL(get_manifest.url, manifest.url);

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
    BOOST_REQUIRE_GE(get_requests().size(), 4);
}

FIXTURE_TEST(recovery_result_clear_before_start, fixture) {
    set_expectations_and_listen(
      {root_level, meta_level, manifest, recovery_state});

    auto& service = app.topic_recovery_service;
    service.local().start_recovery({});
    wait_for_n_requests(5);

    const auto& delete_request = get_requests()[4];
    BOOST_REQUIRE_EQUAL(delete_request.url, "/?delete");
    BOOST_REQUIRE_EQUAL(delete_request.method, "POST");
}

FIXTURE_TEST(recovery_download_tracking, fixture) {
    set_expectations_and_listen(
      {root_level, meta_level, manifest, recovery_state});

    auto& service = app.topic_recovery_service;
    service.local().start_recovery({});
    wait_for_n_requests(3);
    wait_for_topic(tp_ns);

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
    set_expectations_and_listen({
      root_level,
      meta_level,
    });

    ss::http::request r;
    r._headers = {{"Content-Type", "application/json"}};
    r.content = R"JSON({"topic_names_pattern": "abc*"})JSON";
    r.content_length = 1;
    auto& service = app.topic_recovery_service;
    service.local().start_recovery(r);

    wait_for_n_requests(2, equals::yes);

    tests::cooperative_spin_wait_with_timeout(10s, [&service] {
        return !service.local().is_active();
    }).get();

    BOOST_REQUIRE_EQUAL(get_requests().size(), 2);
}

FIXTURE_TEST(recovery_with_topic_name_pattern_with_match, fixture) {
    set_expectations_and_listen(
      {root_level, meta_level, manifest, recovery_state});

    ss::http::request r;
    r._headers = {{"Content-Type", "application/json"}};
    r.content_length = 1;
    r.content = R"JSON({"topic_names_pattern": ".*es*"})JSON";
    auto& service = app.topic_recovery_service;
    service.local().start_recovery(r);

    wait_for_n_requests(5);
    wait_for_topic(tp_ns);
}

FIXTURE_TEST(recovery_with_retention_ms_override, fixture) {
    set_expectations_and_listen(
      {root_level, meta_level, manifest, recovery_state});

    ss::http::request r;
    r._headers = {{"Content-Type", "application/json"}};
    r.content_length = 1;
    r.content
      = R"JSON({"topic_names_pattern": ".*es*", "retention_ms": 10000})JSON";
    auto& service = app.topic_recovery_service;
    service.local().start_recovery(r);

    wait_for_n_requests(5);
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
    set_expectations_and_listen(
      {root_level, meta_level, manifest, recovery_state});

    ss::http::request r;
    r._headers = {{"Content-Type", "application/json"}};
    r.content_length = 1;
    r.content
      = R"JSON({"topic_names_pattern": ".*es*", "retention_bytes": 10000})JSON";
    auto& service = app.topic_recovery_service;
    service.local().start_recovery(r);

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
    set_expectations_and_listen(
      {root_level, meta_level, manifest, recovery_state});

    ss::http::request r;
    r._headers = {{"Content-Type", "application/json"}};
    r.content_length = 1;
    r.content
      = R"JSON({"topic_names_pattern": ".*es*", "retention_bytes": 10000})JSON";
    auto& service = app.topic_recovery_service;
    service.local().start_recovery(r);

    // capture a status where the recovery is running. if the recovery finishes
    // too fast the test may miss it.
    cloud_storage::topic_recovery_service::recovery_status status;
    tests::cooperative_spin_wait_with_timeout(60s, [&service, &status] {
        status = service.local().current_recovery_status();
        return service.local().current_recovery_status().request.has_value();
    }).get();

    BOOST_REQUIRE_NE(
      status.state, cloud_storage::topic_recovery_service::state::inactive);

    cloud_storage::recovery_request rr{r};
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
