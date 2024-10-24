/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_storage/remote.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "cloud_storage/topic_mount_handler.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/client_pool.h"
#include "cloud_storage_clients/types.h"
#include "cluster/topic_configuration.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "test_utils/test.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>

#include <chrono>

using namespace cloud_storage;
using namespace std::chrono_literals;

namespace {
static const ss::sstring test_uuid_str = "deadbeef-0000-0000-0000-000000000000";
static const ss::sstring default_uuid_str = ss::sstring{
  model::default_cluster_uuid()};
static const model::cluster_uuid test_uuid{uuid_t::from_string(test_uuid_str)};
static const remote_label test_label{test_uuid};
static const model::topic_namespace test_tp_ns{
  model::ns{"kafka"}, model::topic{"tp"}};
static const model::topic_namespace test_tp_ns_override{
  model::ns{"kafka"}, model::topic{"override"}};
static ss::abort_source never_abort;
static constexpr model::cloud_credentials_source config_file{
  model::cloud_credentials_source::config_file};
static const model::initial_revision_id rev_id{123};

static cluster::topic_configuration
get_topic_configuration(cluster::topic_properties topic_props) {
    auto topic_cfg = cluster::topic_configuration(
      test_tp_ns.ns, test_tp_ns.tp, 1, 1);
    topic_cfg.properties = std::move(topic_props);
    return topic_cfg;
}

} // namespace

struct TopicMountHandlerFixture
  : public s3_imposter_fixture
  , public testing::TestWithParam<std::tuple<bool, bool>> {
    TopicMountHandlerFixture() {
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

    ~TopicMountHandlerFixture() {
        pool.local().shutdown_connections();
        remote.stop().get();
        io.stop().get();
        pool.stop().get();
    }

    ss::sharded<cloud_storage_clients::client_pool> pool;
    ss::sharded<cloud_io::remote> io;
    ss::sharded<remote> remote;
};

TEST_P(TopicMountHandlerFixture, TestMountTopicManifestDoesNotExist) {
    set_expectations_and_listen({});

    auto topic_props = cluster::topic_properties{};

    auto tp_ns_override_param = std::get<0>(GetParam());
    auto remote_label_param = std::get<1>(GetParam());
    if (tp_ns_override_param) {
        topic_props.remote_topic_namespace_override = test_tp_ns_override;
    }
    if (remote_label_param) {
        topic_props.remote_label = test_label;
    }

    auto topic_cfg = get_topic_configuration(std::move(topic_props));
    auto handler = topic_mount_handler(bucket_name, remote.local());

    retry_chain_node rtc(never_abort, 10s, 20ms);
    auto prepare_result
      = handler.prepare_mount_topic(topic_cfg, rev_id, rtc).get();
    ASSERT_EQ(
      prepare_result, topic_mount_result::mount_manifest_does_not_exist);
    auto confirm_result
      = handler.confirm_mount_topic(topic_cfg, rev_id, rtc).get();
    ASSERT_EQ(
      confirm_result, topic_mount_result::mount_manifest_does_not_exist);
}

TEST_P(TopicMountHandlerFixture, TestMountTopicManifestNotDeleted) {
    set_expectations_and_listen({});
    retry_chain_node rtc(never_abort, 10s, 20ms);

    auto tp_ns_override_param = std::get<0>(GetParam());
    auto remote_label_param = std::get<1>(GetParam());

    const auto expected_tp_ns = tp_ns_override_param
                                  ? test_tp_ns_override.path()
                                  : test_tp_ns.path();
    const auto expected_label = remote_label_param ? test_uuid_str
                                                   : default_uuid_str;
    const auto expected_rev_id = rev_id;
    const auto path = cloud_storage_clients::object_key{fmt::format(
      "migration/{}/{}/{}", expected_label, expected_tp_ns, expected_rev_id)};
    {
        auto result
          = remote.local()
              .upload_object(
                {.transfer_details
                 = {.bucket = bucket_name, .key = path, .parent_rtc = rtc},
                 .payload = iobuf{}})
              .get();
        ASSERT_EQ(cloud_storage::upload_result::success, result);
    }

    auto topic_props = cluster::topic_properties{};
    if (remote_label_param) {
        topic_props.remote_label = test_label;
    }
    if (tp_ns_override_param) {
        topic_props.remote_topic_namespace_override = test_tp_ns_override;
    }
    auto topic_cfg = get_topic_configuration(std::move(topic_props));

    static const ss::sstring delete_error = R"json(
<Error>
    <Code>TestFailure</Code>
    <Key>0</Key>
    <Message>No Deletes Allowed.</Message>
</Error>)json";

    http_test_utils::response fail_response{
      .body = delete_error,
      .status = http_test_utils::response::status_type::bad_request};
    req_pred_t fail_delete_request =
      [](const http_test_utils::request_info& info) {
          return info.method.contains("DELETE");
      };
    fail_request_if(fail_delete_request, fail_response);

    auto handler = topic_mount_handler(bucket_name, remote.local());

    auto prepare_result
      = handler.prepare_mount_topic(topic_cfg, rev_id, rtc).get();
    ASSERT_EQ(prepare_result, topic_mount_result::mount_manifest_exists);
    auto confirm_result
      = handler.confirm_mount_topic(topic_cfg, rev_id, rtc).get();
    ASSERT_EQ(confirm_result, topic_mount_result::mount_manifest_not_deleted);

    const auto exists_result
      = remote.local()
          .object_exists(bucket_name, path, rtc, existence_check_type::manifest)
          .get();
    ASSERT_EQ(exists_result, download_result::success);
}

TEST_P(TopicMountHandlerFixture, TestMountTopicSuccess) {
    set_expectations_and_listen({});
    retry_chain_node rtc(never_abort, 10s, 20ms);

    auto tp_ns_override_param = std::get<0>(GetParam());
    auto remote_label_param = std::get<1>(GetParam());

    const auto expected_tp_ns = tp_ns_override_param
                                  ? test_tp_ns_override.path()
                                  : test_tp_ns.path();
    const auto expected_label = remote_label_param ? test_uuid_str
                                                   : default_uuid_str;
    const auto expected_rev_id = rev_id;
    const auto path = cloud_storage_clients::object_key{fmt::format(
      "migration/{}/{}/{}", expected_label, expected_tp_ns, expected_rev_id)};
    {
        auto result
          = remote.local()
              .upload_object(
                {.transfer_details
                 = {.bucket = bucket_name, .key = path, .parent_rtc = rtc},
                 .payload = iobuf{}})
              .get();
        ASSERT_EQ(cloud_storage::upload_result::success, result);
    }

    auto topic_props = cluster::topic_properties{};
    if (tp_ns_override_param) {
        topic_props.remote_topic_namespace_override = test_tp_ns_override;
    }
    if (remote_label_param) {
        topic_props.remote_label = test_label;
    }
    auto topic_cfg = get_topic_configuration(std::move(topic_props));

    auto handler = topic_mount_handler(bucket_name, remote.local());

    auto prepare_result
      = handler.prepare_mount_topic(topic_cfg, rev_id, rtc).get();
    ASSERT_EQ(prepare_result, topic_mount_result::mount_manifest_exists);
    auto confirm_result
      = handler.confirm_mount_topic(topic_cfg, rev_id, rtc).get();
    ASSERT_EQ(confirm_result, topic_mount_result::success);

    const auto exists_result
      = remote.local()
          .object_exists(bucket_name, path, rtc, existence_check_type::manifest)
          .get();
    ASSERT_EQ(exists_result, download_result::notfound);
}

TEST_P(TopicMountHandlerFixture, TestUnmountTopicManifestNotCreated) {
    set_expectations_and_listen({});
    retry_chain_node rtc(never_abort, 10s, 20ms);

    auto tp_ns_override_param = std::get<0>(GetParam());
    auto remote_label_param = std::get<1>(GetParam());

    const auto expected_tp_ns = tp_ns_override_param
                                  ? test_tp_ns_override.path()
                                  : test_tp_ns.path();
    const auto expected_label = remote_label_param ? test_uuid_str
                                                   : default_uuid_str;
    const auto expected_rev_id = rev_id;
    const auto path = cloud_storage_clients::object_key{fmt::format(
      "migration/{}/{}/{}", expected_label, expected_tp_ns, expected_rev_id)};

    auto topic_props = cluster::topic_properties{};
    if (tp_ns_override_param) {
        topic_props.remote_topic_namespace_override = test_tp_ns_override;
    }
    if (remote_label_param) {
        topic_props.remote_label = test_label;
    }
    auto topic_cfg = get_topic_configuration(std::move(topic_props));

    static const ss::sstring upload_error = R"json(
<Error>
    <Code>TestFailure</Code>
    <Key>0</Key>
    <Message>No Uploads Allowed.</Message>
</Error>)json";

    http_test_utils::response fail_response{
      .body = upload_error,
      .status = http_test_utils::response::status_type::bad_request};
    req_pred_t fail_delete_request =
      [](const http_test_utils::request_info& info) {
          return info.method.contains("PUT");
      };
    fail_request_if(fail_delete_request, fail_response);

    auto handler = topic_mount_handler(bucket_name, remote.local());

    auto unmount_result = handler.unmount_topic(topic_cfg, rev_id, rtc).get();
    ASSERT_EQ(unmount_result, topic_unmount_result::mount_manifest_not_created);

    const auto exists_result
      = remote.local()
          .object_exists(bucket_name, path, rtc, existence_check_type::manifest)
          .get();
    ASSERT_EQ(exists_result, download_result::notfound);
}

TEST_P(TopicMountHandlerFixture, TestUnmountTopicSuccess) {
    set_expectations_and_listen({});
    retry_chain_node rtc(never_abort, 10s, 20ms);

    auto tp_ns_override_param = std::get<0>(GetParam());
    auto remote_label_param = std::get<1>(GetParam());

    const auto expected_tp_ns = tp_ns_override_param
                                  ? test_tp_ns_override.path()
                                  : test_tp_ns.path();
    const auto expected_label = remote_label_param ? test_uuid_str
                                                   : default_uuid_str;
    const auto expected_rev_id = rev_id;
    const auto path = cloud_storage_clients::object_key{fmt::format(
      "migration/{}/{}/{}", expected_label, expected_tp_ns, expected_rev_id)};

    auto topic_props = cluster::topic_properties{};
    if (tp_ns_override_param) {
        topic_props.remote_topic_namespace_override = test_tp_ns_override;
    }
    if (remote_label_param) {
        topic_props.remote_label = test_label;
    }
    auto topic_cfg = get_topic_configuration(std::move(topic_props));

    auto handler = topic_mount_handler(bucket_name, remote.local());

    auto unmount_result = handler.unmount_topic(topic_cfg, rev_id, rtc).get();
    ASSERT_EQ(unmount_result, topic_unmount_result::success);

    const auto exists_result
      = remote.local()
          .object_exists(bucket_name, path, rtc, existence_check_type::manifest)
          .get();
    ASSERT_EQ(exists_result, download_result::success);
}

INSTANTIATE_TEST_SUITE_P(
  TopicMountHandlerOverride,
  TopicMountHandlerFixture,
  testing::Combine(testing::Bool(), testing::Bool()));
