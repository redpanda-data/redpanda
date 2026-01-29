/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "cloud_io/tests/s3_imposter.h"
#include "cloud_storage/configuration.h"
#include "cloud_storage_clients/configuration.h"
#include "cluster/archival/types.h"
#include "cluster/cluster_utils.h"
#include "cluster/controller.h"      // IWYU pragma: keep; public member devex
#include "cluster/topics_frontend.h" // IWYU pragma: keep; public member devex
#include "cluster/types.h"
#include "config/node_config.h"
#include "kafka/client/transport.h"
#include "kafka/protocol/schemata/fetch_request.h"
#include "kafka/protocol/types.h"
#include "kafka/server/connection_context.h"
#include "kafka/server/request_context.h"
#include "kafka/server/server.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "redpanda/application.h"
#include "resource_mgmt/cpu_scheduling.h"
#include "security/acl.h"
#include "security/sasl_authentication.h"
#include "security/scram_algorithm.h"
#include "security/scram_authenticator.h"
#include "test_utils/async.h" // IWYU pragma: export
#include "utils/unresolved_address.h"

#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/util/log.hh>

#include <fmt/format.h>

#include <chrono>
#include <cstddef>
#include <filesystem>
#include <vector>

// Whether or not the fixtures should be configured with a node ID.
// NOTE: several fixtures may still require a node ID be supplied for the sake
// of differentiating ports, data directories, loggers, etc.
using configure_node_id = ss::bool_class<struct configure_node_id_tag>;
using empty_seed_starts_cluster
  = ss::bool_class<struct empty_seed_starts_cluster_tag>;

using namespace std::chrono_literals;

ss::sstring test_directory();

class redpanda_thread_fixture {
public:
    static constexpr const char* rack_name = "i-am-rack";

    redpanda_thread_fixture(
      model::node_id node_id,
      int32_t kafka_port,
      int32_t rpc_port,
      std::optional<int32_t> proxy_port,
      std::optional<int32_t> schema_reg_port,
      std::vector<config::seed_server> seed_servers,
      ss::sstring base_dir,
      bool remove_on_shutdown,
      std::optional<cloud_storage_clients::s3_configuration> s3_config
      = std::nullopt,
      std::optional<archival::configuration> archival_cfg = std::nullopt,
      std::optional<cloud_storage::configuration> cloud_cfg = std::nullopt,
      configure_node_id use_node_id = configure_node_id::yes,
      const empty_seed_starts_cluster empty_seed_starts_cluster_val
      = empty_seed_starts_cluster::yes,
      bool enable_data_transforms = false,
      bool enable_legacy_upload_mode = true,
      bool iceberg_enabled = false,
      bool enable_cloud_topics = false,
      bool development_cluster_linking_enabled = false,
      bool use_lsm_metastore = false);

    // creates single node with default configuration
    redpanda_thread_fixture();

    // Restart the fixture with an existing data directory
    explicit redpanda_thread_fixture(std::filesystem::path existing_data_dir);

    struct init_cloud_storage_tag {};

    // Start redpanda with shadow indexing enabled
    explicit redpanda_thread_fixture(
      init_cloud_storage_tag,
      std::optional<uint16_t> port = std::nullopt,
      cloud_storage_clients::s3_url_style url_style = default_url_style,
      model::node_id node_id = model::node_id(1));

    struct init_cloud_topics_tag {};

    // Start redpanda with shadow indexing enabled
    explicit redpanda_thread_fixture(
      init_cloud_topics_tag,
      std::optional<uint16_t> port = std::nullopt,
      cloud_storage_clients::s3_url_style url_style = default_url_style,
      model::node_id node_id = model::node_id(1));

    struct init_cloud_storage_no_archiver_tag {};

    // Start redpanda with shadow indexing enabled, but do not enable
    // tiered storage by default: this enables constructing topics without
    // the upload code, to later set it up manually in a test.
    explicit redpanda_thread_fixture(
      init_cloud_storage_no_archiver_tag,
      std::optional<uint16_t> port = std::nullopt,
      cloud_storage_clients::s3_url_style url_style = default_url_style);

    ~redpanda_thread_fixture();

    void shutdown();

    using should_wipe = ss::bool_class<struct should_wipe_tag>;
    void restart(should_wipe w = should_wipe::yes);

    config::configuration& lconf();

    static cloud_storage_clients::s3_configuration get_s3_config(
      std::optional<uint16_t> port = std::nullopt,
      cloud_storage_clients::s3_url_style url_style = default_url_style);

    static archival::configuration get_archival_config();

    static cloud_storage::configuration get_cloud_config(
      std::optional<uint16_t> port = std::nullopt,
      cloud_storage_clients::s3_url_style url_style = default_url_style);

    void configure(
      model::node_id node_id,
      int32_t kafka_port,
      int32_t rpc_port,
      std::vector<config::seed_server> seed_servers,
      std::optional<cloud_storage_clients::s3_configuration> s3_config
      = std::nullopt,
      std::optional<archival::configuration> archival_cfg = std::nullopt,
      std::optional<cloud_storage::configuration> cloud_cfg = std::nullopt,
      configure_node_id use_node_id = configure_node_id::yes,
      const empty_seed_starts_cluster empty_seed_starts_cluster_val
      = empty_seed_starts_cluster::yes,
      bool data_transforms_enabled = false,
      bool legacy_upload_mode_enabled = true,
      bool iceberg_enabled = false,
      bool enable_cloud_topics = false,
      bool development_cluster_linking_enabled = false);

    YAML::Node proxy_config(uint16_t proxy_port = 8082);

    YAML::Node proxy_client_config(
      uint16_t kafka_api_port = config::node().kafka_api()[0].address.port());

    YAML::Node schema_reg_config(uint16_t listen_port = 8081);

    YAML::Node audit_log_client_config(
      uint16_t kafka_api_port = config::node().kafka_api()[0].address.port());

    ss::future<> wait_for_controller_leadership();

    // Wait for the Raft leader of the given partition to become leader.
    ss::future<> wait_for_leader(
      model::ntp ntp, model::timeout_clock::duration timeout = 3s);

    ss::future<kafka::client::transport>
    make_kafka_client(std::optional<ss::sstring> client_id = "test_client");

    model::ntp
    make_default_ntp(model::topic topic, model::partition_id partition);

    storage::log_config make_default_config();

    ss::future<> wait_for_topics(std::vector<cluster::topic_result> results);

    ss::future<> add_topic(
      model::topic_namespace_view tp_ns,
      int partitions = 1,
      std::optional<cluster::topic_properties> props = std::nullopt,
      int16_t replication_factor = 1,
      bool wait = true);

    ss::future<> delete_topic(model::topic_namespace tp_ns);

    ss::future<> wait_for_partition_offset(
      model::ntp ntp,
      model::offset o,
      model::timeout_clock::duration tout = 3s);

    ss::future<model::offset> wait_for_quiescent_controller_committed_offset(
      std::chrono::milliseconds quiescent_time = 2s,
      std::chrono::milliseconds timeout = 10s);

    /**
     * Predict the revision ID of the next partition to be created.  Useful
     * if you want to pre-populate data directory.
     */
    ss::future<model::revision_id> get_next_partition_revision_id();

    model::ktp
    make_data(std::optional<model::timestamp> base_ts = std::nullopt);

    using conn_ptr = ss::lw_shared_ptr<kafka::connection_context>;

    struct fake_sasl_mech : public security::sasl_mechanism {
        bool complete() const final { return true; }
        bool failed() const final { return false; }
        const security::acl_principal& principal() const final {
            static const security::acl_principal fake_principal{
              security::principal_type::user, "fake-user"};
            return fake_principal;
        }
        ss::future<result<bytes>> authenticate(bytes) final {
            vunreachable("Don't call this");
        }
        const security::audit::user& audit_user() const override {
            static const security::audit::user user{
              .type_id = security::audit::user::type::unknown};
            return user;
        }
        const char* mechanism_name() const override { return "fake-mechanism"; }
    };

    conn_ptr make_connection_context(bool use_authz = false);

    using encoder_func
      = ss::noncopyable_function<void(kafka::protocol::encoder&)>;

    kafka::request_context make_request_context_erased(
      encoder_func&& encoder, kafka::request_header& header, conn_ptr conn);

    template<typename RequestType>
    requires requires(
      RequestType r,
      kafka::protocol::encoder& writer,
      kafka::api_version version) {
        { r.encode(writer, version) } -> std::same_as<void>;
    }
    kafka::request_context make_request_context(
      RequestType request, kafka::request_header& header, conn_ptr conn = {}) {
        return make_request_context_erased(
          [&](kafka::protocol::encoder& e) mutable {
              request.encode(e, header.version);
          },
          header,
          conn);
    }

    kafka::request_context make_fetch_request_context();

    iobuf rand_iobuf(size_t data_size) const;

    void enable_sasl();

    void disable_sasl();

    security::server_first_message send_scram_client_first(
      kafka::client::transport& client,
      const security::client_first_message& client_first);

    security::server_final_message send_scram_client_final(
      kafka::client::transport& client,
      const security::client_final_message& client_final);

    template<typename Authenticator = security::scram_sha256_authenticator>
    void authn_kafka_client(
      kafka::client::transport& client,
      const ss::sstring& username,
      const ss::sstring& password);

    application app;
    std::optional<uint16_t> proxy_port;
    std::optional<uint16_t> schema_reg_port;
    uint16_t kafka_port;
    std::filesystem::path data_dir;
    ss::sharded<net::server_configuration> configs;
    ss::sharded<kafka::server> proto;
    bool remove_on_shutdown;
    std::unique_ptr<::stop_signal> app_signal;
    bool use_lsm_metastore{true};
};
