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
#include "cloud_roles/types.h"
#include "cloud_storage/configuration.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "cloud_storage_clients/configuration.h"
#include "cluster/archival/types.h"
#include "cluster/cluster_utils.h"
#include "cluster/controller.h"
#include "cluster/controller_stm.h"
#include "cluster/errc.h"
#include "cluster/members_table.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "config/broker_authn_endpoint.h"
#include "config/mock_property.h"
#include "config/node_config.h"
#include "config/types.h"
#include "kafka/client/transport.h"
#include "kafka/protocol/fetch.h"
#include "kafka/protocol/schemata/fetch_request.h"
#include "kafka/protocol/types.h"
#include "kafka/server/connection_context.h"
#include "kafka/server/handlers/fetch.h"
#include "kafka/server/handlers/topics/topic_utils.h"
#include "kafka/server/server.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/timeout_clock.h"
#include "net/dns.h"
#include "pandaproxy/rest/configuration.h"
#include "pandaproxy/schema_registry/configuration.h"
#include "redpanda/application.h"
#include "resource_mgmt/cpu_scheduling.h"
#include "security/acl.h"
#include "security/sasl_authentication.h"
#include "ssx/thread_worker.h"
#include "storage/directories.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"
#include "test_utils/logs.h"
#include "utils/unresolved_address.h"

#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/util/log.hh>

#include <absl/container/flat_hash_set.h>
#include <fmt/format.h>

#include <chrono>
#include <cstddef>
#include <filesystem>
#include <stdexcept>
#include <unordered_set>
#include <vector>

// Whether or not the fixtures should be configured with a node ID.
// NOTE: several fixtures may still require a node ID be supplied for the sake
// of differentiating ports, data directories, loggers, etc.
using configure_node_id = ss::bool_class<struct configure_node_id_tag>;
using empty_seed_starts_cluster
  = ss::bool_class<struct empty_seed_starts_cluster_tag>;

using namespace std::chrono_literals;

class redpanda_thread_fixture {
public:
    static constexpr const char* rack_name = "i-am-rack";

    redpanda_thread_fixture(
      model::node_id node_id,
      int32_t kafka_port,
      int32_t rpc_port,
      int32_t proxy_port,
      int32_t schema_reg_port,
      std::vector<config::seed_server> seed_servers,
      ss::sstring base_dir,
      std::optional<scheduling_groups> sch_groups,
      bool remove_on_shutdown,
      std::optional<cloud_storage_clients::s3_configuration> s3_config
      = std::nullopt,
      std::optional<archival::configuration> archival_cfg = std::nullopt,
      std::optional<cloud_storage::configuration> cloud_cfg = std::nullopt,
      configure_node_id use_node_id = configure_node_id::yes,
      const empty_seed_starts_cluster empty_seed_starts_cluster_val
      = empty_seed_starts_cluster::yes,
      std::optional<uint32_t> kafka_admin_topic_api_rate = std::nullopt,
      bool enable_data_transforms = false,
      bool enable_legacy_upload_mode = true)
      : app(ssx::sformat("redpanda-{}", node_id()))
      , proxy_port(proxy_port)
      , schema_reg_port(schema_reg_port)
      , kafka_port(kafka_port)
      , data_dir(std::move(base_dir))
      , remove_on_shutdown(remove_on_shutdown)
      , app_signal(std::make_unique<::stop_signal>()) {
        configure(
          node_id,
          kafka_port,
          rpc_port,
          std::move(seed_servers),
          std::move(s3_config),
          std::move(archival_cfg),
          std::move(cloud_cfg),
          use_node_id,
          empty_seed_starts_cluster_val,
          kafka_admin_topic_api_rate,
          enable_data_transforms,
          enable_legacy_upload_mode);
        app.initialize(
          proxy_config(proxy_port),
          proxy_client_config(kafka_port),
          schema_reg_config(schema_reg_port),
          proxy_client_config(kafka_port),
          audit_log_client_config(kafka_port),
          sch_groups);
        app.check_environment();
        app.wire_up_and_start(*app_signal, true);

        net::server_configuration scfg("fixture_config");
        scfg.max_service_memory_per_core = int64_t(
          memory_groups().rpc_total_memory());
        scfg.disable_metrics = net::metrics_disabled::yes;
        scfg.disable_public_metrics = net::public_metrics_disabled::yes;
        configs.start(scfg).get();

        // used by request context builder
        proto
          .start(
            &configs,
            app.smp_service_groups.kafka_smp_sg(),
            app.sched_groups.fetch_sg(),
            std::ref(app.metadata_cache),
            std::ref(app.controller->get_topics_frontend()),
            std::ref(app.controller->get_config_frontend()),
            std::ref(app.controller->get_feature_table()),
            std::ref(app.controller->get_quota_frontend()),
            std::ref(app.controller->get_quota_store()),
            std::ref(app.quota_mgr),
            std::ref(app.snc_quota_mgr),
            std::ref(app.group_router),
            std::ref(app.usage_manager),
            std::ref(app.shard_table),
            std::ref(app.partition_manager),
            std::ref(app.id_allocator_frontend),
            std::ref(app.controller->get_credential_store()),
            std::ref(app.controller->get_authorizer()),
            std::ref(app.audit_mgr),
            std::ref(app.controller->get_oidc_service()),
            std::ref(app.controller->get_security_frontend()),
            std::ref(app.controller->get_api()),
            std::ref(app.tx_gateway_frontend),
            std::nullopt,
            std::ref(*app.thread_worker),
            std::ref(app.schema_registry()))
          .get();

        configs.stop().get();
    }

    // creates single node with default configuration
    redpanda_thread_fixture()
      : redpanda_thread_fixture(
          model::node_id(1),
          9092,
          33145,
          8082,
          8081,
          {},
          ssx::sformat("test.dir_{}", time(0)),
          std::nullopt,
          true) {}

    // Restart the fixture with an existing data directory
    explicit redpanda_thread_fixture(std::filesystem::path existing_data_dir)
      : redpanda_thread_fixture(
          model::node_id(1),
          9092,
          33145,
          8082,
          8081,
          {},
          existing_data_dir.string(),
          std::nullopt,
          true) {}

    struct init_cloud_storage_tag {};

    // Start redpanda with shadow indexing enabled
    explicit redpanda_thread_fixture(
      init_cloud_storage_tag,
      std::optional<uint16_t> port = std::nullopt,
      cloud_storage_clients::s3_url_style url_style = default_url_style,
      model::node_id node_id = model::node_id(1))
      : redpanda_thread_fixture(
          node_id,
          9092,
          33145,
          8082,
          8081,
          {},
          ssx::sformat("test.dir_{}", time(0)),
          std::nullopt,
          true,
          get_s3_config(port, url_style),
          get_archival_config(),
          get_cloud_config(port, url_style)) {}

    struct init_cloud_storage_no_archiver_tag {};

    // Start redpanda with shadow indexing enabled, but do not enable
    // tiered storage by default: this enables constructing topics without
    // the upload code, to later set it up manually in a test.
    explicit redpanda_thread_fixture(
      init_cloud_storage_no_archiver_tag,
      std::optional<uint16_t> port = std::nullopt,
      cloud_storage_clients::s3_url_style url_style = default_url_style)
      : redpanda_thread_fixture(
          model::node_id(1),
          9092,
          33145,
          8082,
          8081,
          {},
          ssx::sformat("test.dir_{}", time(0)),
          std::nullopt,
          true,
          get_s3_config(port, url_style),
          get_archival_config(),
          std::nullopt) {}

    ~redpanda_thread_fixture() {
        shutdown();
        proto.stop().get();
        if (remove_on_shutdown) {
            std::filesystem::remove_all(data_dir);
        }
    }

    void shutdown() {
        if (!app_signal->abort_source().abort_requested()) {
            app_signal->abort_source().request_abort();
        }
        app.shutdown();
    }

    using should_wipe = ss::bool_class<struct should_wipe_tag>;
    void restart(should_wipe w = should_wipe::yes) {
        shutdown();
        if (w == should_wipe::yes) {
            std::filesystem::remove_all(data_dir);
        }
        app_signal = std::make_unique<::stop_signal>();
        ss::smp::invoke_on_all([] {
            auto& config = config::shard_local_cfg();
            config.get("disable_metrics").set_value(false);
        }).get();
        app.initialize(proxy_config(), proxy_client_config());
        app.check_environment();
        app.wire_up_and_start(*app_signal, true);
    }

    config::configuration& lconf() { return config::shard_local_cfg(); }

    static cloud_storage_clients::s3_configuration get_s3_config(
      std::optional<uint16_t> port = std::nullopt,
      cloud_storage_clients::s3_url_style url_style = default_url_style) {
        net::unresolved_address server_addr("localhost", port.value_or(4430));
        cloud_storage_clients::s3_configuration s3conf;
        s3conf.uri = cloud_storage_clients::access_point_uri("localhost");
        s3conf.access_key = cloud_roles::public_key_str("access-key");
        s3conf.secret_key = cloud_roles::private_key_str("secret-key");
        s3conf.region = cloud_roles::aws_region_name("us-east-1");
        s3conf.url_style = url_style;
        s3conf.server_addr = server_addr;
        return s3conf;
    }

    static archival::configuration get_archival_config() {
        archival::configuration aconf{
          .cloud_storage_initial_backoff = config::mock_binding(100ms),
          .segment_upload_timeout = config::mock_binding(1000ms),
          .manifest_upload_timeout = config::mock_binding(1000ms),
          .garbage_collect_timeout = config::mock_binding(1000ms),
          .upload_loop_initial_backoff = config::mock_binding(100ms),
          .upload_loop_max_backoff = config::mock_binding(5000ms)};
        aconf.bucket_name = cloud_storage_clients::bucket_name("test-bucket");
        aconf.ntp_metrics_disabled = archival::per_ntp_metrics_disabled::yes;
        aconf.svc_metrics_disabled = archival::service_metrics_disabled::yes;
        aconf.time_limit = std::nullopt;
        return aconf;
    }

    static cloud_storage::configuration get_cloud_config(
      std::optional<uint16_t> port = std::nullopt,
      cloud_storage_clients::s3_url_style url_style = default_url_style) {
        auto s3conf = get_s3_config(port, url_style);
        cloud_storage::configuration cconf;
        cconf.client_config = s3conf;
        cconf.bucket_name = cloud_storage_clients::bucket_name("test-bucket");
        cconf.connection_limit = archival::connection_limit(4);
        return cconf;
    }

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
      std::optional<uint32_t> kafka_admin_topic_api_rate = std::nullopt,
      bool data_transforms_enabled = false,
      bool legacy_upload_mode_enabled = true) {
        auto base_path = std::filesystem::path(data_dir);
        ss::smp::invoke_on_all([=]() {
            auto& config = config::shard_local_cfg();

            config.get("enable_pid_file").set_value(false);
            config.get("join_retry_timeout_ms").set_value(100ms);
            config.get("members_backend_retry_ms").set_value(1000ms);
            config.get("disable_metrics").set_value(true);
            config.get("disable_public_metrics").set_value(true);

            auto& node_config = config::node();
            node_config.get("admin").set_value(
              std::vector<model::broker_endpoint>());
            node_config.get("developer_mode").set_value(true);
            node_config.get("node_id").set_value(
              use_node_id ? std::make_optional(node_id)
                          : std::optional<model::node_id>(std::nullopt));
            node_config.get("empty_seed_starts_cluster")
              .set_value(bool(empty_seed_starts_cluster_val));
            node_config.get("rack").set_value(
              std::make_optional(model::rack_id(rack_name)));
            node_config.get("seed_servers").set_value(seed_servers);
            node_config.get("rpc_server")
              .set_value(net::unresolved_address("127.0.0.1", rpc_port));
            node_config.get("kafka_api")
              .set_value(std::vector<config::broker_authn_endpoint>{
                config::broker_authn_endpoint{
                  .address = net::unresolved_address(
                    "127.0.0.1", kafka_port)}});
            node_config.get("data_directory")
              .set_value(config::data_directory_path{.path = base_path});
            if (s3_config) {
                config.get("cloud_storage_enabled").set_value(true);
                config.get("cloud_storage_region")
                  .set_value(std::make_optional(s3_config->region()));
                config.get("cloud_storage_access_key")
                  .set_value(std::make_optional((*s3_config->access_key)()));
                config.get("cloud_storage_secret_key")
                  .set_value(std::make_optional((*s3_config->secret_key)()));
                config.get("cloud_storage_api_endpoint")
                  .set_value(std::make_optional(s3_config->server_addr.host()));
                config.get("cloud_storage_url_style")
                  .set_value(std::make_optional([&] {
                      switch (s3_config->url_style) {
                      case cloud_storage_clients::s3_url_style::virtual_host:
                          return config::s3_url_style::virtual_host;
                      case cloud_storage_clients::s3_url_style::path:
                          return config::s3_url_style::path;
                      }
                  }()));
                config.get("cloud_storage_api_endpoint_port")
                  .set_value(
                    static_cast<int16_t>(s3_config->server_addr.port()));
            }
            if (archival_cfg) {
                // Copy archival config to this shard to avoid `config::binding`
                // asserting on cross-shard access.
                auto local_cfg = archival_cfg;

                config.get("cloud_storage_disable_tls").set_value(true);
                config.get("cloud_storage_bucket")
                  .set_value(std::make_optional(local_cfg->bucket_name()));
                config.get("cloud_storage_initial_backoff_ms")
                  .set_value(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                      local_cfg->cloud_storage_initial_backoff()));
                config.get("cloud_storage_manifest_upload_timeout_ms")
                  .set_value(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                      local_cfg->manifest_upload_timeout()));
                config.get("cloud_storage_segment_upload_timeout_ms")
                  .set_value(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                      local_cfg->segment_upload_timeout()));
                config.get("cloud_storage_garbage_collect_timeout_ms")
                  .set_value(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                      local_cfg->garbage_collect_timeout()));
            }
            if (cloud_cfg) {
                config.get("cloud_storage_enable_remote_read").set_value(true);
                config.get("cloud_storage_enable_remote_write").set_value(true);
                config.get("cloud_storage_max_connections")
                  .set_value(
                    static_cast<int16_t>(cloud_cfg->connection_limit()));
            }

            if (kafka_admin_topic_api_rate) {
                config.get("kafka_admin_topic_api_rate")
                  .set_value(kafka_admin_topic_api_rate);
            }
            config.get("data_transforms_enabled")
              .set_value(data_transforms_enabled);
            config.get("cloud_storage_disable_archiver_manager")
              .set_value(legacy_upload_mode_enabled);
        }).get();
    }

    YAML::Node proxy_config(uint16_t proxy_port = 8082) {
        pandaproxy::rest::configuration cfg;
        cfg.get("pandaproxy_api")
          .set_value(std::vector<config::rest_authn_endpoint>{
            config::rest_authn_endpoint{
              .address = net::unresolved_address("127.0.0.1", proxy_port)}});
        return to_yaml(cfg, config::redact_secrets::no);
    }

    YAML::Node proxy_client_config(
      uint16_t kafka_api_port = config::node().kafka_api()[0].address.port()) {
        kafka::client::configuration cfg;
        net::unresolved_address kafka_api{
          config::node().kafka_api()[0].address.host(), kafka_api_port};
        cfg.brokers.set_value(
          std::vector<net::unresolved_address>({kafka_api}));
        return to_yaml(cfg, config::redact_secrets::no);
    }

    YAML::Node schema_reg_config(uint16_t listen_port = 8081) {
        pandaproxy::schema_registry::configuration cfg;
        cfg.get("schema_registry_api")
          .set_value(std::vector<config::rest_authn_endpoint>{
            config::rest_authn_endpoint{
              .address = net::unresolved_address("127.0.0.1", listen_port)}});
        cfg.get("schema_registry_replication_factor")
          .set_value(std::make_optional<int16_t>(1));
        return to_yaml(cfg, config::redact_secrets::no);
    }

    YAML::Node audit_log_client_config(
      uint16_t kafka_api_port = config::node().kafka_api()[0].address.port()) {
        kafka::client::configuration cfg;
        net::unresolved_address kafka_api{
          config::node().kafka_api()[0].address.host(), kafka_api_port};
        cfg.brokers.set_value(
          std::vector<net::unresolved_address>({kafka_api}));
        return to_yaml(cfg, config::redact_secrets::no);
    }

    ss::future<> wait_for_controller_leadership() {
        auto tout = ss::lowres_clock::now() + std::chrono::seconds(10);
        auto id = co_await app.controller->get_partition_leaders()
                    .local()
                    .wait_for_leader(model::controller_ntp, tout, {});

        RPTEST_REQUIRE_EVENTUALLY_CORO(10s, [this, id] {
            auto& members = app.controller->get_members_table();
            return members.local().contains(id);
        });

        // Wait for feature manager to be initialized: this writes to
        // the raft0 log on first startup, so must be complete before
        // tests start (tests use raft0 offsets to guess at the revision
        // ids of partitions they create)
        RPTEST_REQUIRE_EVENTUALLY_CORO(10s, [this]() -> bool {
            // Await feature manager bootstrap
            auto& feature_table = app.controller->get_feature_table().local();
            if (
              feature_table.get_active_version() == cluster::invalid_version) {
                return false;
            }

            // Await config manager bootstrap
            auto& config_mgr = app.controller->get_config_manager().local();
            if (config_mgr.get_version() == cluster::config_version_unset) {
                return false;
            }

            // Await initial config status messages from all nodes
            auto& members = app.controller->get_members_table().local();
            return config_mgr.get_status().size() == members.node_count();
        });
    }

    // Wait for the Raft leader of the given partition to become leader.
    ss::future<> wait_for_leader(
      model::ntp ntp, model::timeout_clock::duration timeout = 3s) {
        RPTEST_REQUIRE_EVENTUALLY_CORO(timeout, [this, ntp = std::move(ntp)]() {
            auto shard = app.shard_table.local().shard_for(ntp);
            if (!shard) {
                return ss::make_ready_future<bool>(false);
            }
            return app.partition_manager.invoke_on(
              *shard, [ntp](cluster::partition_manager& mgr) {
                  auto partition = mgr.get(ntp);
                  return partition
                         && partition->raft()->term() != model::term_id{}
                         && partition->raft()->is_leader();
              });
        });
    }

    ss::future<kafka::client::transport>
    make_kafka_client(std::optional<ss::sstring> client_id = "test_client") {
        return ss::make_ready_future<kafka::client::transport>(
          net::base_transport::configuration{
            .server_addr = {"127.0.0.1", kafka_port},
          },
          std::move(client_id));
    }

    model::ntp
    make_default_ntp(model::topic topic, model::partition_id partition) {
        return model::ntp(model::kafka_namespace, std::move(topic), partition);
    }

    storage::log_config make_default_config() {
        return storage::log_config(
          data_dir.string(),
          1_GiB,
          ss::default_priority_class(),
          storage::make_sanitized_file_config());
    }

    ss::future<> wait_for_topics(std::vector<cluster::topic_result> results) {
        RPTEST_REQUIRE_EVENTUALLY_CORO(
          10s, [this, results = std::move(results)] {
              return std::all_of(
                results.begin(),
                results.end(),
                [this](const cluster::topic_result& r) {
                    auto md = app.metadata_cache.local().get_topic_metadata(
                      r.tp_ns);
                    return md
                           && std::all_of(
                             md->get_assignments().begin(),
                             md->get_assignments().end(),
                             [this, &r](
                               const cluster::assignments_set::value_type& p) {
                                 return app.shard_table.local().shard_for(
                                   model::ntp(
                                     r.tp_ns.ns, r.tp_ns.tp, p.second.id));
                             });
                });
          });
    }

    ss::future<> add_topic(
      model::topic_namespace_view tp_ns,
      int partitions = 1,
      std::optional<cluster::topic_properties> props = std::nullopt,
      int16_t replication_factor = 1,
      bool wait = true) {
        cluster::topic_configuration_vector cfgs = {
          cluster::topic_configuration{
            tp_ns.ns, tp_ns.tp, partitions, replication_factor}};
        if (props.has_value()) {
            cfgs[0].properties = std::move(props.value());
        }
        return app.controller->get_topics_frontend()
          .local()
          .create_topics(
            cluster::without_custom_assignments(std::move(cfgs)),
            model::no_timeout)
          .then([this, wait](std::vector<cluster::topic_result> results) {
              vassert(
                results.size() == 1,
                "expected exactly 1 result but got {}",
                results.size());
              const auto& result = results.at(0);
              if (result.ec != cluster::errc::success) {
                  throw std::runtime_error(fmt::format(
                    "error creating topic {}: {}",
                    result.tp_ns,
                    cluster::make_error_code(result.ec).message()));
              }

              if (wait) {
                  return wait_for_topics(std::move(results));
              }
              return ss::make_ready_future<>();
          });
    }

    ss::future<> delete_topic(model::topic_namespace tp_ns) {
        std::vector<model::topic_namespace> topics{std::move(tp_ns)};
        return app.controller->get_topics_frontend()
          .local()
          .delete_topics(std::move(topics), model::no_timeout)
          .then([this](std::vector<cluster::topic_result> results) {
              return tests::cooperative_spin_wait_with_timeout(
                2s, [this, results = std::move(results)] {
                    return std::all_of(
                      results.begin(),
                      results.end(),
                      [this](const cluster::topic_result& r) {
                          return !app.metadata_cache.local().get_topic_metadata(
                            r.tp_ns);
                      });
                });
          });
    }

    ss::future<> wait_for_partition_offset(
      model::ntp ntp,
      model::offset o,
      model::timeout_clock::duration tout = 3s) {
        RPTEST_REQUIRE_EVENTUALLY_CORO(
          tout, [this, ntp = std::move(ntp), o]() mutable {
              auto shard = app.shard_table.local().shard_for(ntp);
              if (!shard) {
                  return ss::make_ready_future<bool>(false);
              }
              return app.partition_manager.invoke_on(
                *shard, [ntp, o](cluster::partition_manager& mgr) {
                    auto partition = mgr.get(ntp);
                    return partition && partition->committed_offset() >= o;
                });
          });
    }

    ss::future<model::offset> wait_for_quiescent_controller_committed_offset(
      std::chrono::milliseconds quiescent_time = 2s,
      std::chrono::milliseconds timeout = 10s) {
        model::offset last_committed_offset;
        auto last_update_ts = model::timeout_clock::now();
        const auto start_ts = last_update_ts;
        while (model::timeout_clock::now() - last_update_ts < quiescent_time) {
            auto current_committed_offset
              = co_await app.partition_manager.invoke_on(
                cluster::controller_stm_shard,
                [](cluster::partition_manager& p_mgr) {
                    auto ctrl_p = p_mgr.get(model::controller_ntp);
                    vassert(ctrl_p, "Controller partition must exists");
                    return ctrl_p->linearizable_barrier().then(
                      [ctrl_p](result<model::offset> o) {
                          return ctrl_p->committed_offset();
                      });
                });
            auto now = model::timeout_clock::now();
            if (now > start_ts + timeout) {
                throw std::runtime_error(
                  "Timeout waiting for controller to reach quiestent state");
            }

            if (current_committed_offset != last_committed_offset) {
                last_update_ts = model::timeout_clock::now();
            }

            last_committed_offset = current_committed_offset;
        }

        co_return last_committed_offset;
    }

    /**
     * Predict the revision ID of the next partition to be created.  Useful
     * if you want to pre-populate data directory.
     */
    ss::future<model::revision_id> get_next_partition_revision_id() {
        auto ntp = model::controller_ntp;
        auto shard = app.shard_table.local().shard_for(ntp);
        assert(shard);
        // flush any in flight changes for a consistent committed_offset.
        co_await wait_for_quiescent_controller_committed_offset();

        co_return co_await app.partition_manager.invoke_on(
          *shard, [ntp](cluster::partition_manager& mgr) -> model::revision_id {
              auto partition = mgr.get(ntp);
              assert(partition);
              return model::revision_id{
                model::next_offset(partition->committed_offset())};
          });
    }

    model::ktp
    make_data(std::optional<model::timestamp> base_ts = std::nullopt) {
        auto topic_name = ssx::sformat("my_topic_{}", 0);
        model::ktp ktp(model::topic(topic_name), model::partition_id(0));

        const auto& topic_table = app.controller->get_topics_state().local();
        model::topic_namespace tp_ns{model::kafka_namespace, ktp.get_topic()};
        while (true) {
            if (topic_table.contains(ktp.as_tn_view())) {
                delete_topic(tp_ns).get();
            }

            // Here we first request a potential revision from the controller
            // and create an ntp structure on the disk. The expectation is that
            // when the topic is eventually created, the segments are recovered
            // at bootstrap and we have the exact data in the structure we
            // populated.

            // This is inherently racy because there could be other controller
            // updates between the time we request a revision and create a
            // table. That invalidates the revision as the topic is created with
            // a newer revision and the desired segements are not recovered from
            // disk. This loop mitigates this problem by deleting and recreating
            // the topic if there is a revision mismatch.

            // Get a potential revision for ntp.
            auto rev = get_next_partition_revision_id().get();
            storage::ntp_config ntp_cfg(
              ktp.to_ntp(),
              config::node().data_directory().as_sstring(),
              nullptr,
              rev);

            storage::disk_log_builder builder(make_default_config());
            using namespace storage; // NOLINT

            builder | start(std::move(ntp_cfg)) | add_segment(model::offset(0))
              | add_random_batches(
                model::offset(0),
                20,
                maybe_compress_batches::yes,
                log_append_config{
                  .should_fsync = log_append_config::fsync::yes,
                  .io_priority = ss::default_priority_class(),
                  .timeout = model::no_timeout},
                disk_log_builder::should_flush_after::yes,
                base_ts)
              | stop();

            add_topic(ktp.as_tn_view()).get();
            // validate if version matches
            const auto& topic_meta = topic_table.get_topic_metadata(
              ktp.as_tn_view());
            assert(topic_meta);
            // Check if the topic revision matches the desired revision, if not
            // delete and recreate the topic.
            if (topic_meta->get_revision() == rev) {
                break;
            }
        }

        return ktp;
    }

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
            vassert(false, "Don't call this");
        }
        const security::audit::user& audit_user() const override {
            static const security::audit::user user{
              .type_id = security::audit::user::type::unknown};
            return user;
        }
        const char* mechanism_name() const override { return "fake-mechanism"; }
    };

    conn_ptr make_connection_context() {
        security::sasl_server sasl(security::sasl_server::sasl_state::complete);
        sasl.set_mechanism(std::make_unique<fake_sasl_mech>());
        return ss::make_lw_shared<kafka::connection_context>(
          std::nullopt,
          proto.local(),
          nullptr,
          std::move(sasl),
          false,
          std::nullopt,
          config::mock_property<uint32_t>(100_MiB).bind(),
          config::mock_property<std::vector<ss::sstring>>({"produce", "fetch"})
            .bind<std::vector<bool>>(
              &kafka::server::convert_api_names_to_key_bitmap));
    }

    template<typename RequestType>
    requires requires(
      RequestType r,
      kafka::protocol::encoder& writer,
      kafka::api_version version) {
        { r.encode(writer, version) } -> std::same_as<void>;
    }
    kafka::request_context make_request_context(
      RequestType request, kafka::request_header& header, conn_ptr conn = {}) {
        if (!conn) {
            conn = make_connection_context();
        }

        iobuf buf;
        kafka::protocol::encoder writer(buf);
        request.encode(writer, header.version);

        return kafka::request_context(
          conn,
          std::move(header),
          std::move(buf),
          std::chrono::milliseconds(0));
    }

    kafka::request_context make_fetch_request_context() {
        kafka::fetch_request request;
        // do not use incremental fetch requests
        request.data.max_wait_ms = std::chrono::milliseconds::zero();

        kafka::request_header header{
          .key = kafka::fetch_handler::api::key,
          .version = kafka::fetch_handler::max_supported};

        return make_request_context(std::move(request), header);
    }

    iobuf rand_iobuf(size_t data_size) const {
        iobuf b;
        auto data = random_generators::gen_alphanum_string(data_size);
        b.append(data.data(), data.size());
        return b;
    }

    application app;
    uint16_t proxy_port;
    uint16_t schema_reg_port;
    uint16_t kafka_port;
    std::filesystem::path data_dir;
    ss::sharded<net::server_configuration> configs;
    ss::sharded<kafka::server> proto;
    bool remove_on_shutdown;
    std::unique_ptr<::stop_signal> app_signal;
};
