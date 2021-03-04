/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "cluster/metadata_cache.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "kafka/client/transport.h"
#include "kafka/server/handlers/topics/topic_utils.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/timeout_clock.h"
#include "redpanda/application.h"
#include "resource_mgmt/cpu_scheduling.h"
#include "rpc/dns.h"
#include "storage/directories.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "storage/tests/utils/random_batch.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"
#include "test_utils/logs.h"

#include <seastar/util/log.hh>

#include <fmt/format.h>

#include <filesystem>

class redpanda_thread_fixture {
public:
    static constexpr const char* rack_name = "i-am-rack";

    redpanda_thread_fixture(
      model::node_id node_id,
      int32_t kafka_port,
      int32_t rpc_port,
      int32_t coproc_supervisor_port,
      std::vector<config::seed_server> seed_servers,
      ss::sstring base_dir,
      std::optional<scheduling_groups> sch_groups,
      bool remove_on_shutdown)
      : app(fmt::format("redpanda-{}", node_id()))
      , data_dir(std::move(base_dir))
      , remove_on_shutdown(remove_on_shutdown) {
        configure(
          node_id,
          kafka_port,
          rpc_port,
          coproc_supervisor_port,
          std::move(seed_servers));
        app.initialize(sch_groups);
        app.check_environment();
        app.configure_admin_server();
        app.wire_up_services();
        app.start();

        // used by request context builder
        proto = std::make_unique<kafka::protocol>(
          app.smp_service_groups.kafka_smp_sg(),
          app.metadata_cache,
          app.controller->get_topics_frontend(),
          app.quota_mgr,
          app.group_router,
          app.shard_table,
          app.partition_manager,
          app.coordinator_ntp_mapper,
          app.fetch_session_cache,
          app.id_allocator_frontend,
          app.credentials);
    }

    // creates single node with default configuration
    redpanda_thread_fixture()
      : redpanda_thread_fixture(
        model::node_id(1),
        9092,
        33145,
        43189,
        {},
        fmt::format("test.dir_{}", time(0)),
        std::nullopt,
        true) {}

    ~redpanda_thread_fixture() {
        app.shutdown();
        if (remove_on_shutdown) {
            std::filesystem::remove_all(data_dir);
        }
    }

    config::configuration& lconf() { return config::shard_local_cfg(); }

    void configure(
      model::node_id node_id,
      int32_t kafka_port,
      int32_t rpc_port,
      int32_t coproc_supervisor_port,
      std::vector<config::seed_server> seed_servers) {
        auto base_path = std::filesystem::path(data_dir);
        ss::smp::invoke_on_all([node_id,
                                kafka_port,
                                rpc_port,
                                coproc_supervisor_port,
                                seed_servers = std::move(seed_servers),
                                base_path]() mutable {
            auto& config = config::shard_local_cfg();
            config.get("node_id").set_value(node_id());

            config.get("rpc_server")
              .set_value(unresolved_address("127.0.0.1", rpc_port));
            config.get("kafka_api")
              .set_value(
                std::vector<model::broker_endpoint>{model::broker_endpoint(
                  unresolved_address("127.0.0.1", kafka_port))});
            config.get("seed_servers").set_value(seed_servers);
            config.get("enable_pid_file").set_value(false);
            config.get("developer_mode").set_value(true);
            config.get("enable_admin_api").set_value(false);
            config.get("enable_coproc").set_value(true);
            config.get("join_retry_timeout_ms").set_value(100ms);
            config.get("coproc_supervisor_server")
              .set_value(
                unresolved_address("127.0.0.1", coproc_supervisor_port));
            config.get("rack").set_value(std::optional<ss::sstring>(rack_name));
            config.get("disable_metrics").set_value(true);
            config.get("data_directory")
              .set_value(config::data_directory_path{.path = base_path});
        }).get0();
    }

    ss::future<> wait_for_controller_leadership() {
        return app.controller->get_partition_leaders()
          .local()
          .wait_for_leader(
            model::controller_ntp,
            ss::lowres_clock::now() + std::chrono::seconds(10),
            {})
          .discard_result();
    }

    ss::future<kafka::client::transport> make_kafka_client() {
        return rpc::resolve_dns(
                 config::shard_local_cfg().kafka_api()[0].address)
          .then([](ss::socket_address addr) {
              return kafka::client::transport(
                rpc::base_transport::configuration{
                  .server_addr = addr,
                });
          });
    }

    model::ntp
    make_default_ntp(model::topic topic, model::partition_id partition) {
        return model::ntp(model::kafka_namespace, std::move(topic), partition);
    }

    storage::log_config make_default_config() {
        return storage::log_config(
          storage::log_config::storage_type::disk,
          data_dir.string(),
          1_GiB,
          storage::debug_sanitize_files::yes);
    }

    ss::future<>
    add_topic(model::topic_namespace_view tp_ns, int partitions = 1) {
        std::vector<cluster::topic_configuration> cfgs{
          cluster::topic_configuration(tp_ns.ns, tp_ns.tp, partitions, 1)};
        return app.controller->get_topics_frontend()
          .local()
          .create_topics(std::move(cfgs), model::no_timeout)
          .then([this](std::vector<cluster::topic_result> results) {
              return tests::cooperative_spin_wait_with_timeout(
                2s, [this, results = std::move(results)] {
                    return std::all_of(
                      results.begin(),
                      results.end(),
                      [this](const cluster::topic_result& r) {
                          auto md = app.metadata_cache.local()
                                      .get_topic_metadata(r.tp_ns);
                          return md
                                 && std::all_of(
                                   md->partitions.begin(),
                                   md->partitions.end(),
                                   [this,
                                    &r](const model::partition_metadata& p) {
                                       return app.shard_table.local().shard_for(
                                         model::ntp(
                                           r.tp_ns.ns, r.tp_ns.tp, p.id));
                                   });
                      });
                });
          });
    }

    ss::future<> wait_for_partition_offset(
      model::ntp ntp,
      model::offset o,
      model::timeout_clock::duration tout = 3s) {
        return tests::cooperative_spin_wait_with_timeout(
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

    model::ntp make_data(model::revision_id rev) {
        auto topic_name = fmt::format("my_topic_{}", 0);
        model::ntp ntp(
          model::kafka_namespace,
          model::topic(topic_name),
          model::partition_id(0));

        storage::ntp_config ntp_cfg(
          ntp, lconf().data_directory().as_sstring(), nullptr, rev);

        storage::disk_log_builder builder(make_default_config());
        using namespace storage; // NOLINT
        builder | start(std::move(ntp_cfg)) | add_segment(model::offset(0))
          | add_random_batches(
            model::offset(0), 20, maybe_compress_batches::yes)
          | stop();

        add_topic(model::topic_namespace_view(ntp)).get();

        return ntp;
    }

    kafka::request_context make_request_context() {
        kafka::sasl_server sasl(kafka::sasl_server::sasl_state::complete);
        auto conn = ss::make_lw_shared<kafka::connection_context>(
          *proto, rpc::server::resources(nullptr, nullptr), std::move(sasl));

        kafka::request_header header;
        auto encoder_context = kafka::request_context(
          conn, std::move(header), iobuf(), std::chrono::milliseconds(0));

        iobuf buf;
        kafka::fetch_request request;
        // do not use incremental fetch requests
        request.max_wait_time = std::chrono::milliseconds::zero();
        kafka::response_writer writer(buf);
        request.encode(writer, encoder_context.header().version);

        return kafka::request_context(
          conn,
          std::move(header),
          std::move(buf),
          std::chrono::milliseconds(0));
    }

    application app;
    std::filesystem::path data_dir;
    std::unique_ptr<kafka::protocol> proto;
    bool remove_on_shutdown;
};
