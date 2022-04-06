// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "redpanda/application.h"

#include "archival/ntp_archiver_service.h"
#include "archival/service.h"
#include "archival/upload_controller.h"
#include "cluster/cluster_utils.h"
#include "cluster/controller.h"
#include "cluster/fwd.h"
#include "cluster/id_allocator.h"
#include "cluster/id_allocator_frontend.h"
#include "cluster/metadata_dissemination_handler.h"
#include "cluster/metadata_dissemination_service.h"
#include "cluster/partition_manager.h"
#include "cluster/rm_partition_frontend.h"
#include "cluster/security_frontend.h"
#include "cluster/service.h"
#include "cluster/topics_frontend.h"
#include "cluster/tx_gateway.h"
#include "cluster/tx_gateway_frontend.h"
#include "config/configuration.h"
#include "config/endpoint_tls_config.h"
#include "config/node_config.h"
#include "config/seed_server.h"
#include "coproc/api.h"
#include "coproc/partition_manager.h"
#include "kafka/client/configuration.h"
#include "kafka/server/coordinator_ntp_mapper.h"
#include "kafka/server/group_manager.h"
#include "kafka/server/group_metadata_migration.h"
#include "kafka/server/group_router.h"
#include "kafka/server/protocol.h"
#include "kafka/server/queue_depth_monitor.h"
#include "kafka/server/quota_manager.h"
#include "kafka/server/rm_group_frontend.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "net/server.h"
#include "pandaproxy/rest/configuration.h"
#include "pandaproxy/rest/proxy.h"
#include "pandaproxy/schema_registry/api.h"
#include "platform/stop_signal.h"
#include "raft/group_manager.h"
#include "raft/recovery_throttle.h"
#include "raft/service.h"
#include "redpanda/admin_server.h"
#include "resource_mgmt/io_priority.h"
#include "rpc/simple_protocol.h"
#include "storage/backlog_controller.h"
#include "storage/chunk_cache.h"
#include "storage/compaction_controller.h"
#include "storage/directories.h"
#include "syschecks/syschecks.h"
#include "utils/file_io.h"
#include "utils/human.h"
#include "v8_engine/data_policy_table.h"
#include "version.h"
#include "vlog.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/prometheus.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/thread.hh>
#include <seastar/json/json_elements.hh>
#include <seastar/net/tls.hh>
#include <seastar/util/conversions.hh>
#include <seastar/util/defer.hh>

#include <sys/resource.h>
#include <sys/utsname.h>

#include <chrono>
#include <exception>
#include <vector>

static void set_local_kafka_client_config(
  std::optional<kafka::client::configuration>& client_config,
  const config::node_config& config) {
    client_config.emplace();
    const auto& kafka_api = config.kafka_api.value();
    vassert(!kafka_api.empty(), "There are no kafka_api listeners");
    client_config->brokers.set_value(
      std::vector<net::unresolved_address>{kafka_api[0].address});
    const auto& kafka_api_tls = config::node().kafka_api_tls.value();
    auto tls_it = std::find_if(
      kafka_api_tls.begin(),
      kafka_api_tls.end(),
      [&kafka_api](const config::endpoint_tls_config& tls) {
          return tls.name == kafka_api[0].name;
      });
    if (tls_it != kafka_api_tls.end()) {
        client_config->broker_tls.set_value(tls_it->config);
    }
}

static void set_sr_local_kafka_client_config(
  std::optional<kafka::client::configuration>& client_config,
  const config::node_config& config) {
    set_local_kafka_client_config(client_config, config);
    if (client_config.has_value()) {
        if (!client_config->produce_batch_delay.is_overriden()) {
            client_config->produce_batch_delay.set_value(0ms);
        }
        if (!client_config->produce_batch_record_count.is_overriden()) {
            client_config->produce_batch_record_count.set_value(int32_t(0));
        }
        if (!client_config->produce_batch_size_bytes.is_overriden()) {
            client_config->produce_batch_size_bytes.set_value(int32_t(0));
        }
    }
}

application::application(ss::sstring logger_name)
  : _log(std::move(logger_name)){};

application::~application() = default;

static void log_system_resources(
  ss::logger& log, const boost::program_options::variables_map& cfg) {
    const auto shard_mem = ss::memory::stats();
    auto total_mem = shard_mem.total_memory() * ss::smp::count;
    /**
     * IMPORTANT: copied out of seastar `resources.cc`, if logic in seastar will
     * change we have to change our logic in here.
     */
    const size_t default_reserve_memory = std::max<size_t>(
      1536_MiB, 0.07 * total_mem);
    auto reserve = cfg.contains("reserve-memory") ? ss::parse_memory_size(
                     cfg["reserve-memory"].as<std::string>())
                                                  : default_reserve_memory;
    vlog(
      log.info,
      "System resources: {{ cpus: {}, available memory: {}, reserved memory: "
      "{}}}",
      ss::smp::count,
      human::bytes(total_mem),
      human::bytes(reserve));

    struct rlimit nofile = {0, 0};
    if (getrlimit(RLIMIT_NOFILE, &nofile) == 0) {
        vlog(
          log.info,
          "File handle limit: {}/{}",
          nofile.rlim_cur,
          nofile.rlim_max);
    } else {
        vlog(log.warn, "Error {} querying file handle limit", errno);
    }
}

int application::run(int ac, char** av) {
    init_env();
    vlog(_log.info, "Redpanda {}", redpanda_version());
    struct ::utsname buf;
    ::uname(&buf);
    vlog(
      _log.info,
      "kernel={}, nodename={}, machine={}",
      buf.release,
      buf.nodename,
      buf.machine);
    ss::app_template app(setup_app_config());
    app.add_options()(
      "redpanda-cfg",
      po::value<std::string>(),
      ".yaml file config for redpanda");

    return app.run(ac, av, [this, &app] {
        auto& cfg = app.configuration();
        log_system_resources(_log, cfg);
        validate_arguments(cfg);
        return ss::async([this, &cfg] {
            try {
                ::stop_signal app_signal;
                auto deferred = ss::defer([this] {
                    auto deferred = std::move(_deferred);
                    // stop services in reverse order
                    while (!deferred.empty()) {
                        deferred.pop_back();
                    }
                });
                // must initialize configuration before services
                hydrate_config(cfg);
                initialize();
                check_environment();
                setup_metrics();
                wire_up_services();
                configure_admin_server();
                start(app_signal);
                app_signal.wait().get();
                vlog(_log.info, "Stopping...");
            } catch (...) {
                vlog(
                  _log.info,
                  "Failure during startup: {}",
                  std::current_exception());
                return 1;
            }
            return 0;
        });
    });
}

void application::initialize(
  std::optional<YAML::Node> proxy_cfg,
  std::optional<YAML::Node> proxy_client_cfg,
  std::optional<YAML::Node> schema_reg_cfg,
  std::optional<YAML::Node> schema_reg_client_cfg,
  std::optional<scheduling_groups> groups) {
    /*
     * allocate per-core zstd decompression workspace. it can be several
     * megabytes in size, so do it before memory becomes fragmented.
     */
    ss::smp::invoke_on_all([] {
        compression::stream_zstd::init_workspace(
          config::shard_local_cfg().zstd_decompress_workspace_bytes());
    }).get0();

    if (config::shard_local_cfg().enable_pid_file()) {
        syschecks::pidfile_create(config::node().pidfile_path());
    }
    smp_groups::config smp_groups_cfg{
      .raft_group_max_non_local_requests
      = config::shard_local_cfg().raft_smp_max_non_local_requests(),
    };

    smp_service_groups.create_groups(smp_groups_cfg).get();
    _deferred.emplace_back(
      [this] { smp_service_groups.destroy_groups().get(); });

    if (groups) {
        _scheduling_groups = *groups;
        return;
    }

    _scheduling_groups.create_groups().get();
    _deferred.emplace_back(
      [this] { _scheduling_groups.destroy_groups().get(); });

    if (proxy_cfg) {
        _proxy_config.emplace(*proxy_cfg);
    }

    if (proxy_client_cfg) {
        _proxy_client_config.emplace(*proxy_client_cfg);
    }
    if (schema_reg_cfg) {
        _schema_reg_config.emplace(*schema_reg_cfg);
    }

    if (schema_reg_client_cfg) {
        _schema_reg_client_config.emplace(*schema_reg_client_cfg);
    }
}

void application::setup_metrics() {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    namespace sm = ss::metrics;

    // build info
    auto version_label = sm::label("version");
    auto revision_label = sm::label("revision");
    std::vector<sm::label_instance> build_labels{
      version_label(redpanda_git_version()),
      revision_label(redpanda_git_revision()),
    };

    _metrics.add_group(
      "application",
      {
        sm::make_gauge(
          "uptime",
          [] {
              return std::chrono::duration_cast<std::chrono::milliseconds>(
                       ss::engine().uptime())
                .count();
          },
          sm::description("Redpanda uptime in milliseconds")),

        sm::make_gauge(
          "build",
          [] { return 1; },
          sm::description("Redpanda build information"),
          build_labels),
      });
}

void application::validate_arguments(const po::variables_map& cfg) {
    if (!cfg.count("redpanda-cfg")) {
        throw std::invalid_argument("Missing redpanda-cfg flag");
    }
}

void application::init_env() { std::setvbuf(stdout, nullptr, _IOLBF, 1024); }

ss::app_template::config application::setup_app_config() {
    ss::app_template::config app_cfg;
    app_cfg.name = "Redpanda";
    using namespace std::literals::chrono_literals; // NOLINT
    app_cfg.default_task_quota = 500us;
    app_cfg.auto_handle_sigint_sigterm = false;
    return app_cfg;
}

void application::hydrate_config(const po::variables_map& cfg) {
    std::filesystem::path cfg_path(cfg["redpanda-cfg"].as<std::string>());
    const YAML::Node config = YAML::Load(read_fully_to_string(cfg_path).get0());
    auto config_printer = [this](std::string_view service) {
        return [this, service](const config::base_property& item) {
            std::stringstream val;
            item.print(val);
            vlog(_log.info, "{}.{}\t- {}", service, val.str(), item.desc());
        };
    };
    _redpanda_enabled = config["redpanda"];
    if (_redpanda_enabled) {
        ss::smp::invoke_on_all([&config, cfg_path] {
            config::node().load(cfg_path, config);
        }).get0();

        auto node_config_errors = config::node().load(config);
        for (const auto& i : node_config_errors) {
            vlog(
              _log.warn,
              "Node property '{}' validation error: {}",
              i.first,
              i.second);
        }
        if (node_config_errors.size() > 0) {
            throw std::invalid_argument("Validation errors in node config");
        }

        // This includes loading from local bootstrap file or legacy
        // config file on first-start or upgrade cases.
        _config_preload = cluster::config_manager::preload(config).get0();

        vlog(_log.info, "Cluster configuration properties:");
        vlog(_log.info, "(use `rpk cluster config edit` to change)");
        config::shard_local_cfg().for_each(config_printer("redpanda"));

        vlog(_log.info, "Node configuration properties:");
        vlog(_log.info, "(use `rpk config set <cfg> <value>` to change)");
        config::node().for_each(config_printer("redpanda"));
    }
    if (config["pandaproxy"]) {
        _proxy_config.emplace(config["pandaproxy"]);
        if (config["pandaproxy_client"]) {
            _proxy_client_config.emplace(config["pandaproxy_client"]);
        } else {
            set_local_kafka_client_config(_proxy_client_config, config::node());
        }
        // override pandaparoxy_client.consumer_session_timeout_ms with
        // pandaproxy.consumer_instance_timeout_ms
        _proxy_client_config->consumer_session_timeout.set_value(
          _proxy_config->consumer_instance_timeout.value());
        _proxy_config->for_each(config_printer("pandaproxy"));
        _proxy_client_config->for_each(config_printer("pandaproxy_client"));
    }
    if (config["schema_registry"]) {
        _schema_reg_config.emplace(config["schema_registry"]);
        if (config["schema_registry_client"]) {
            _schema_reg_client_config.emplace(config["schema_registry_client"]);
        } else {
            set_sr_local_kafka_client_config(
              _schema_reg_client_config, config::node());
        }
        _schema_reg_config->for_each(config_printer("schema_registry"));
        _schema_reg_client_config->for_each(
          config_printer("schema_registry_client"));
    }
}

void application::check_environment() {
    syschecks::systemd_message("checking environment (CPU, Mem)").get();
    syschecks::cpu();
    syschecks::memory(config::node().developer_mode());
    if (_redpanda_enabled) {
        storage::directories::initialize(
          config::node().data_directory().as_sstring())
          .get();
    }
}

static admin_server_cfg
admin_server_cfg_from_global_cfg(scheduling_groups& sgs) {
    return admin_server_cfg{
      .endpoints = config::node().admin(),
      .endpoints_tls = config::node().admin_api_tls(),
      .dashboard_dir = config::node().dashboard_dir(),
      .admin_api_docs_dir = config::node().admin_api_doc_dir(),
      .sg = sgs.admin_sg()};
}

void application::configure_admin_server() {
    if (config::node().admin().empty()) {
        return;
    }

    syschecks::systemd_message("constructing http server").get();
    construct_service(
      _admin,
      admin_server_cfg_from_global_cfg(_scheduling_groups),
      std::ref(partition_manager),
      std::ref(cp_partition_manager),
      controller.get(),
      std::ref(shard_table),
      std::ref(metadata_cache))
      .get();
}

static storage::kvstore_config kvstore_config_from_global_config() {
    /*
     * The key-value store is rooted at the configured data directory, and
     * the internal kvstore topic-namespace results in a storage layout of:
     *
     *    /var/lib/redpanda/data/
     *       - redpanda/kvstore/
     *           - 0
     *           - 1
     *           - ... #cores
     */
    return storage::kvstore_config(
      config::shard_local_cfg().kvstore_max_segment_size(),
      config::shard_local_cfg().kvstore_flush_interval.bind(),
      config::node().data_directory().as_sstring(),
      storage::debug_sanitize_files::no);
}

static storage::log_config
manager_config_from_global_config(scheduling_groups& sgs) {
    return storage::log_config(
      storage::log_config::storage_type::disk,
      config::node().data_directory().as_sstring(),
      config::shard_local_cfg().log_segment_size.bind(),
      config::shard_local_cfg().compacted_log_segment_size.bind(),
      config::shard_local_cfg().max_compacted_log_segment_size.bind(),
      storage::debug_sanitize_files::no,
      priority_manager::local().compaction_priority(),
      config::shard_local_cfg().retention_bytes.bind(),
      config::shard_local_cfg().log_compaction_interval_ms.bind(),
      config::shard_local_cfg().delete_retention_ms.bind(),
      storage::with_cache(!config::shard_local_cfg().disable_batch_cache()),
      storage::batch_cache::reclaim_options{
        .growth_window = config::shard_local_cfg().reclaim_growth_window(),
        .stable_window = config::shard_local_cfg().reclaim_stable_window(),
        .min_size = config::shard_local_cfg().reclaim_min_size(),
        .max_size = config::shard_local_cfg().reclaim_max_size(),
        .min_free_memory
        = config::shard_local_cfg().reclaim_batch_cache_min_free(),
      },
      config::shard_local_cfg().readers_cache_eviction_timeout_ms(),
      sgs.compaction_sg());
}

static storage::backlog_controller_config compaction_controller_config(
  ss::scheduling_group sg, const ss::io_priority_class& iopc) {
    auto space_info = std::filesystem::space(
      config::node().data_directory().path);
    /**
     * By default we set desired compaction backlog size to 10% of disk
     * capacity.
     */
    static const int64_t backlog_capacity_percents = 10;
    int64_t backlog_size
      = config::shard_local_cfg().compaction_ctrl_backlog_size().value_or(
        (space_info.capacity / 100) * backlog_capacity_percents
        / ss::smp::count);

    /**
     * We normalize internals using disk capacity to make controller settings
     * independent from disk space. After normalization all values equal to disk
     * capacity will be represented in the controller with value equal 1000.
     *
     * Set point = 10% of disk capacity will always be equal to 100.
     *
     * This way we can calculate proportional coefficient.
     *
     * We assume that when error is greater than 80% of setpoint we should be
     * running compaction with maximum allowed shares.
     * This way we can calculate proportional coefficient as
     *
     *  k_p = 1000 / 80 = 12.5
     *
     */
    auto normalization = space_info.capacity / (1000 * ss::smp::count);

    return storage::backlog_controller_config(
      config::shard_local_cfg().compaction_ctrl_p_coeff(),
      config::shard_local_cfg().compaction_ctrl_i_coeff(),
      config::shard_local_cfg().compaction_ctrl_d_coeff(),
      normalization,
      backlog_size,
      200,
      config::shard_local_cfg().compaction_ctrl_update_interval_ms(),
      sg,
      iopc,
      config::shard_local_cfg().compaction_ctrl_min_shares(),
      config::shard_local_cfg().compaction_ctrl_max_shares());
}

static storage::backlog_controller_config
make_upload_controller_config(ss::scheduling_group sg) {
    // This settings are similar to compaction_controller_config.
    // The desired setpoint for archival is set to 0 since the goal is to upload
    // all data that we have.
    // If the size of the backlog (the data which should be uploaded to S3) is
    // larger than this value we need to bump the scheduling priority.
    // Otherwise, we're good with the minimal.
    // Since the setpoint is 0 we can't really use integral component of the
    // controller. This is because upload backlog size never gets negative so
    // once integral part will rump up high enough it won't be able to go down
    // even if everything is uploaded.

    auto available
      = ss::fs_avail(config::node().data_directory().path.string()).get();
    int64_t setpoint = 0;
    int64_t normalization = static_cast<int64_t>(available)
                            / (1000 * ss::smp::count);
    return {
      config::shard_local_cfg().cloud_storage_upload_ctrl_p_coeff(),
      0,
      config::shard_local_cfg().cloud_storage_upload_ctrl_d_coeff(),
      normalization,
      setpoint,
      static_cast<int>(
        priority_manager::local().archival_priority().get_shares()),
      config::shard_local_cfg().cloud_storage_upload_ctrl_update_interval_ms(),
      sg,
      priority_manager::local().archival_priority(),
      config::shard_local_cfg().cloud_storage_upload_ctrl_min_shares(),
      config::shard_local_cfg().cloud_storage_upload_ctrl_max_shares()};
}

// add additional services in here
void application::wire_up_services() {
    if (_redpanda_enabled) {
        wire_up_redpanda_services();
    }
    if (_proxy_config) {
        construct_service(_proxy_client, to_yaml(*_proxy_client_config)).get();
        construct_service(
          _proxy,
          to_yaml(*_proxy_config),
          smp_service_groups.proxy_smp_sg(),
          // TODO: Improve memory budget for services
          // https://github.com/redpanda-data/redpanda/issues/1392
          memory_groups::kafka_total_memory(),
          std::reference_wrapper(_proxy_client))
          .get();
    }
    if (_schema_reg_config) {
        construct_single_service(
          _schema_registry,
          config::node().node_id(),
          smp_service_groups.proxy_smp_sg(),
          // TODO: Improve memory budget for services
          // https://github.com/redpanda-data/redpanda/issues/1392
          memory_groups::kafka_total_memory(),
          *_schema_reg_client_config,
          *_schema_reg_config);
    }
}

void application::wire_up_redpanda_services() {
    ss::smp::invoke_on_all([] {
        return storage::internal::chunks().start();
    }).get();

    // cluster
    syschecks::systemd_message("Adding raft client cache").get();
    construct_service(_connection_cache).get();
    syschecks::systemd_message("Building shard-lookup tables").get();
    construct_service(shard_table).get();

    syschecks::systemd_message("Intializing storage services").get();
    construct_single_service_sharded(storage_node).get();

    construct_service(
      storage,
      []() { return kvstore_config_from_global_config(); },
      [this]() {
          auto log_cfg = manager_config_from_global_config(_scheduling_groups);
          log_cfg.reclaim_opts.background_reclaimer_sg
            = _scheduling_groups.cache_background_reclaim_sg();
          return log_cfg;
      })
      .get();

    syschecks::systemd_message("Intializing raft recovery throttle").get();
    recovery_throttle
      .start(
        config::shard_local_cfg().raft_learner_recovery_rate() / ss::smp::count)
      .get();

    syschecks::systemd_message("Intializing raft group manager").get();
    raft_group_manager
      .start(
        model::node_id(config::node().node_id()),
        config::shard_local_cfg().raft_io_timeout_ms(),
        _scheduling_groups.raft_sg(),
        config::shard_local_cfg().raft_heartbeat_interval_ms(),
        config::shard_local_cfg().raft_heartbeat_timeout_ms(),
        [] {
            return raft::recovery_memory_quota::configuration{
              .max_recovery_memory
              = config::shard_local_cfg().raft_max_recovery_memory.bind(),
              .default_read_buffer_size
              = config::shard_local_cfg()
                  .raft_recovery_default_read_size.bind(),
            };
        },
        std::ref(_connection_cache),
        std::ref(storage),
        std::ref(recovery_throttle))
      .get();

    // custom handling for recovery_throttle and raft group manager shutdown.
    // the former needs to happen first in order to ensure that any raft groups
    // that are being throttled are released so that they can make be quickly
    // shutdown by the group manager.
    _deferred.emplace_back([this] {
        recovery_throttle.invoke_on_all(&raft::recovery_throttle::shutdown)
          .get();
        raft_group_manager.stop().get();
        recovery_throttle.stop().get();
    });

    if (archival_storage_enabled()) {
        syschecks::systemd_message("Starting cloud storage api").get();
        ss::sharded<cloud_storage::configuration> cloud_configs;
        cloud_configs.start().get();
        cloud_configs
          .invoke_on_all([](cloud_storage::configuration& c) {
              return cloud_storage::configuration::get_config().then(
                [&c](cloud_storage::configuration cfg) { c = std::move(cfg); });
          })
          .get();
        construct_service(cloud_storage_api, std::ref(cloud_configs)).get();

        construct_service(
          partition_recovery_manager,
          cloud_configs.local().bucket_name,
          std::ref(cloud_storage_api))
          .get();

        cloud_configs.stop().get();
    }

    syschecks::systemd_message("Adding partition manager").get();
    construct_service(
      partition_manager,
      std::ref(storage),
      std::ref(raft_group_manager),
      std::ref(tx_gateway_frontend),
      std::ref(partition_recovery_manager),
      std::ref(cloud_storage_api),
      std::ref(shadow_index_cache))
      .get();
    vlog(_log.info, "Partition manager started");

    construct_service(cp_partition_manager, std::ref(storage)).get();

    // controller

    construct_service(data_policies).get();

    syschecks::systemd_message("Creating cluster::controller").get();

    construct_single_service(
      controller,
      std::move(_config_preload),
      _connection_cache,
      partition_manager,
      shard_table,
      storage,
      storage_node,
      std::ref(raft_group_manager),
      data_policies);

    controller->wire_up().get0();
    syschecks::systemd_message("Creating kafka metadata cache").get();
    construct_service(
      metadata_cache,
      std::ref(controller->get_topics_state()),
      std::ref(controller->get_members_table()),
      std::ref(controller->get_partition_leaders()),
      std::ref(controller->get_health_monitor()))
      .get();
    /**
     * Wait for all requests to finish before removing critical redpanda
     * services, that may be used by
     */
    _deferred.emplace_back([this] {
        if (_rpc.local_is_initialized()) {
            _rpc.invoke_on_all(&net::server::wait_for_shutdown).get();
            _rpc.stop().get();
        }
    });

    // metrics and quota management
    syschecks::systemd_message("Adding kafka quota manager").get();
    construct_service(quota_mgr).get();

    _deferred.emplace_back([this] {
        if (_kafka_server.local_is_initialized()) {
            _kafka_server.invoke_on_all(&net::server::wait_for_shutdown).get();
            _kafka_server.stop().get();
            _kafka_conn_quotas.stop().get();
        }
    });

    _deferred.emplace_back([this] {
        partition_manager
          .invoke_on_all(&cluster::partition_manager::stop_partitions)
          .get();
    });
    _deferred.emplace_back([this] {
        // Prior to shutting down partition manager (which clears out all the
        // raft `consensus` instances), stop processing heartbeats.  Otherwise
        // we are receiving heartbeats that we can't match up to raft groups.
        raft_group_manager.invoke_on_all(&raft::group_manager::stop_heartbeats)
          .get();
    });
    _deferred.emplace_back([this] {
        cp_partition_manager
          .invoke_on_all(&coproc::partition_manager::stop_partitions)
          .get();
    });
    syschecks::systemd_message("Creating metadata dissemination service").get();
    construct_service(
      md_dissemination_service,
      std::ref(raft_group_manager),
      std::ref(partition_manager),
      std::ref(controller->get_partition_leaders()),
      std::ref(controller->get_members_table()),
      std::ref(controller->get_topics_state()),
      std::ref(_connection_cache),
      std::ref(controller->get_health_monitor()))
      .get();

    if (archival_storage_enabled()) {
        syschecks::systemd_message("Starting shadow indexing cache").get();
        auto cache_path_cfg
          = config::node().cloud_storage_cache_directory.value();
        auto redpanda_dir = config::node().data_directory.value();
        std::filesystem::path cache_dir = redpanda_dir.path
                                          / "cloud_storage_cache";
        if (cache_path_cfg) {
            cache_dir = std::filesystem::path(cache_path_cfg.value());
        }
        auto cache_size
          = config::shard_local_cfg().cloud_storage_cache_size.value();
        auto cache_interval = config::shard_local_cfg()
                                .cloud_storage_cache_check_interval_ms.value();
        construct_service(
          shadow_index_cache, cache_dir, cache_size, cache_interval)
          .get();

        shadow_index_cache
          .invoke_on_all(
            [](cloud_storage::cache& cache) { return cache.start(); })
          .get();

        syschecks::systemd_message("Starting archival scheduler").get();
        ss::sharded<archival::configuration> arch_configs;
        arch_configs.start().get();
        arch_configs
          .invoke_on_all([this](archival::configuration& c) {
              return archival::scheduler_service::get_archival_service_config(
                       _scheduling_groups.archival_upload(),
                       archival_priority())
                .then(
                  [&c](archival::configuration cfg) { c = std::move(cfg); });
          })
          .get();
        construct_service(
          archival_scheduler,
          std::ref(cloud_storage_api),
          std::ref(storage),
          std::ref(partition_manager),
          std::ref(controller->get_topics_state()),
          std::ref(arch_configs))
          .get();
        arch_configs.stop().get();

        construct_service(
          _archival_upload_controller,
          std::ref(archival_scheduler),
          make_upload_controller_config(_scheduling_groups.archival_upload()))
          .get();
    }
    // group membership
    syschecks::systemd_message("Creating partition manager").get();
    construct_service(
      _group_manager,
      model::kafka_group_nt,
      std::ref(raft_group_manager),
      std::ref(partition_manager),
      std::ref(controller->get_topics_state()),
      &kafka::make_backward_compatible_serializer,
      std::ref(config::shard_local_cfg()),
      kafka::enable_group_metrics::no)
      .get();
    construct_service(
      _co_group_manager,
      model::kafka_consumer_offsets_nt,
      std::ref(raft_group_manager),
      std::ref(partition_manager),
      std::ref(controller->get_topics_state()),
      &kafka::make_consumer_offsets_serializer,
      std::ref(config::shard_local_cfg()),
      kafka::enable_group_metrics::yes)
      .get();
    syschecks::systemd_message("Creating kafka group shard mapper").get();
    construct_service(
      coordinator_ntp_mapper, std::ref(metadata_cache), model::kafka_group_nt)
      .get();
    construct_service(
      co_coordinator_ntp_mapper,
      std::ref(metadata_cache),
      model::kafka_consumer_offsets_nt)
      .get();
    syschecks::systemd_message("Creating kafka group router").get();
    construct_service(
      group_router,
      _scheduling_groups.kafka_sg(),
      smp_service_groups.kafka_smp_sg(),
      std::ref(_group_manager),
      std::ref(_co_group_manager),
      std::ref(shard_table),
      std::ref(coordinator_ntp_mapper),
      std::ref(co_coordinator_ntp_mapper),
      std::ref(controller->get_feature_table()))
      .get();
    if (coproc_enabled()) {
        syschecks::systemd_message("Creating coproc::api").get();
        construct_single_service(
          coprocessing,
          config::node().coproc_supervisor_server(),
          std::ref(storage),
          std::ref(controller->get_topics_state()),
          std::ref(shard_table),
          std::ref(controller->get_topics_frontend()),
          std::ref(metadata_cache),
          std::ref(partition_manager),
          std::ref(cp_partition_manager));
        coprocessing->start().get();
    }

    // rpc
    ss::sharded<net::server_configuration> rpc_cfg;
    rpc_cfg.start(ss::sstring("internal_rpc")).get();
    rpc_cfg
      .invoke_on_all([this](net::server_configuration& c) {
          return ss::async([this, &c] {
              auto rpc_server_addr
                = net::resolve_dns(config::node().rpc_server()).get0();
              c.load_balancing_algo
                = ss::server_socket::load_balancing_algorithm::port;
              c.max_service_memory_per_core = memory_groups::rpc_total_memory();
              c.disable_metrics = net::metrics_disabled(
                config::shard_local_cfg().disable_metrics());
              c.listen_backlog
                = config::shard_local_cfg().rpc_server_listen_backlog;
              c.tcp_recv_buf
                = config::shard_local_cfg().rpc_server_tcp_recv_buf;
              c.tcp_send_buf
                = config::shard_local_cfg().rpc_server_tcp_send_buf;
              auto rpc_builder = config::node()
                                   .rpc_server_tls()
                                   .get_credentials_builder()
                                   .get0();
              auto credentials
                = rpc_builder
                    ? rpc_builder
                        ->build_reloadable_server_credentials(
                          [this](
                            const std::unordered_set<ss::sstring>& updated,
                            const std::exception_ptr& eptr) {
                              cluster::log_certificate_reload_event(
                                _log, "Internal RPC TLS", updated, eptr);
                          })
                        .get0()
                    : nullptr;
              c.addrs.emplace_back(rpc_server_addr, credentials);
          });
      })
      .get();
    /**
     * Use port based load_balancing_algorithm to make connection shard
     * assignment deterministic.
     **/
    syschecks::systemd_message("Starting internal RPC {}", rpc_cfg.local())
      .get();
    _rpc.start(&rpc_cfg).get();
    rpc_cfg.stop().get();

    syschecks::systemd_message("Creating id allocator frontend").get();
    construct_service(
      id_allocator_frontend,
      smp_service_groups.raft_smp_sg(),
      std::ref(partition_manager),
      std::ref(shard_table),
      std::ref(metadata_cache),
      std::ref(_connection_cache),
      std::ref(controller->get_partition_leaders()),
      std::ref(controller))
      .get();

    syschecks::systemd_message("Creating group resource manager frontend")
      .get();
    construct_service(
      rm_group_frontend,
      std::ref(metadata_cache),
      std::ref(_connection_cache),
      std::ref(controller->get_partition_leaders()),
      controller.get(),
      std::ref(group_router))
      .get();

    _rm_group_proxy = std::make_unique<kafka::rm_group_proxy_impl>(
      std::ref(rm_group_frontend));

    syschecks::systemd_message("Creating partition resource manager frontend")
      .get();
    construct_service(
      rm_partition_frontend,
      smp_service_groups.raft_smp_sg(),
      std::ref(partition_manager),
      std::ref(shard_table),
      std::ref(metadata_cache),
      std::ref(_connection_cache),
      std::ref(controller->get_partition_leaders()),
      controller.get())
      .get();

    syschecks::systemd_message("Creating tx coordinator frontend").get();
    // usually it'a an anti-pattern to let the same object be accessed
    // from different cores without precautionary measures like foreign
    // ptr. we treat exceptions on the case by case basis validating the
    // access patterns, sharing sharded service with only `.local()' uses
    // is a safe bet, sharing _rm_group_proxy is fine because it wraps
    // sharded service with only `.local()' access
    construct_service(
      tx_gateway_frontend,
      smp_service_groups.raft_smp_sg(),
      std::ref(partition_manager),
      std::ref(shard_table),
      std::ref(metadata_cache),
      std::ref(_connection_cache),
      std::ref(controller->get_partition_leaders()),
      controller.get(),
      std::ref(id_allocator_frontend),
      _rm_group_proxy.get(),
      std::ref(rm_partition_frontend))
      .get();

    _kafka_conn_quotas
      .start([]() {
          return net::conn_quota_config{
            .max_connections
            = config::shard_local_cfg().kafka_connections_max.bind(),
            .max_connections_per_ip
            = config::shard_local_cfg().kafka_connections_max_per_ip.bind(),
          };
      })
      .get();

    ss::sharded<net::server_configuration> kafka_cfg;
    kafka_cfg.start(ss::sstring("kafka_rpc")).get();
    kafka_cfg
      .invoke_on_all([this](net::server_configuration& c) {
          return ss::async([this, &c] {
              c.conn_quotas = std::ref(_kafka_conn_quotas);
              c.max_service_memory_per_core
                = memory_groups::kafka_total_memory();
              c.listen_backlog
                = config::shard_local_cfg().rpc_server_listen_backlog;
              c.tcp_recv_buf
                = config::shard_local_cfg().rpc_server_tcp_recv_buf;
              c.tcp_send_buf
                = config::shard_local_cfg().rpc_server_tcp_send_buf;
              auto& tls_config = config::node().kafka_api_tls.value();
              for (const auto& ep : config::node().kafka_api()) {
                  ss::shared_ptr<ss::tls::server_credentials> credentails;
                  // find credentials for this endpoint
                  auto it = find_if(
                    tls_config.begin(),
                    tls_config.end(),
                    [&ep](const config::endpoint_tls_config& cfg) {
                        return cfg.name == ep.name;
                    });
                  // if tls is configured for this endpoint build reloadable
                  // credentails
                  if (it != tls_config.end()) {
                      syschecks::systemd_message(
                        "Building TLS credentials for kafka")
                        .get();
                      auto kafka_builder
                        = it->config.get_credentials_builder().get0();
                      credentails
                        = kafka_builder
                            ? kafka_builder
                                ->build_reloadable_server_credentials(
                                  [this, name = it->name](
                                    const std::unordered_set<ss::sstring>&
                                      updated,
                                    const std::exception_ptr& eptr) {
                                      cluster::log_certificate_reload_event(
                                        _log, "Kafka RPC TLS", updated, eptr);
                                  })
                                .get0()
                            : nullptr;
                  }

                  c.addrs.emplace_back(
                    ep.name, net::resolve_dns(ep.address).get0(), credentails);
              }

              c.disable_metrics = net::metrics_disabled(
                config::shard_local_cfg().disable_metrics());

              net::config_connection_rate_bindings bindings{
                .config_general_rate
                = config::shard_local_cfg().kafka_connection_rate_limit.bind(),
                .config_overrides_rate
                = config::shard_local_cfg()
                    .kafka_connection_rate_limit_overrides.bind(),
              };

              c.connection_rate_bindings.emplace(std::move(bindings));
          });
      })
      .get();
    syschecks::systemd_message("Starting kafka RPC {}", kafka_cfg.local())
      .get();
    _kafka_server.start(&kafka_cfg).get();
    kafka_cfg.stop().get();
    construct_service(
      fetch_session_cache,
      config::shard_local_cfg().fetch_session_eviction_timeout_ms())
      .get();
    construct_service(
      _compaction_controller,
      std::ref(storage),
      compaction_controller_config(
        _scheduling_groups.compaction_sg(),
        priority_manager::local().compaction_priority()))
      .get();
}

ss::future<> application::set_proxy_config(ss::sstring name, std::any val) {
    return _proxy.invoke_on_all(
      [name{std::move(name)}, val{std::move(val)}](pandaproxy::rest::proxy& p) {
          p.config().get(name).set_value(val);
      });
}

bool application::archival_storage_enabled() {
    const auto& cfg = config::shard_local_cfg();
    return cfg.cloud_storage_enabled();
}

ss::future<>
application::set_proxy_client_config(ss::sstring name, std::any val) {
    return _proxy.invoke_on_all(
      [name{std::move(name)}, val{std::move(val)}](pandaproxy::rest::proxy& p) {
          p.client_config().get(name).set_value(val);
      });
}

void application::start(::stop_signal& app_signal) {
    if (_redpanda_enabled) {
        start_redpanda(app_signal);
    }

    if (_proxy_config) {
        _proxy.invoke_on_all(&pandaproxy::rest::proxy::start).get();
        vlog(
          _log.info,
          "Started Pandaproxy listening at {}",
          _proxy_config->pandaproxy_api());
    }

    if (_schema_reg_config) {
        _schema_registry->start().get();
        vlog(
          _log.info,
          "Started Schema Registry listening at {}",
          _schema_reg_config->schema_registry_api());
    }

    if (_redpanda_enabled) {
        start_kafka(app_signal);
    }

    _admin.invoke_on_all([](admin_server& admin) { admin.set_ready(); }).get();

    vlog(_log.info, "Successfully started Redpanda!");
    syschecks::systemd_notify_ready().get();
}

void application::start_redpanda(::stop_signal& app_signal) {
    syschecks::systemd_message("Staring storage services").get();
    // single instance
    storage_node.invoke_on_all(&storage::node_api::start).get0();
    storage.invoke_on_all(&storage::api::start).get();

    syschecks::systemd_message("Starting the partition manager").get();
    partition_manager.invoke_on_all(&cluster::partition_manager::start).get();

    syschecks::systemd_message("Starting the coproc partition manager").get();
    cp_partition_manager.invoke_on_all(&coproc::partition_manager::start).get();

    syschecks::systemd_message("Starting Raft group manager").get();
    raft_group_manager.invoke_on_all(&raft::group_manager::start).get();

    syschecks::systemd_message("Starting Kafka group manager").get();
    _group_manager.invoke_on_all(&kafka::group_manager::start).get();
    _co_group_manager.invoke_on_all(&kafka::group_manager::start).get();

    syschecks::systemd_message("Starting controller").get();
    controller->start().get0();
    /**
     * We schedule shutting down controller input and aborting its operation
     * as a first shutdown step. (other services are stopeed in
     * an order reverse to the startup sequence.) This way we terminate all long
     * running opertions before shutting down the RPC server, preventing it to
     * wait on background dispatch gate `close` call.
     *
     * NOTE controller has to be stopped only after it was started
     */
    auto group_migration = ss::make_lw_shared<kafka::group_metadata_migration>(
      *controller, group_router);

    _deferred.emplace_back(
      [group_migration] { group_migration->await().get(); });

    _deferred.emplace_back([this] { controller->shutdown_input().get(); });
    // FIXME: in first patch explain why this is started after the
    // controller so the broker set will be available. Then next patch fix.
    syschecks::systemd_message("Starting metadata dissination service").get();
    md_dissemination_service
      .invoke_on_all(&cluster::metadata_dissemination_service::start)
      .get();

    syschecks::systemd_message("Starting RPC").get();
    _rpc
      .invoke_on_all([this](net::server& s) {
          auto proto = std::make_unique<rpc::simple_protocol>();
          proto->register_service<cluster::id_allocator>(
            _scheduling_groups.raft_sg(),
            smp_service_groups.raft_smp_sg(),
            std::ref(id_allocator_frontend));
          // _rm_group_proxy is wrap around a sharded service with only
          // `.local()' access so it's ok to share without foreign_ptr
          proto->register_service<cluster::tx_gateway>(
            _scheduling_groups.raft_sg(),
            smp_service_groups.raft_smp_sg(),
            std::ref(tx_gateway_frontend),
            _rm_group_proxy.get(),
            std::ref(rm_partition_frontend));
          proto->register_service<
            raft::service<cluster::partition_manager, cluster::shard_table>>(
            _scheduling_groups.raft_sg(),
            smp_service_groups.raft_smp_sg(),
            partition_manager,
            shard_table.local(),
            config::shard_local_cfg().raft_heartbeat_interval_ms());
          proto->register_service<cluster::service>(
            _scheduling_groups.cluster_sg(),
            smp_service_groups.cluster_smp_sg(),
            std::ref(controller->get_topics_frontend()),
            std::ref(controller->get_members_manager()),
            std::ref(metadata_cache),
            std::ref(controller->get_security_frontend()),
            std::ref(controller->get_api()),
            std::ref(controller->get_members_frontend()),
            std::ref(controller->get_config_frontend()),
            std::ref(controller->get_feature_manager()),
            std::ref(controller->get_feature_table()),
            std::ref(controller->get_health_monitor()));

          proto->register_service<cluster::metadata_dissemination_handler>(
            _scheduling_groups.cluster_sg(),
            smp_service_groups.cluster_smp_sg(),
            std::ref(controller->get_partition_leaders()));
          if (!config::shard_local_cfg().disable_metrics()) {
              proto->setup_metrics();
          }
          s.set_protocol(std::move(proto));
      })
      .get();
    _rpc.invoke_on_all(&net::server::start).get();
    // shutdown input on RPC server
    _deferred.emplace_back(
      [this] { _rpc.invoke_on_all(&net::server::shutdown_input).get(); });
    vlog(
      _log.info,
      "Started RPC server listening at {}",
      config::node().rpc_server());

    // After we have started internal RPC listener, we may join
    // the cluster (if we aren't already a member)
    controller->get_members_manager()
      .invoke_on(
        cluster::members_manager::shard,
        &cluster::members_manager::join_cluster)
      .get();

    if (archival_storage_enabled()) {
        syschecks::systemd_message("Starting archival storage").get();
        archival_scheduler
          .invoke_on_all(
            [](archival::scheduler_service& svc) { return svc.start(); })
          .get();
    }

    quota_mgr.invoke_on_all(&kafka::quota_manager::start).get();

    if (!config::node().admin().empty()) {
        _admin.invoke_on_all(&admin_server::start).get0();
    }

    _compaction_controller.invoke_on_all(&storage::compaction_controller::start)
      .get();
    _archival_upload_controller
      .invoke_on_all(&archival::upload_controller::start)
      .get();

    group_migration->start(app_signal.abort_source()).get();
}

/**
 * The Kafka protocol listener startup is separate to the rest of Redpanda,
 * because it includes a wait for this node to be a full member of a redpanda
 * cluster -- this is expected to be run last, after everything else is
 * started.
 */
void application::start_kafka(::stop_signal& app_signal) {
    // Kafka API
    // The Kafka listener is intentionally the last thing we start: during
    // this phase we will wait for the node to be a cluster member before
    // proceeding, because it is not helpful to clients for us to serve
    // kafka requests before we have up to date knowledge of the system.
    std::optional<kafka::qdc_monitor::config> qdc_config;
    if (config::shard_local_cfg().kafka_qdc_enable()) {
        qdc_config = kafka::qdc_monitor::config{
          .latency_alpha = config::shard_local_cfg().kafka_qdc_latency_alpha(),
          .max_latency = config::shard_local_cfg().kafka_qdc_max_latency_ms(),
          .window_count = config::shard_local_cfg().kafka_qdc_window_count(),
          .window_size = config::shard_local_cfg().kafka_qdc_window_size_ms(),
          .depth_alpha = config::shard_local_cfg().kafka_qdc_depth_alpha(),
          .idle_depth = config::shard_local_cfg().kafka_qdc_idle_depth(),
          .min_depth = config::shard_local_cfg().kafka_qdc_min_depth(),
          .max_depth = config::shard_local_cfg().kafka_qdc_max_depth(),
          .depth_update_freq
          = config::shard_local_cfg().kafka_qdc_depth_update_ms(),
        };
    }
    _kafka_server
      .invoke_on_all([this, qdc_config](net::server& s) {
          auto proto = std::make_unique<kafka::protocol>(
            smp_service_groups.kafka_smp_sg(),
            metadata_cache,
            controller->get_topics_frontend(),
            controller->get_config_frontend(),
            controller->get_feature_table(),
            quota_mgr,
            group_router,
            shard_table,
            partition_manager,
            fetch_session_cache,
            id_allocator_frontend,
            controller->get_credential_store(),
            controller->get_authorizer(),
            controller->get_security_frontend(),
            controller->get_api(),
            tx_gateway_frontend,
            cp_partition_manager,
            data_policies,
            qdc_config);
          s.set_protocol(std::move(proto));
      })
      .get();
    vlog(_log.info, "Waiting for cluster membership");
    controller->get_members_table()
      .local()
      .await_membership(config::node().node_id(), app_signal.abort_source())
      .get();
    _kafka_server.invoke_on_all(&net::server::start).get();
    // shutdown Kafka server input
    _deferred.emplace_back([this] {
        _kafka_server.invoke_on_all(&net::server::shutdown_input).get();
    });
    vlog(
      _log.info,
      "Started Kafka API server listening at {}",
      config::node().kafka_api());
}
