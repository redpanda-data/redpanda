// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "redpanda/application.h"

#include "archival/fwd.h"
#include "archival/ntp_archiver_service.h"
#include "archival/upload_controller.h"
#include "archival/upload_housekeeping_service.h"
#include "cli_parser.h"
#include "cloud_storage/cache_service.h"
#include "cloud_storage/remote.h"
#include "cloud_storage_clients/client_pool.h"
#include "cluster/bootstrap_service.h"
#include "cluster/cluster_discovery.h"
#include "cluster/cluster_utils.h"
#include "cluster/cluster_uuid.h"
#include "cluster/controller.h"
#include "cluster/ephemeral_credential_frontend.h"
#include "cluster/ephemeral_credential_service.h"
#include "cluster/fwd.h"
#include "cluster/id_allocator.h"
#include "cluster/id_allocator_frontend.h"
#include "cluster/members_manager.h"
#include "cluster/members_table.h"
#include "cluster/metadata_dissemination_handler.h"
#include "cluster/metadata_dissemination_service.h"
#include "cluster/node/local_monitor.h"
#include "cluster/node_isolation_watcher.h"
#include "cluster/node_status_rpc_handler.h"
#include "cluster/partition_balancer_rpc_handler.h"
#include "cluster/partition_manager.h"
#include "cluster/partition_recovery_manager.h"
#include "cluster/rm_partition_frontend.h"
#include "cluster/security_frontend.h"
#include "cluster/self_test_rpc_handler.h"
#include "cluster/service.h"
#include "cluster/tm_stm_cache_manager.h"
#include "cluster/topic_recovery_service.h"
#include "cluster/topic_recovery_status_frontend.h"
#include "cluster/topic_recovery_status_rpc_handler.h"
#include "cluster/topics_frontend.h"
#include "cluster/tx_gateway.h"
#include "cluster/tx_gateway_frontend.h"
#include "cluster/types.h"
#include "compression/async_stream_zstd.h"
#include "compression/stream_zstd.h"
#include "config/configuration.h"
#include "config/endpoint_tls_config.h"
#include "config/node_config.h"
#include "config/seed_server.h"
#include "coproc/api.h"
#include "coproc/partition_manager.h"
#include "features/feature_table_snapshot.h"
#include "features/fwd.h"
#include "kafka/client/configuration.h"
#include "kafka/server/coordinator_ntp_mapper.h"
#include "kafka/server/fetch_session_cache.h"
#include "kafka/server/group_manager.h"
#include "kafka/server/group_router.h"
#include "kafka/server/queue_depth_monitor.h"
#include "kafka/server/quota_manager.h"
#include "kafka/server/rm_group_frontend.h"
#include "kafka/server/server.h"
#include "kafka/server/snc_quota_manager.h"
#include "kafka/server/usage_manager.h"
#include "migrations/migrators.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "net/server.h"
#include "pandaproxy/rest/api.h"
#include "pandaproxy/schema_registry/api.h"
#include "raft/group_manager.h"
#include "raft/recovery_throttle.h"
#include "raft/service.h"
#include "redpanda/admin_server.h"
#include "resource_mgmt/io_priority.h"
#include "ssx/thread_worker.h"
#include "storage/backlog_controller.h"
#include "storage/chunk_cache.h"
#include "storage/compaction_controller.h"
#include "storage/directories.h"
#include "syschecks/syschecks.h"
#include "utils/file_io.h"
#include "utils/human.h"
#include "utils/uuid.h"
#include "version.h"
#include "vlog.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/prometheus.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/thread.hh>
#include <seastar/json/json_elements.hh>
#include <seastar/net/tls.hh>
#include <seastar/util/conversions.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/log.hh>

#include <sys/resource.h>
#include <sys/utsname.h>

#include <chrono>
#include <exception>
#include <memory>
#include <vector>

// This file in the data directory tracks the metadata
// needed to detect crash loops.
static constexpr std::string_view crash_loop_tracker_file = "startup_log";
// Crash tracking resets every 1h.
static constexpr model::timestamp_clock::duration crash_reset_duration{1h};

static void set_local_kafka_client_config(
  std::optional<kafka::client::configuration>& client_config,
  const config::node_config& config) {
    client_config.emplace();
    const auto& kafka_api = config.kafka_api.value();
    if (kafka_api.empty()) {
        // No Kafka listeners configured, cannot configure
        // a client.
        return;
    }
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

static void
set_sr_kafka_client_defaults(kafka::client::configuration& client_config) {
    if (!client_config.produce_batch_delay.is_overriden()) {
        client_config.produce_batch_delay.set_value(0ms);
    }
    if (!client_config.produce_batch_record_count.is_overriden()) {
        client_config.produce_batch_record_count.set_value(int32_t(0));
    }
    if (!client_config.produce_batch_size_bytes.is_overriden()) {
        client_config.produce_batch_size_bytes.set_value(int32_t(0));
    }
}

application::application(ss::sstring logger_name)
  : _log(std::move(logger_name)){};

application::~application() = default;

void application::shutdown() {
    // Stop accepting new requests.
    if (_kafka_server.local_is_initialized()) {
        _kafka_server.invoke_on_all(&net::server::shutdown_input).get();
    }
    if (_rpc.local_is_initialized()) {
        _rpc.invoke_on_all(&rpc::rpc_server::shutdown_input).get();
    }

    // We schedule shutting down controller input and aborting its operation as
    // one of the first shutdown steps. This way we terminate all long running
    // operations before shutting down the RPC server, preventing it from
    // waiting on background dispatch gate `close` call.
    if (controller) {
        controller->shutdown_input().get();
    }

    ss::do_for_each(
      _migrators,
      [](std::unique_ptr<features::feature_migrator>& fm) {
          return fm->stop();
      })
      .get();

    // Stop processing heartbeats before stopping the partition manager (and
    // the underlying Raft consensus instances). Otherwise we'd process
    // heartbeats for consensus objects that no longer exist.
    if (raft_group_manager.local_is_initialized()) {
        raft_group_manager.invoke_on_all(&raft::group_manager::stop_heartbeats)
          .get();
    }

    if (topic_recovery_service.local_is_initialized()) {
        topic_recovery_service
          .invoke_on_all(
            &cloud_storage::topic_recovery_service::shutdown_recovery)
          .get();
    }

    // Stop any I/O to object store: this will cause any readers in flight
    // to abort and enables partition shutdown to proceed reliably.
    if (cloud_storage_clients.local_is_initialized()) {
        cloud_storage_clients
          .invoke_on_all(
            &cloud_storage_clients::client_pool::shutdown_connections)
          .get();
    }

    // Stop all partitions before destructing the subsystems (transaction
    // coordinator, etc). This interrupts ongoing replication requests,
    // allowing higher level state machines to shutdown cleanly.
    if (partition_manager.local_is_initialized()) {
        partition_manager
          .invoke_on_all(&cluster::partition_manager::stop_partitions)
          .get();
    }
    if (cp_partition_manager.local_is_initialized()) {
        cp_partition_manager
          .invoke_on_all(&coproc::partition_manager::stop_partitions)
          .get();
    }

    // Wait for all requests to finish before destructing services that may be
    // used by pending requests.
    if (_kafka_server.local_is_initialized()) {
        _kafka_server.invoke_on_all(&net::server::wait_for_shutdown).get();
        _kafka_server.stop().get();
    }
    if (_kafka_conn_quotas.local_is_initialized()) {
        _kafka_conn_quotas.stop().get();
    }
    if (_rpc.local_is_initialized()) {
        _rpc.invoke_on_all(&rpc::rpc_server::wait_for_shutdown).get();
        _rpc.stop().get();
    }

    // Shut down services in reverse order to which they were registered.
    while (!_deferred.empty()) {
        _deferred.pop_back();
    }
}

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

namespace {

static constexpr std::string_view community_msg = R"banner(

Welcome to the Redpanda community!

Documentation: https://docs.redpanda.com - Product documentation site
GitHub Discussion: https://github.com/redpanda-data/redpanda/discussions - Longer, more involved discussions
GitHub Issues: https://github.com/redpanda-data/redpanda/issues - Report and track issues with the codebase
Support: https://support.redpanda.com - Contact the support team privately
Product Feedback: https://redpanda.com/feedback - Let us know how we can improve your experience
Slack: https://redpanda.com/slack - Chat about all things Redpanda. Join the conversation!
Twitter: https://twitter.com/redpandadata - All the latest Redpanda news!

)banner";

} // anonymous namespace

int application::run(int ac, char** av) {
    std::setvbuf(stdout, nullptr, _IOLBF, 1024);
    ss::app_template app(setup_app_config());
    app.add_options()("version", po::bool_switch(), "print version and exit");
    app.add_options()(
      "redpanda-cfg",
      po::value<std::string>(),
      ".yaml file config for redpanda");

    // Validate command line args using options registered by the app and
    // seastar. Keep the resulting variables in a temporary map so they don't
    // live for the lifetime of the application.
    {
        po::variables_map vm;
        if (!cli_parser{
              ac,
              av,
              cli_parser::app_opts{app.get_options_description()},
              cli_parser::ss_opts{app.get_conf_file_options_description()},
              _log}
               .validate_into(vm)) {
            return 1;
        }
        if (vm["version"].as<bool>()) {
            std::cout << redpanda_version() << std::endl;
            return 0;
        }
    }
    // use endl for explicit flushing
    std::cout << community_msg << std::endl;

    return app.run(ac, av, [this, &app] {
        vlog(_log.info, "Redpanda {}", redpanda_version());
        struct ::utsname buf;
        ::uname(&buf);
        vlog(
          _log.info,
          "kernel={}, nodename={}, machine={}",
          buf.release,
          buf.nodename,
          buf.machine);
        auto& cfg = app.configuration();
        log_system_resources(_log, cfg);
        // NOTE: we validate required args here instead of above because run()
        // catches some Seastar-specific args like --help that may result in
        // valid omissions of required args.
        validate_arguments(cfg);
        return ss::async([this, &cfg] {
            try {
                ::stop_signal app_signal;
                auto deferred = ss::defer([this] {
                    shutdown();
                    vlog(_log.info, "Shutdown complete.");
                });
                // must initialize configuration before services
                hydrate_config(cfg);
                initialize();
                check_environment();
                check_for_crash_loop();
                setup_metrics();
                wire_up_and_start(app_signal);
                post_start_tasks();
                app_signal.wait().get();
                vlog(_log.info, "Stopping...");
            } catch (const ss::abort_requested_exception&) {
                vlog(_log.info, "Redpanda startup aborted");
                return 0;
            } catch (...) {
                vlog(
                  _log.error,
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
    // Set up the abort_on_oom value based on the associated cluster config
    // property, and watch for changes.
    _abort_on_oom
      = config::shard_local_cfg().memory_abort_on_alloc_failure.bind();

    auto oom_config_watch = [this]() {
        const bool value = (*_abort_on_oom)();
        vlog(
          _log.info,
          "Setting abort_on_allocation_failure (abort on OOM): {}",
          value);
        ss::memory::set_abort_on_allocation_failure(value);
    };

    // execute the callback to apply the initial value
    oom_config_watch();

    _abort_on_oom->watch(oom_config_watch);

    /*
     * allocate per-core zstd decompression workspace and per-core
     * async_stream_zstd workspaces. it can be several megabytes in size, so do
     * it before memory becomes fragmented.
     */
    ss::smp::invoke_on_all([] {
        // TODO: remove this when stream_zstd is replaced with async_stream_zstd
        // in v/kafka
        compression::stream_zstd::init_workspace(
          config::shard_local_cfg().zstd_decompress_workspace_bytes());

        compression::initialize_async_stream_zstd(
          config::shard_local_cfg().zstd_decompress_workspace_bytes());
    }).get0();

    if (config::shard_local_cfg().enable_pid_file()) {
        syschecks::pidfile_create(config::node().pidfile_path());
    }
    smp_groups::config smp_groups_cfg{
      .raft_group_max_non_local_requests
      = config::shard_local_cfg().raft_smp_max_non_local_requests().value_or(
        smp_groups::default_raft_non_local_requests(
          config::shard_local_cfg().topic_partitions_per_shard()))};

    smp_service_groups.create_groups(smp_groups_cfg).get();
    _deferred.emplace_back(
      [this] { smp_service_groups.destroy_groups().get(); });

    if (groups) {
        _scheduling_groups = *groups;
        return;
    }

    _scheduling_groups.create_groups().get();
    _scheduling_groups_probe.wire_up(_scheduling_groups);
    _deferred.emplace_back([this] {
        _scheduling_groups_probe.clear();
        _scheduling_groups.destroy_groups().get();
    });

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
    setup_internal_metrics();
    setup_public_metrics();
}

void application::setup_public_metrics() {
    namespace sm = ss::metrics;

    if (config::shard_local_cfg().disable_public_metrics()) {
        return;
    }

    seastar::metrics::replicate_metric_families(
      seastar::metrics::default_handle(),
      {{"io_queue_total_read_ops", ssx::metrics::public_metrics_handle},
       {"io_queue_total_write_ops", ssx::metrics::public_metrics_handle},
       {"memory_allocated_memory", ssx::metrics::public_metrics_handle},
       {"memory_free_memory", ssx::metrics::public_metrics_handle}})
      .get();

    _public_metrics.start().get();

    const auto version_label = ssx::metrics::make_namespaced_label("version")(
      redpanda_git_version());
    const auto revision_label = ssx::metrics::make_namespaced_label("revision")(
      redpanda_git_revision());
    const auto build_labels = {version_label, revision_label};

    _public_metrics
      .invoke_on(
        ss::this_shard_id(),
        [build_labels](auto& public_metrics) {
            public_metrics.groups.add_group(
              "application",
              {sm::make_gauge(
                 "uptime_seconds_total",
                 [] {
                     return std::chrono::duration<double>(ss::engine().uptime())
                       .count();
                 },
                 sm::description("Redpanda uptime in seconds"))
                 .aggregate({sm::shard_label}),
               sm::make_gauge(
                 "build",
                 [] { return 1; },
                 sm::description("Redpanda build information"),
                 build_labels)
                 .aggregate({sm::shard_label})});
        })
      .get();

    _public_metrics
      .invoke_on_all([](auto& public_metrics) {
          public_metrics.groups.add_group(
            "cpu",
            {sm::make_gauge(
              "busy_seconds_total",
              [] {
                  return std::chrono::duration<double>(
                           ss::engine().total_busy_time())
                    .count();
              },
              sm::description("Total CPU busy time in seconds"))});
      })
      .get();

    _deferred.emplace_back([this] { _public_metrics.stop().get(); });
}

void application::setup_internal_metrics() {
    namespace sm = ss::metrics;

    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

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

    // Retain the original bytes loaded so that we can hexdump them later
    // if YAML Parse fails.
    // Related: https://github.com/redpanda-data/redpanda/issues/3798
    auto yaml_raw_bytes = read_fully_tmpbuf(cfg_path).get0();
    auto yaml_raw_str = ss::to_sstring(yaml_raw_bytes.clone());
    YAML::Node config;
    try {
        config = YAML::Load(yaml_raw_str);
    } catch (const YAML::ParserException& e) {
        // For most parse errors we do not want to do a binary dump.  For
        // "unknown escape character" we dump it, to debug issue #3789 where
        // apparently valid config files can cause this exception.
        if (e.msg.find("unknown escape character") != std::string::npos) {
            vlog(_log.error, "Dumping config on 'unknown escape character':");
            iobuf iob;
            iob.append(std::move(yaml_raw_bytes));

            // A reasonable config file is usually only hundreds of bytes.
            auto hexdump = iob.hexdump(16384);
            vlog(_log.error, "{}", hexdump);
        }

        throw;
    }

    auto config_printer = [this](std::string_view service, const auto& cfg) {
        std::vector<ss::sstring> items;
        cfg.for_each([&items, &service](const auto& item) {
            items.push_back(
              ssx::sformat("{}.{}\t- {}", service, item, item.desc()));
        });
        std::sort(items.begin(), items.end());
        for (const auto& item : items) {
            vlog(_log.info, "{}", item);
        }
    };

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
    config_printer("redpanda", config::shard_local_cfg());

    vlog(_log.info, "Node configuration properties:");
    vlog(_log.info, "(use `rpk redpanda config set <cfg> <value>` to change)");
    config_printer("redpanda", config::node());

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
        config_printer("pandaproxy", *_proxy_config);
        config_printer("pandaproxy_client", *_proxy_client_config);
    }
    if (config["schema_registry"]) {
        _schema_reg_config.emplace(config["schema_registry"]);
        if (config["schema_registry_client"]) {
            _schema_reg_client_config.emplace(config["schema_registry_client"]);
        } else {
            set_local_kafka_client_config(
              _schema_reg_client_config, config::node());
        }
        set_sr_kafka_client_defaults(*_schema_reg_client_config);
        config_printer("schema_registry", *_schema_reg_config);
        config_printer("schema_registry_client", *_schema_reg_client_config);
    }
}

void application::check_environment() {
    syschecks::systemd_message("checking environment (CPU, Mem)").get();
    syschecks::cpu();
    syschecks::memory(config::node().developer_mode());
    storage::directories::initialize(
      config::node().data_directory().as_sstring())
      .get();
    cloud_storage::cache::initialize(config::node().cloud_storage_cache_path())
      .get();

    if (config::shard_local_cfg().storage_strict_data_init()) {
        // Look for the special file that indicates a user intends
        // for the found data directory to be the one we use.
        auto strict_data_dir_file
          = config::node().strict_data_dir_file_path().string();
        auto file_exists = ss::file_exists(strict_data_dir_file).get();

        if (!file_exists) {
            throw std::invalid_argument(ssx::sformat(
              "Data directory not in expected state: {} not found, is the "
              "expected filesystem mounted?",
              strict_data_dir_file));
        }
    }
}

/// Here we check for too many consecutive unclean shutdowns/crashes
/// and abort the startup sequence if the limit exceeds
/// crash_loop_limit until the operator intervenes. Crash tracking
/// is reset if the node configuration changes or its been 1h since
/// the broker last failed to start. This metadata is tracked in the
/// tracker file. This is to prevent on disk state from piling up in
/// each unclean run and creating more state to recover for the next run.
void application::check_for_crash_loop() {
    auto file_path = config::node().data_directory().path
                     / crash_loop_tracker_file;
    std::optional<crash_tracker_metadata> maybe_crash_md;
    if (ss::file_exists(file_path.string()).get()) {
        // Ok to read the entire file, it contains a serialized uint32_t.
        auto buf = read_fully(file_path).get();
        try {
            maybe_crash_md = serde::from_iobuf<crash_tracker_metadata>(
              std::move(buf));
        } catch (const serde::serde_exception&) {
            // A malformed log file, ignore and reset it later.
            // We truncate it below.
            vlog(_log.warn, "Ignorning malformed tracker file {}", file_path);
        }
    }

    // Compute the checksum of the current node configuration.
    auto current_config
      = read_fully_to_string(config::node().get_cfg_file_path()).get0();
    auto checksum = xxhash_64(current_config.c_str(), current_config.length());

    if (maybe_crash_md) {
        auto& crash_md = maybe_crash_md.value();
        auto& limit = config::node().crash_loop_limit.value();

        // Check if it has been atleast 1h since last unsuccessful restart.
        // Tracking resets every 1h.
        auto time_since_last_start
          = model::duration_since_epoch(model::timestamp::now())
            - model::duration_since_epoch(crash_md._last_start_ts);

        auto crash_limit_ok = !limit || crash_md._crash_count <= limit.value();
        auto node_config_changed = crash_md._config_checksum != checksum;
        auto tracking_reset = time_since_last_start > crash_reset_duration;

        auto ok_to_proceed = crash_limit_ok || node_config_changed
                             || tracking_reset;

        if (!ok_to_proceed) {
            vlog(
              _log.error,
              "Crash loop detected. Too many consecutive crashes {}, exceeded "
              "{} configured value {}. To recover Redpanda from this state, "
              "manually remove file at path {}. Crash loop automatically "
              "resets 1h after last crash or with node configuration changes.",
              crash_md._crash_count,
              config::node().crash_loop_limit.name(),
              limit.value(),
              file_path);
            throw std::runtime_error("Crash loop detected, aborting startup.");
        }

        vlog(
          _log.debug,
          "Consecutive crashes detected: {} node config changed: {} "
          "time based tracking reset: {}",
          crash_md._crash_count,
          node_config_changed,
          tracking_reset);

        if (node_config_changed || tracking_reset) {
            crash_md._crash_count = 0;
        }
    }

    // Truncate and bump the crash count. We consider a run to be unclean by
    // default unless the scheduled cleanup (that runs very late in shutdown)
    // resets the file. See schedule_crash_tracker_file_cleanup().
    auto new_crash_count = maybe_crash_md
                             ? maybe_crash_md.value()._crash_count + 1
                             : 1;
    crash_tracker_metadata updated{
      ._crash_count = new_crash_count,
      ._config_checksum = checksum,
      ._last_start_ts = model::timestamp::now()};
    write_fully(file_path, serde::to_iobuf(updated)).get();
    ss::sync_directory(config::node().data_directory.value().as_sstring())
      .get();
}

void application::schedule_crash_tracker_file_cleanup() {
    // Schedule a deletion of the tracker file. On a clean shutdown,
    // the tracker file should be deleted thus reseting the crash count on the
    // next run. In case of an unclean shutdown, we already bumped
    // the crash count and that should be taken into account in the
    // next run.
    // We emplace it in the front to make it the last task to run.
    _deferred.emplace_front([&] {
        auto file = config::node().data_directory().path
                    / crash_loop_tracker_file;
        ss::remove_file(file.string()).get();
        ss::sync_directory(config::node().data_directory().as_sstring()).get();
        vlog(_log.debug, "Deleted crash loop tracker file: {}", file);
    });
}

static admin_server_cfg
admin_server_cfg_from_global_cfg(scheduling_groups& sgs) {
    return admin_server_cfg{
      .endpoints = config::node().admin(),
      .endpoints_tls = config::node().admin_api_tls(),
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
      std::ref(metadata_cache),
      std::ref(_connection_cache),
      std::ref(node_status_table),
      std::ref(self_test_frontend),
      std::ref(usage_manager),
      _proxy.get(),
      _schema_registry.get(),
      std::ref(topic_recovery_service),
      std::ref(topic_recovery_status_frontend))
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
      config::node().data_directory().as_sstring(),
      config::shard_local_cfg().log_segment_size.bind(),
      config::shard_local_cfg().compacted_log_segment_size.bind(),
      config::shard_local_cfg().max_compacted_log_segment_size.bind(),
      storage::jitter_percents(
        config::shard_local_cfg().log_segment_size_jitter_percent()),
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
void application::wire_up_runtime_services(model::node_id node_id) {
    wire_up_redpanda_services(node_id);
    if (_proxy_config) {
        construct_single_service(
          _proxy,
          smp_service_groups.proxy_smp_sg(),
          // TODO: Improve memory budget for services
          // https://github.com/redpanda-data/redpanda/issues/1392
          memory_groups::kafka_total_memory(),
          *_proxy_client_config,
          *_proxy_config,
          controller.get());
    }
    if (_schema_reg_config) {
        construct_single_service(
          _schema_registry,
          node_id,
          smp_service_groups.proxy_smp_sg(),
          // TODO: Improve memory budget for services
          // https://github.com/redpanda-data/redpanda/issues/1392
          memory_groups::kafka_total_memory(),
          *_schema_reg_client_config,
          *_schema_reg_config,
          std::reference_wrapper(controller));
    }
    construct_single_service(_monitor_unsafe_log_flag, std::ref(feature_table));

    configure_admin_server();
}

void application::wire_up_redpanda_services(model::node_id node_id) {
    ss::smp::invoke_on_all([] {
        resources::available_memory::local().register_metrics();
        return storage::internal::chunks().start();
    }).get();

    construct_single_service(thread_worker);

    // cluster
    syschecks::systemd_message("Initializing connection cache").get();
    construct_service(_connection_cache).get();
    syschecks::systemd_message("Building shard-lookup tables").get();
    construct_service(shard_table).get();

    syschecks::systemd_message("Intializing raft recovery throttle").get();
    recovery_throttle
      .start(ss::sharded_parameter([] {
          return config::shard_local_cfg().raft_learner_recovery_rate.bind();
      }))
      .get();

    syschecks::systemd_message("Intializing raft group manager").get();
    raft_group_manager
      .start(
        node_id,
        _scheduling_groups.raft_sg(),
        [] {
            return raft::group_manager::configuration{
              .heartbeat_interval
              = config::shard_local_cfg().raft_heartbeat_interval_ms.bind(),
              .heartbeat_timeout
              = config::shard_local_cfg().raft_heartbeat_timeout_ms.bind(),
              .raft_io_timeout_ms
              = config::shard_local_cfg().raft_io_timeout_ms.bind()};
        },
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
        std::ref(recovery_throttle),
        std::ref(feature_table))
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
        auto stop_config = ss::defer(
          [&cloud_configs] { cloud_configs.stop().get(); });
        cloud_configs
          .invoke_on_all([](cloud_storage::configuration& c) {
              return cloud_storage::configuration::get_config().then(
                [&c](cloud_storage::configuration cfg) { c = std::move(cfg); });
          })
          .get();
        construct_service(
          cloud_storage_clients,
          cloud_configs.local().connection_limit,
          ss::sharded_parameter(
            [&cloud_configs] { return cloud_configs.local().client_config; }),
          cloud_storage_clients::client_pool_overdraft_policy::borrow_if_empty)
          .get();
        construct_service(
          cloud_storage_api,
          std::ref(cloud_storage_clients),
          ss::sharded_parameter(
            [&cloud_configs] { return cloud_configs.local(); }))
          .get();
        cloud_storage_api.invoke_on_all(&cloud_storage::remote::start).get();

        construct_service(
          partition_recovery_manager,
          cloud_configs.local().bucket_name,
          std::ref(cloud_storage_api))
          .get();

        construct_service(
          _archival_upload_housekeeping,
          std::ref(cloud_storage_api),
          ss::sharded_parameter(
            [sg = _scheduling_groups.archival_upload()] { return sg; }))
          .get();
        _archival_upload_housekeeping
          .invoke_on_all(&archival::upload_housekeeping_service::start)
          .get();
    }

    syschecks::systemd_message("Creating tm_stm_cache_manager").get();

    construct_service(
      tm_stm_cache_manager,
      config::shard_local_cfg().transaction_coordinator_partitions())
      .get();

    syschecks::systemd_message("Adding partition manager").get();
    construct_service(
      partition_manager,
      std::ref(storage),
      std::ref(raft_group_manager),
      std::ref(tx_gateway_frontend),
      std::ref(partition_recovery_manager),
      std::ref(cloud_storage_api),
      std::ref(shadow_index_cache),
      ss::sharded_parameter(
        [sg = _scheduling_groups.archival_upload(),
         p = archival_priority(),
         enabled = archival_storage_enabled()]()
          -> ss::lw_shared_ptr<archival::configuration> {
            if (enabled) {
                return ss::make_lw_shared<archival::configuration>(
                  archival::get_archival_service_config(sg, p));
            } else {
                return nullptr;
            }
        }),
      std::ref(feature_table),
      std::ref(tm_stm_cache_manager),
      std::ref(_archival_upload_housekeeping),
      ss::sharded_parameter([] {
          return config::shard_local_cfg().max_concurrent_producer_ids.bind();
      }))
      .get();
    vlog(_log.info, "Partition manager started");

    construct_service(cp_partition_manager, std::ref(storage)).get();

    // controller
    syschecks::systemd_message("Creating cluster::controller").get();

    construct_single_service(
      controller,
      std::move(_config_preload),
      _connection_cache,
      partition_manager,
      shard_table,
      storage,
      local_monitor,
      std::ref(raft_group_manager),
      std::ref(feature_table),
      std::ref(cloud_storage_api));
    controller->wire_up().get0();

    construct_single_service_sharded(
      self_test_backend,
      node_id,
      std::ref(local_monitor),
      std::ref(_connection_cache),
      _scheduling_groups.self_test_sg())
      .get();

    construct_single_service_sharded(
      self_test_frontend,
      node_id,
      std::ref(controller->get_members_table()),
      std::ref(self_test_backend),
      std::ref(_connection_cache))
      .get();

    construct_service(node_status_table, node_id).get();

    construct_single_service_sharded(
      node_status_backend,
      node_id,
      std::ref(controller->get_members_table()),
      std::ref(feature_table),
      std::ref(node_status_table),
      ss::sharded_parameter(
        [] { return config::shard_local_cfg().node_status_interval.bind(); }))
      .get();

    syschecks::systemd_message("Creating kafka metadata cache").get();
    construct_service(
      metadata_cache,
      std::ref(controller->get_topics_state()),
      std::ref(controller->get_members_table()),
      std::ref(controller->get_partition_leaders()),
      std::ref(controller->get_health_monitor()))
      .get();

    syschecks::systemd_message("Creating isolation node watcher").get();
    construct_single_service(
      _node_isolation_watcher,
      metadata_cache,
      controller->get_health_monitor(),
      node_status_table);

    // metrics and quota management
    syschecks::systemd_message("Adding kafka quota managers").get();
    construct_service(quota_mgr).get();
    construct_service(snc_quota_mgr).get();

    syschecks::systemd_message("Creating metadata dissemination service").get();
    construct_service(
      md_dissemination_service,
      std::ref(raft_group_manager),
      std::ref(partition_manager),
      std::ref(controller->get_partition_leaders()),
      std::ref(controller->get_members_table()),
      std::ref(controller->get_topics_state()),
      std::ref(_connection_cache),
      std::ref(controller->get_health_monitor()),
      std::ref(feature_table))
      .get();

    if (archival_storage_enabled()) {
        syschecks::systemd_message("Starting shadow indexing cache").get();
        auto redpanda_dir = config::node().data_directory.value();
        construct_service(
          shadow_index_cache,
          config::node().cloud_storage_cache_path(),
          ss::sharded_parameter([] {
              return config::shard_local_cfg().cloud_storage_cache_size.bind();
          }))
          .get();

        // Hook up local_monitor to update storage_resources when disk state
        // changes
        auto cloud_storage_cache_disk_notification
          = storage_node.local().register_disk_notification(
            storage::node_api::disk_type::cache,
            [this](
              uint64_t total_space,
              uint64_t free_space,
              storage::disk_space_alert alert) {
                return shadow_index_cache.local().notify_disk_status(
                  total_space, free_space, alert);
            });
        _deferred.emplace_back([this, cloud_storage_cache_disk_notification] {
            storage_node.local().unregister_disk_notification(
              storage::node_api::disk_type::cache,
              cloud_storage_cache_disk_notification);
        });

        shadow_index_cache
          .invoke_on_all(
            [](cloud_storage::cache& cache) { return cache.start(); })
          .get();

        construct_service(
          _archival_upload_controller,
          std::ref(partition_manager),
          make_upload_controller_config(_scheduling_groups.archival_upload()))
          .get();

        construct_service(
          topic_recovery_status_frontend,
          node_id,
          std::ref(_connection_cache),
          std::ref(controller->get_members_table()))
          .get();

        construct_service(
          topic_recovery_service,
          std::ref(cloud_storage_api),
          std::ref(controller->get_topics_state()),
          std::ref(controller->get_topics_frontend()),
          std::ref(topic_recovery_status_frontend))
          .get();

        partition_recovery_manager
          .invoke_on_all(
            [this](cloud_storage::partition_recovery_manager& prm) {
                prm.set_topic_recovery_components(
                  topic_recovery_status_frontend, topic_recovery_service);
            })
          .get();
    }

    // group membership
    syschecks::systemd_message("Creating kafka group manager").get();
    construct_service(
      _group_manager,
      model::kafka_consumer_offsets_nt,
      std::ref(raft_group_manager),
      std::ref(partition_manager),
      std::ref(controller->get_topics_state()),
      std::ref(tx_gateway_frontend),
      std::ref(controller->get_feature_table()),
      &kafka::make_consumer_offsets_serializer,
      kafka::enable_group_metrics::yes)
      .get();
    syschecks::systemd_message("Creating kafka group shard mapper").get();
    construct_service(
      coordinator_ntp_mapper,
      std::ref(metadata_cache),
      model::kafka_consumer_offsets_nt)
      .get();
    syschecks::systemd_message("Creating kafka group router").get();
    construct_service(
      group_router,
      _scheduling_groups.kafka_sg(),
      smp_service_groups.kafka_smp_sg(),
      std::ref(_group_manager),
      std::ref(shard_table),
      std::ref(coordinator_ntp_mapper))
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

    syschecks::systemd_message("Creating kafka usage manager frontend").get();
    construct_service(
      usage_manager,
      std::ref(controller->get_health_monitor()),
      std::ref(storage))
      .get();

    syschecks::systemd_message("Creating tx coordinator mapper").get();
    construct_service(
      tx_coordinator_ntp_mapper, std::ref(metadata_cache), model::tx_manager_nt)
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
      std::ref(rm_partition_frontend),
      std::ref(feature_table),
      std::ref(tm_stm_cache_manager),
      std::ref(tx_coordinator_ntp_mapper))
      .get();
    _kafka_conn_quotas
      .start([]() {
          return net::conn_quota_config{
            .max_connections
            = config::shard_local_cfg().kafka_connections_max.bind(),
            .max_connections_per_ip
            = config::shard_local_cfg().kafka_connections_max_per_ip.bind(),
            .max_connections_overrides
            = config::shard_local_cfg().kafka_connections_max_overrides.bind(),
          };
      })
      .get();

    ss::sharded<net::server_configuration> kafka_cfg;
    kafka_cfg.start(ss::sstring("kafka_rpc")).get();
    auto kafka_cfg_cleanup = ss::defer(
      [&kafka_cfg]() { kafka_cfg.stop().get(); });
    kafka_cfg
      .invoke_on_all([this](net::server_configuration& c) {
          return ss::async([this, &c] {
              c.conn_quotas = std::ref(_kafka_conn_quotas);
              c.max_service_memory_per_core
                = memory_groups::kafka_total_memory();
              c.listen_backlog
                = config::shard_local_cfg().rpc_server_listen_backlog;
              if (config::shard_local_cfg().kafka_rpc_server_tcp_recv_buf()) {
                  c.tcp_recv_buf
                    = config::shard_local_cfg().kafka_rpc_server_tcp_recv_buf;
              } else {
                  // Backward compat: prior to Redpanda 22.2, rpc_server_*
                  // settings applied to both Kafka and Internal RPC listeners.
                  c.tcp_recv_buf
                    = config::shard_local_cfg().rpc_server_tcp_recv_buf;
              };
              if (config::shard_local_cfg().kafka_rpc_server_tcp_send_buf()) {
                  c.tcp_send_buf
                    = config::shard_local_cfg().kafka_rpc_server_tcp_send_buf;
              } else {
                  // Backward compat: prior to Redpanda 22.2, rpc_server_*
                  // settings applied to both Kafka and Internal RPC listeners.
                  c.tcp_send_buf
                    = config::shard_local_cfg().rpc_server_tcp_send_buf;
              }

              c.stream_recv_buf
                = config::shard_local_cfg().kafka_rpc_server_stream_recv_buf;
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
              c.disable_public_metrics = net::public_metrics_disabled(
                config::shard_local_cfg().disable_public_metrics());

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
    syschecks::systemd_message("Starting kafka RPC {}", kafka_cfg.local())
      .get();
    _kafka_server
      .start(
        &kafka_cfg,
        smp_service_groups.kafka_smp_sg(),
        std::ref(metadata_cache),
        std::ref(controller->get_topics_frontend()),
        std::ref(controller->get_config_frontend()),
        std::ref(controller->get_feature_table()),
        std::ref(quota_mgr),
        std::ref(snc_quota_mgr),
        std::ref(group_router),
        std::ref(usage_manager),
        std::ref(shard_table),
        std::ref(partition_manager),
        std::ref(fetch_session_cache),
        std::ref(id_allocator_frontend),
        std::ref(controller->get_credential_store()),
        std::ref(controller->get_authorizer()),
        std::ref(controller->get_security_frontend()),
        std::ref(controller->get_api()),
        std::ref(tx_gateway_frontend),
        std::ref(cp_partition_manager),
        qdc_config,
        std::ref(*thread_worker))
      .get();
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
    return _proxy->set_config(std::move(name), std::move(val));
}

bool application::archival_storage_enabled() {
    const auto& cfg = config::shard_local_cfg();
    return cfg.cloud_storage_enabled();
}

ss::future<>
application::set_proxy_client_config(ss::sstring name, std::any val) {
    return _proxy->set_client_config(std::move(name), std::move(val));
}

void application::wire_up_bootstrap_services() {
    // Wire up local storage.
    ss::smp::invoke_on_all([] {
        return storage::internal::chunks().start();
    }).get();
    syschecks::systemd_message("Constructing storage services").get();
    construct_single_service_sharded(storage_node).get();
    construct_single_service_sharded(
      local_monitor,
      config::shard_local_cfg().storage_space_alert_free_threshold_bytes.bind(),
      config::shard_local_cfg()
        .storage_space_alert_free_threshold_percent.bind(),
      config::shard_local_cfg().storage_min_free_bytes.bind(),
      config::node().data_directory().as_sstring(),
      config::node().cloud_storage_cache_path().string(),
      std::ref(storage_node),
      std::ref(storage))
      .get();

    construct_service(
      storage,
      []() { return kvstore_config_from_global_config(); },
      [this]() {
          auto log_cfg = manager_config_from_global_config(_scheduling_groups);
          log_cfg.reclaim_opts.background_reclaimer_sg
            = _scheduling_groups.cache_background_reclaim_sg();
          return log_cfg;
      },
      std::ref(feature_table))
      .get();

    // Hook up local_monitor to update storage_resources when disk state changes
    auto storage_disk_notification
      = storage_node.local().register_disk_notification(
        storage::node_api::disk_type::data,
        [this](
          uint64_t total_space,
          uint64_t free_space,
          storage::disk_space_alert) {
            return storage.invoke_on_all(
              [total_space, free_space](storage::api& api) {
                  api.resources().update_allowance(total_space, free_space);
              });
        });
    _deferred.emplace_back([this, storage_disk_notification] {
        storage_node.local().unregister_disk_notification(
          storage::node_api::disk_type::data, storage_disk_notification);
    });

    // Start empty, populated from snapshot in start_bootstrap_services
    syschecks::systemd_message("Creating feature table").get();
    construct_service(feature_table).get();

    // Wire up the internal RPC server.
    ss::sharded<net::server_configuration> rpc_cfg;
    rpc_cfg.start(ss::sstring("internal_rpc")).get();
    auto stop_cfg = ss::defer([&rpc_cfg] { rpc_cfg.stop().get(); });
    rpc_cfg
      .invoke_on_all([this](net::server_configuration& c) {
          return ss::async([this, &c] {
              auto rpc_server_addr
                = net::resolve_dns(config::node().rpc_server()).get0();
              // Use port based load_balancing_algorithm to make connection
              // shard assignment deterministic.
              c.load_balancing_algo
                = ss::server_socket::load_balancing_algorithm::port;
              c.max_service_memory_per_core = memory_groups::rpc_total_memory();
              c.disable_metrics = net::metrics_disabled(
                config::shard_local_cfg().disable_metrics());
              c.disable_public_metrics = net::public_metrics_disabled(
                config::shard_local_cfg().disable_public_metrics());
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

    syschecks::systemd_message(
      "Constructing internal RPC services {}", rpc_cfg.local())
      .get();
    _rpc.start(&rpc_cfg).get();
}

void application::start_bootstrap_services() {
    syschecks::systemd_message("Starting storage services").get();

    // single instance
    storage_node.invoke_on_all(&storage::node_api::start).get0();
    local_monitor.invoke_on_all(&cluster::node::local_monitor::start).get0();

    storage.invoke_on_all(&storage::api::start).get();

    // As soon as storage is up, load our feature_table snapshot, if any,
    // so that all other services may rely on having features activated as soon
    // as they start.
    load_feature_table_snapshot();

    // Before we start up our bootstrapping RPC service, load any relevant
    // on-disk state we may need: existing cluster UUID, node ID, etc.
    if (std::optional<iobuf> cluster_uuid_buf = storage.local().kvs().get(
          cluster::cluster_uuid_key_space, bytes(cluster::cluster_uuid_key));
        cluster_uuid_buf) {
        const auto cluster_uuid = model::cluster_uuid{
          serde::from_iobuf<uuid_t>(std::move(*cluster_uuid_buf))};
        storage
          .invoke_on_all([&cluster_uuid](storage::api& storage) {
              storage.set_cluster_uuid(cluster_uuid);
          })
          .get();
    }

    // If the feature table is blank, and we have not yet joined a cluster,
    // then assume we are about to join a cluster or form a new one, and
    // fast-forward the feature table before we do any network operations:
    // this way features like rpc_v2_by_default will be present before the
    // first network I/O we do.
    //
    // Absence of a cluster_uuid is not evidence of not having joined a cluster,
    // because we might have joined via an earlier version of redpanda, and
    // just upgraded to a version that stores cluster and node UUIDs.  We must
    // also check for an controller log state on disk.
    //
    // Ordering: bootstrap_backend writes a feature table snapshot _before_
    // persisting the cluster UUID to kvstore, so if restart in the middle,
    // we will hit this path again: this is important to avoid ever starting
    // network requests before we have reached a defined cluster version.

    auto controller_log_exists = storage.local()
                                   .kvs()
                                   .get(
                                     storage::kvstore::key_space::consensus,
                                     raft::details::serialize_group_key(
                                       raft::group_id{0},
                                       raft::metadata_key::config_map))
                                   .has_value();

    if (
      feature_table.local().get_active_version() == cluster::invalid_version
      && !storage.local().get_cluster_uuid().has_value()
      && !controller_log_exists) {
        feature_table
          .invoke_on_all([](features::feature_table& ft) {
              ft.bootstrap_active_version(
                features::feature_table::get_earliest_logical_version(),
                features::feature_table::version_durability::ephemeral);

              // We do _not_ write a snapshot here: the persistent record of
              // feature table state is only set for the first time in
              // bootstrap_backend (or feature_backend).  This is important,
              // so that someone who starts a too-new Redpanda that can't join
              // their cluster can easily stop it and run an older version,
              // before we've committed any version info to disk.
          })
          .get0();
    }

    static const bytes invariants_key("configuration_invariants");
    auto configured_node_id = config::node().node_id();
    if (auto invariants_buf = storage.local().kvs().get(
          storage::kvstore::key_space::controller, invariants_key);
        invariants_buf) {
        auto invariants
          = reflection::from_iobuf<cluster::configuration_invariants>(
            std::move(*invariants_buf));
        const auto& stored_node_id = invariants.node_id;
        vlog(_log.info, "Loaded stored node ID for node: {}", stored_node_id);
        if (
          configured_node_id != std::nullopt
          && *configured_node_id != stored_node_id) {
            throw std::invalid_argument(ssx::sformat(
              "Configured node ID {} doesn't match stored node ID {}",
              *configured_node_id,
              stored_node_id));
        }
        ss::smp::invoke_on_all([stored_node_id] {
            config::node().node_id.set_value(
              std::make_optional(stored_node_id));
        }).get0();
    }

    // Load the local node UUID, or create one if none exists.
    auto& kvs = storage.local().kvs();
    static const bytes node_uuid_key = "node_uuid";
    model::node_uuid node_uuid;
    auto node_uuid_buf = kvs.get(
      storage::kvstore::key_space::controller, node_uuid_key);
    if (node_uuid_buf) {
        node_uuid = serde::from_iobuf<model::node_uuid>(
          std::move(*node_uuid_buf));
        vlog(
          _log.info,
          "Loaded existing UUID for node: {}",
          model::node_uuid(node_uuid));
    } else {
        node_uuid = model::node_uuid(uuid_t::create());
        vlog(_log.info, "Generated new UUID for node: {}", node_uuid);
        kvs
          .put(
            storage::kvstore::key_space::controller,
            node_uuid_key,
            serde::to_iobuf(node_uuid))
          .get();
    }
    storage
      .invoke_on_all([node_uuid](storage::api& storage) mutable {
          storage.set_node_uuid(node_uuid);
      })
      .get();

    syschecks::systemd_message("Starting internal RPC bootstrap service").get();
    _rpc
      .invoke_on_all([this](rpc::rpc_server& s) {
          std::vector<std::unique_ptr<rpc::service>> bootstrap_service;
          bootstrap_service.push_back(
            std::make_unique<cluster::bootstrap_service>(
              _scheduling_groups.cluster_sg(),
              smp_service_groups.cluster_smp_sg(),
              std::ref(storage)));
          s.add_services(std::move(bootstrap_service));
      })
      .get();
    _rpc.invoke_on_all(&rpc::rpc_server::start).get();
    vlog(
      _log.info,
      "Started RPC server listening at {}",
      config::node().rpc_server());
}

void application::wire_up_and_start(::stop_signal& app_signal, bool test_mode) {
    wire_up_bootstrap_services();
    start_bootstrap_services();

    // Begin the cluster discovery manager so we can confirm our initial node
    // ID. A valid node ID is required before we can initialize the rest of our
    // subsystems.
    const auto& node_uuid = storage.local().node_uuid();
    cluster::cluster_discovery cd(
      node_uuid, storage.local(), app_signal.abort_source());
    auto node_id = cd.determine_node_id().get();
    if (config::node().node_id() == std::nullopt) {
        // If we previously didn't have a node ID, set it in the config. We
        // will persist it in the kvstore when the controller starts up.
        ss::smp::invoke_on_all([node_id] {
            config::node().node_id.set_value(
              std::make_optional<model::node_id>(node_id));
        }).get();
    }

    vlog(
      _log.info,
      "Starting Redpanda with node_id {}, cluster UUID {}",
      node_id,
      storage.local().get_cluster_uuid());

    wire_up_runtime_services(node_id);

    if (test_mode) {
        // When running inside a unit test fixture, we may fast-forward
        // some of initialization that would usually wait for the controller
        // to commit some state to its log.
        vlog(_log.warn, "Running in unit test mode");
        if (
          feature_table.local().get_active_version()
          == cluster::invalid_version) {
            vlog(_log.info, "Switching on all features");
            feature_table
              .invoke_on_all(
                [](features::feature_table& ft) { ft.testing_activate_all(); })
              .get();
        }
    } else {
        // Only populate migrators in non-unit-test mode
        _migrators.push_back(
          std::make_unique<features::migrators::cloud_storage_config>(
            *controller));
    }

    if (cd.is_cluster_founder().get()) {
        controller->set_ready().get();
    }

    start_runtime_services(cd, app_signal);

    if (_proxy_config) {
        _proxy->start().get();
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

    start_kafka(node_id, app_signal);
    controller->set_ready().get();
    _admin.invoke_on_all([](admin_server& admin) { admin.set_ready(); }).get();
    _monitor_unsafe_log_flag->start().get();

    vlog(_log.info, "Successfully started Redpanda!");
    syschecks::systemd_notify_ready().get();
}

void application::start_runtime_services(
  cluster::cluster_discovery& cd, ::stop_signal& app_signal) {
    ssx::background = feature_table.invoke_on_all(
      [this](features::feature_table& ft) {
          return ft.await_feature_then(
            features::feature::rpc_transport_unknown_errc, [this] {
                if (ss::this_shard_id() == 0) {
                    vlog(
                      _log.debug, "All nodes support unknown RPC error codes");
                }
                // Redpanda versions <= v22.3.x don't properly parse error
                // codes they don't know about.
                _rpc.local().set_use_service_unavailable();
            });
      });

    thread_worker->start().get();

    // single instance
    node_status_backend.invoke_on_all(&cluster::node_status_backend::start)
      .get();
    syschecks::systemd_message("Starting the partition manager").get();
    partition_manager.invoke_on_all(&cluster::partition_manager::start).get();

    syschecks::systemd_message("Starting the coproc partition manager").get();
    cp_partition_manager.invoke_on_all(&coproc::partition_manager::start).get();

    syschecks::systemd_message("Starting Raft group manager").get();
    raft_group_manager.invoke_on_all(&raft::group_manager::start).get();

    syschecks::systemd_message("Starting Kafka group manager").get();
    _group_manager.invoke_on_all(&kafka::group_manager::start).get();

    // Initialize the Raft RPC endpoint before the rest of the runtime RPC
    // services so the cluster seeds can elect a leader and write a cluster
    // UUID before proceeding with the rest of bootstrap.
    const bool start_raft_rpc_early = cd.is_cluster_founder().get();
    if (start_raft_rpc_early) {
        syschecks::systemd_message("Starting RPC/raft").get();
        _rpc
          .invoke_on_all([this](rpc::rpc_server& s) {
              std::vector<std::unique_ptr<rpc::service>> runtime_services;
              runtime_services.push_back(std::make_unique<raft::service<
                                           cluster::partition_manager,
                                           cluster::shard_table>>(
                _scheduling_groups.raft_sg(),
                smp_service_groups.raft_smp_sg(),
                partition_manager,
                shard_table.local(),
                config::shard_local_cfg().raft_heartbeat_interval_ms()));
              s.add_services(std::move(runtime_services));
          })
          .get();
    }
    syschecks::systemd_message("Starting controller").get();
    controller->start(cd, app_signal.abort_source()).get0();

    // FIXME: in first patch explain why this is started after the
    // controller so the broker set will be available. Then next patch fix.
    syschecks::systemd_message("Starting metadata dissination service").get();
    md_dissemination_service
      .invoke_on_all(&cluster::metadata_dissemination_service::start)
      .get();

    syschecks::systemd_message("Starting RPC").get();
    _rpc
      .invoke_on_all([this, start_raft_rpc_early](rpc::rpc_server& s) {
          std::vector<std::unique_ptr<rpc::service>> runtime_services;
          runtime_services.push_back(std::make_unique<cluster::id_allocator>(
            _scheduling_groups.raft_sg(),
            smp_service_groups.raft_smp_sg(),
            std::ref(id_allocator_frontend)));
          // _rm_group_proxy is wrap around a sharded service with only
          // `.local()' access so it's ok to share without foreign_ptr
          runtime_services.push_back(std::make_unique<cluster::tx_gateway>(
            _scheduling_groups.raft_sg(),
            smp_service_groups.raft_smp_sg(),
            std::ref(tx_gateway_frontend),
            _rm_group_proxy.get(),
            std::ref(rm_partition_frontend)));

          if (!start_raft_rpc_early) {
              runtime_services.push_back(std::make_unique<raft::service<
                                           cluster::partition_manager,
                                           cluster::shard_table>>(
                _scheduling_groups.raft_sg(),
                smp_service_groups.raft_smp_sg(),
                partition_manager,
                shard_table.local(),
                config::shard_local_cfg().raft_heartbeat_interval_ms()));
          }

          runtime_services.push_back(std::make_unique<cluster::service>(
            _scheduling_groups.cluster_sg(),
            smp_service_groups.cluster_smp_sg(),
            std::ref(controller->get_topics_frontend()),
            std::ref(controller->get_members_manager()),
            std::ref(metadata_cache),
            std::ref(controller->get_security_frontend()),
            std::ref(controller->get_api()),
            std::ref(controller->get_members_frontend()),
            std::ref(controller->get_config_frontend()),
            std::ref(controller->get_config_manager()),
            std::ref(controller->get_feature_manager()),
            std::ref(controller->get_feature_table()),
            std::ref(controller->get_health_monitor()),
            std::ref(_connection_cache),
            std::ref(controller->get_partition_manager())));

          runtime_services.push_back(
            std::make_unique<cluster::metadata_dissemination_handler>(
              _scheduling_groups.cluster_sg(),
              smp_service_groups.cluster_smp_sg(),
              std::ref(controller->get_partition_leaders())));

          runtime_services.push_back(
            std::make_unique<cluster::node_status_rpc_handler>(
              _scheduling_groups.node_status(),
              smp_service_groups.cluster_smp_sg(),
              std::ref(node_status_backend)));

          runtime_services.push_back(
            std::make_unique<cluster::self_test_rpc_handler>(
              _scheduling_groups.node_status(),
              smp_service_groups.cluster_smp_sg(),
              std::ref(self_test_backend)));

          runtime_services.push_back(
            std::make_unique<cluster::partition_balancer_rpc_handler>(
              _scheduling_groups.cluster_sg(),
              smp_service_groups.cluster_smp_sg(),
              std::ref(controller->get_partition_balancer())));

          runtime_services.push_back(
            std::make_unique<cluster::ephemeral_credential_service>(
              _scheduling_groups.cluster_sg(),
              smp_service_groups.cluster_smp_sg(),
              std::ref(controller->get_ephemeral_credential_frontend())));

          runtime_services.push_back(
            std::make_unique<cluster::topic_recovery_status_rpc_handler>(
              _scheduling_groups.cluster_sg(),
              smp_service_groups.cluster_smp_sg(),
              std::ref(topic_recovery_service)));
          s.add_services(std::move(runtime_services));

          // Done! Disallow unknown method errors.
          s.set_all_services_added();
      })
      .get();

    syschecks::systemd_message("Starting node isolation watcher").get();
    _node_isolation_watcher->start();

    // After we have started internal RPC listener, we may join
    // the cluster (if we aren't already a member)
    controller->get_members_manager()
      .invoke_on(
        cluster::members_manager::shard,
        &cluster::members_manager::join_cluster)
      .get();

    quota_mgr.invoke_on_all(&kafka::quota_manager::start).get();
    snc_quota_mgr.invoke_on_all(&kafka::snc_quota_manager::start).get();
    usage_manager.invoke_on_all(&kafka::usage_manager::start).get();

    if (!config::node().admin().empty()) {
        _admin.invoke_on_all(&admin_server::start).get0();
    }

    _compaction_controller.invoke_on_all(&storage::compaction_controller::start)
      .get();
    _archival_upload_controller
      .invoke_on_all(&archival::upload_controller::start)
      .get();

    for (const auto& m : _migrators) {
        m->start(controller->get_abort_source().local());
    }
}

/**
 * The Kafka protocol listener startup is separate to the rest of Redpanda,
 * because it includes a wait for this node to be a full member of a redpanda
 * cluster -- this is expected to be run last, after everything else is
 * started.
 */
void application::start_kafka(
  const model::node_id& node_id, ::stop_signal& app_signal) {
    // Kafka API
    // The Kafka listener is intentionally the last thing we start: during
    // this phase we will wait for the node to be a cluster member before
    // proceeding, because it is not helpful to clients for us to serve
    // kafka requests before we have up to date knowledge of the system.
    vlog(_log.info, "Waiting for cluster membership");
    controller->get_members_table()
      .local()
      .await_membership(node_id, app_signal.abort_source())
      .get();
    _kafka_server.invoke_on_all(&net::server::start).get();
    vlog(
      _log.info,
      "Started Kafka API server listening at {}",
      config::node().kafka_api());
}

/**
 * Feature table is generally updated via controller, but we need it to
 * be initialized very early in startup so that other subsystems (including
 * e.g. the controller raft group) may rely on up to date knowledge of which
 * feature bits are enabled.
 */
void application::load_feature_table_snapshot() {
    auto val_bytes_opt = storage.local().kvs().get(
      storage::kvstore::key_space::controller,
      features::feature_table_snapshot::kvstore_key());

    if (!val_bytes_opt) {
        // No snapshot?  Probably we are yet to join cluster.
        return;
    }

    features::feature_table_snapshot snap;
    try {
        snap = serde::from_iobuf<features::feature_table_snapshot>(
          std::move(*val_bytes_opt));
    } catch (...) {
        // Do not block redpanda from starting if there is something invalid
        // here: the feature table should get replayed eventually via
        // the controller.
        vlog(
          _log.error,
          "Exception decoding feature table snapshot: {}",
          std::current_exception());
#ifndef NDEBUG
        vassert(false, "Snapshot decode failed");
#endif
        return;
    }

    auto my_version = features::feature_table::get_latest_logical_version();
    if (my_version < snap.version) {
        vlog(
          _log.error,
          "Incompatible downgrade detected!  My version {}, feature table {} "
          "indicates that all nodes in cluster were previously >= that version",
          my_version,
          snap.version);
        // From this point, it is undefined to whether this process will be able
        // to decode anything it sees on the network or on disk.
        //
        // This case will have stricter enforcement in future, to protect the
        // user from acccidentally getting a cluster into a broken state by
        // downgrading too far:
        // https://github.com/redpanda-data/redpanda/issues/7018
#ifndef NDEBUG
        vassert(my_version >= snap.version, "Incompatible downgrade detected");
#endif
    } else {
        vlog(
          _log.debug,
          "Loaded feature table snapshot at cluster version {} (vs my binary "
          "{})",
          snap.version,
          my_version);
    }

    feature_table
      .invoke_on_all([snap](features::feature_table& ft) { snap.apply(ft); })
      .get();

    // Having loaded a snapshot, do our strict check for version compat.
    feature_table.local().assert_compatible_version(
      config::node().upgrade_override_checks);
}

/**
 * Contains tasks that should only run after all other services have been
 * initialized and started.
 */
void application::post_start_tasks() {
    // This warning is set after we start RP since we want to allow
    // services to make large allocations if need be during startup.
    auto warning_threshold
      = config::node().memory_allocation_warning_threshold();
    if (warning_threshold.has_value()) {
        ss::smp::invoke_on_all([threshold = warning_threshold.value()] {
            ss::memory::set_large_allocation_warning_threshold(threshold);
        }).get();
    }

    // We schedule the deletion _after_ the application fully
    // starts up. This ensures that any errors like
    // misconfigurations are also treated as unclean shutdowns
    // thus avoiding crashloops.
    schedule_crash_tracker_file_cleanup();
}
