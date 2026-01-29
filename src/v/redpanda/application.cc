// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "redpanda/application.h"

#include "absl/log/globals.h"
#include "base/vlog.h"
#include "cli_parser.h"
#include "cloud_io/cache_service.h"
#include "cloud_storage_clients/client_pool.h"
#include "cluster/cloud_metadata/offsets_upload_router.h"
#include "cluster/cloud_metadata/offsets_uploader.h"
#include "cluster/config_manager.h"
#include "cluster/controller.h"
#include "cluster/node_isolation_watcher.h"
#include "cluster/topic_recovery_service.h"
#include "compression/async_stream_zstd.h"
#include "compression/lz4_decompression_buffers.h"
#include "compression/stream_zstd.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "crash_tracker/signals.h"
#include "datalake/coordinator/coordinator_manager.h"
#include "datalake/credential_manager.h"
#include "datalake/datalake_manager.h"
#include "datalake/datalake_usage_aggregator.h"
#include "kafka/client/configuration.h"
#include "kafka/server/rm_group_frontend.h"
#include "metrics/prometheus_sanitize.h"
#include "migrations/migrators.h"
#include "pandaproxy/rest/api.h"
#include "pandaproxy/rest/configuration.h"
#include "pandaproxy/schema_registry/api.h"
#include "resource_mgmt/cpu_profiler.h"
#include "resource_mgmt/memory_groups.h"
#include "resource_mgmt/memory_sampling.h"
#include "resource_mgmt/scheduling_groups_probe.h"
#include "resource_mgmt/smp_groups.h"
#include "security/audit/audit_log_manager.h"
#include "storage/api.h"
#include "storage/directories.h"
#include "syschecks/syschecks.h"
#include "utils/file_io.h"
#include "utils/human.h"
#include "version/version.h"
#include "wasm/cache.h"

#include <seastar/core/memory.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/prometheus.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>

#if __has_include(<google/protobuf/runtime_version.h>)
#include <google/protobuf/runtime_version.h>
#endif
#if __has_include(<google/protobuf/stubs/logging.h>)
#include <google/protobuf/stubs/logging.h>
#endif
#include <sys/resource.h>
#include <sys/utsname.h>

#include <algorithm>
#include <chrono>
#include <exception>
#include <memory>
#include <vector>

// Forward declarations for helper functions defined in application_config.cc
void set_local_kafka_client_config(
  std::optional<kafka::client::configuration>& client_config,
  const config::node_config& config);

void set_pp_kafka_client_defaults(
  pandaproxy::rest::configuration& proxy_config,
  kafka::client::configuration& client_config);

void set_sr_kafka_client_defaults(kafka::client::configuration& client_config);

void set_auditing_kafka_client_defaults(
  kafka::client::configuration& client_config);

void log_system_resources(
  ss::logger& log, const boost::program_options::variables_map& cfg);

application::application(ss::sstring logger_name)
  : ssx::sharded_service_container(logger_name) {}

application::~application() {}

void application::shutdown() {
    storage.invoke_on_all(&storage::api::stop_cluster_uuid_waiters).get();
    // Stop accepting new requests.
    if (_kafka_server.ref().local_is_initialized()) {
        shutdown_with_watchdog(_kafka_server, [](auto& kafka_server) {
            return kafka_server.shutdown_input();
        });
    }
    if (_rpc.local_is_initialized()) {
        shutdown_with_watchdog(_rpc, [](auto& rpc_server) {
            return rpc_server.invoke_on_all(&rpc::rpc_server::shutdown_input);
        });
    }
    // Stop routing upload requests, as each may take a while to finish.
    if (offsets_upload_router.local_is_initialized()) {
        shutdown_with_watchdog(
          offsets_upload_router, [](auto& offsets_upload_router) {
              return offsets_upload_router.invoke_on_all(
                &cluster::cloud_metadata::offsets_upload_router::request_stop);
          });
    }
    if (offsets_uploader.local_is_initialized()) {
        shutdown_with_watchdog(offsets_uploader, [](auto& offsets_uploader) {
            return offsets_uploader.invoke_on_all(
              &cluster::cloud_metadata::offsets_uploader::request_stop);
        });
    }

    // We schedule shutting down controller input and aborting its operation as
    // one of the first shutdown steps. This way we terminate all long running
    // operations before shutting down the RPC server, preventing it from
    // waiting on background dispatch gate `close` call.
    if (controller) {
        shutdown_with_watchdog(controller, [](auto& controller) {
            return controller->shutdown_input();
        });
    }

    shutdown_with_watchdog(_migrators, [](auto& migrators) {
        return ss::do_for_each(
          migrators, [](std::unique_ptr<features::feature_migrator>& fm) {
              return fm->stop();
          });
    });

    // Stop processing heartbeats before stopping the partition manager (and
    // the underlying Raft consensus instances). Otherwise we'd process
    // heartbeats for consensus objects that no longer exist.
    if (raft_group_manager.local_is_initialized()) {
        shutdown_with_watchdog(raft_group_manager, [](auto& mgr) {
            return mgr.invoke_on_all(&raft::group_manager::stop_heartbeats);
        });
    }

    if (topic_recovery_service.local_is_initialized()) {
        shutdown_with_watchdog(
          topic_recovery_service, [](auto& topic_recovery_service) {
              return topic_recovery_service.invoke_on_all(
                &cloud_storage::topic_recovery_service::shutdown_recovery);
          });
    }

    if (upstreams.local_is_initialized()) {
        upstreams
          .invoke_on_all(
            &cloud_storage_clients::upstream_registry::prepare_stop)
          .get();
    }

    // Stop any I/O to object store: this will cause any readers in flight
    // to abort and enables partition shutdown to proceed reliably.
    if (cloud_storage_clients.local_is_initialized()) {
        shutdown_with_watchdog(
          cloud_storage_clients, [](auto& cloud_storage_clients) {
              return cloud_storage_clients.invoke_on_all(
                &cloud_storage_clients::client_pool::shutdown_connections);
          });
    }
    if (cloud_io.local_is_initialized()) {
        shutdown_with_watchdog(cloud_io, [](auto& cloud_io) {
            return cloud_io.invoke_on_all(&cloud_io::remote::request_stop);
        });
    }
    /**
     * Shutdown the datalake services before stopping all the partitions.
     * NOTE: translators may call into the coordinator via the coordinator
     * frontend; stop the coordinators first to stop all work as quickly as
     * possible.
     */
    if (_datalake_coordinator_mgr.local_is_initialized()) {
        shutdown_with_watchdog(_datalake_coordinator_mgr, [](auto& mgr) {
            return mgr.invoke_on_all(
              &datalake::coordinator::coordinator_manager::shutdown);
        });
    }
    if (_datalake_manager.local_is_initialized()) {
        shutdown_with_watchdog(_datalake_manager, [](auto& mgr) {
            return mgr.invoke_on_all(&datalake::datalake_manager::shutdown);
        });
    }
    if (_datalake_credential_mgr.local_is_initialized()) {
        shutdown_with_watchdog(_datalake_credential_mgr, [](auto& mgr) {
            return mgr.invoke_on_all(&datalake::credential_manager::stop);
        });
    }
    // Stop all partitions before destructing the subsystems (transaction
    // coordinator, etc). This interrupts ongoing replication requests,
    // allowing higher level state machines to shutdown cleanly.
    if (partition_manager.local_is_initialized()) {
        shutdown_with_watchdog(partition_manager, [](auto& partition_manager) {
            return partition_manager.invoke_on_all(
              &cluster::partition_manager::stop_partitions);
        });
    }

    // NOTE: we must shut down the partitions first above to ensure in-flight
    // replication is stopped, as it may cause cloud topics shutdown to hang.
    if (cloud_topics_app) {
        shutdown_with_watchdog(
          cloud_topics_app, [](auto& app) { return app->stop(); });
    }
    // Wait for all requests to finish before destructing services that may be
    // used by pending requests.
    if (_kafka_server.ref().local_is_initialized()) {
        shutdown_with_watchdog(_kafka_server, [](auto& kafka_server) {
            return kafka_server.wait_for_shutdown();
        });
        shutdown_with_watchdog(_kafka_server, [](auto& kafka_server) {
            return kafka_server.stop();
        });
    }
    if (_kafka_conn_quotas.local_is_initialized()) {
        shutdown_with_watchdog(_kafka_conn_quotas, [](auto& conn_quotas) {
            return conn_quotas.stop();
        });
    }
    if (_rpc.local_is_initialized()) {
        shutdown_with_watchdog(_rpc, [](auto& rpc_server) {
            return rpc_server.invoke_on_all(
              &rpc::rpc_server::wait_for_shutdown);
        });
        shutdown_with_watchdog(_rpc, [](auto& rpc_server) {
            return rpc_server.invoke_on_all(&rpc::rpc_server::stop);
        });
    }

    ssx::sharded_service_container::shutdown();
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
    app.add_options()(
      "node-id-overrides",
      po::value<std::vector<config::node_id_override>>()->multitoken(),
      "Override node UUID and ID iff current UUID matches "
      "- usage: <current UUID>:<new UUID>:<new ID>[/ignore_existing_node_id]?");

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

        if (!vm["node-id-overrides"].empty()) {
            fmt::print(
              std::cout,
              "Node ID overrides: {}",
              vm["node-id-overrides"]
                .as<std::vector<config::node_id_override>>());
        }
    }
    // use endl for explicit flushing
    std::cout << community_msg << std::endl;

    std::string cmd_line = fmt::to_string(
      fmt::join(std::span{av, size_t(ac)}, " "));

    return app.run(ac, av, [this, &app, cmd_line = std::move(cmd_line)] {
        vlog(_log.info, "Redpanda {}", redpanda_version());
        vlog(_log.info, "Command line: {}", cmd_line);
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
                init_crashtracker(app_signal);
                initialize();
                check_environment();
                setup_metrics();
                wire_up_and_start(app_signal);
                post_start_tasks();
                app_signal.wait().get();
                if (!audit_mgr.local().report_redpanda_app_event(
                      security::audit::is_started::no)) {
                    vlog(
                      _log.warn,
                      "Failed to enqueue Redpanda shutdown audit event!");
                }
                trigger_abort_source();
                vlog(_log.info, "Stopping...");
            } catch (const ss::abort_requested_exception&) {
                vlog(_log.info, "Redpanda startup aborted");
                return 0;
            } catch (...) {
                vlog(
                  _log.error,
                  "Failure during startup: {}",
                  std::current_exception());
                if (_crash_tracker_service) {
                    _crash_tracker_service->get_recorder()
                      .record_crash_exception(std::current_exception());
                }
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
  std::optional<YAML::Node> audit_log_client_cfg) {
    ss::smp::invoke_on_all([] {
        // initialize memory groups now that our configuration is loaded
        memory_groups();
    }).get();
    construct_service(
      _memory_sampling, std::ref(_log), ss::sharded_parameter([]() {
          return config::shard_local_cfg().sampled_memory_profile.bind();
      }))
      .get();
    _memory_sampling.invoke_on_all(&memory_sampling::start).get();

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

    construct_service(
      _cpu_profiler,
      ss::sharded_parameter(
        [] { return config::shard_local_cfg().cpu_profiler_enabled.bind(); }),
      ss::sharded_parameter([] {
          return config::shard_local_cfg().cpu_profiler_sample_period_ms.bind();
      }))
      .get();
    _cpu_profiler.invoke_on_all(&resources::cpu_profiler::start).get();

    /*
     * Disable the logger for protobuf; some interfaces don't allow a pluggable
     * error collector.
     */
    // Protobuf uses absl logging in the latest version
    absl::SetMinLogLevel(absl::LogSeverityAtLeast::kInfinity);

    /*
     * allocate per-core zstd decompression workspace and per-core
     * async_stream_zstd workspaces. it can be several megabytes in size, so
     * do it before memory becomes fragmented.
     */
    ss::smp::invoke_on_all([] {
        // TODO: remove this when stream_zstd is replaced with
        // async_stream_zstd in v/kafka
        compression::stream_zstd::init_workspace(
          config::shard_local_cfg().zstd_decompress_workspace_bytes());

        compression::initialize_async_stream_zstd(
          config::shard_local_cfg().zstd_decompress_workspace_bytes());

        compression::init_lz4_decompression_buffers(
          compression::lz4_decompression_buffers::bufsize,
          compression::lz4_decompression_buffers::min_threshold,
          config::shard_local_cfg().lz4_decompress_reusable_buffers_disabled());
    }).get();

    if (config::shard_local_cfg().enable_pid_file()) {
        // check that the data directory exists now, because we are about to
        // create the pidfile
        syschecks::directory_must_exist(
          "data directory", config::node().data_directory().path);
        syschecks::pidfile_create(config::node().pidfile_path());
    }
    smp_groups::config smp_groups_cfg{
      .raft_group_max_non_local_requests
      = config::shard_local_cfg().raft_smp_max_non_local_requests().value_or(
        smp_groups::default_raft_non_local_requests(
          config::shard_local_cfg().topic_partitions_per_shard())),
      .proxy_group_max_non_local_requests
      = config::shard_local_cfg().pp_sr_smp_max_non_local_requests().value_or(
        smp_groups::default_max_nonlocal_requests)};

    smp_service_groups.create_groups(smp_groups_cfg).get();
    _deferred.emplace_back(
      [this] { smp_service_groups.destroy_groups().get(); });

    // Ensure the scheduling groups singleton is initialized early
    std::ignore = scheduling_groups::instance();

    construct_service(_scheduling_groups_probe).get();
    _scheduling_groups_probe
      .invoke_on_all([](scheduling_groups_probe& s) {
          return s.start(scheduling_groups::instance());
      })
      .get();

    if (proxy_cfg) {
        _proxy_config.emplace(*proxy_cfg);
        for (const auto& e : _proxy_config->errors()) {
            vlog(
              _log.warn,
              "Pandaproxy property '{}' validation error: {}",
              e.first,
              e.second);
        }
        if (_proxy_config->errors().size() > 0) {
            throw std::invalid_argument(
              "Validation errors in pandaproxy config");
        }
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
    if (audit_log_client_cfg) {
        _audit_log_client_config.emplace(*audit_log_client_cfg);
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
      {{"io_queue_total_read_ops", metrics::public_metrics_handle},
       {"io_queue_total_write_ops", metrics::public_metrics_handle},
       {"memory_allocated_memory", metrics::public_metrics_handle},
       {"memory_free_memory", metrics::public_metrics_handle}})
      .get();

    _public_metrics.start().get();

    const auto version_label = metrics::make_namespaced_label("version")(
      redpanda_git_version());
    const auto revision_label = metrics::make_namespaced_label("revision")(
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
                 .aggregate({sm::shard_label}),
               sm::make_gauge(
                 "fips_mode",
                 [] {
                     return static_cast<unsigned int>(
                       config::node().fips_mode());
                 },
                 sm::description(
                   "Identifies whether or not Redpanda is "
                   "running in FIPS mode."))
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
      {sm::make_gauge(
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

       sm::make_gauge(
         "fips_mode",
         [] { return static_cast<unsigned int>(config::node().fips_mode()); },
         sm::description(
           "Identifies whether or not Redpanda is running in FIPS mode."))});
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
    auto raw_cfg_path = cfg["redpanda-cfg"].as<std::string>();
    // Expand ~/redpanda.yaml to the full path
    if (raw_cfg_path.starts_with("~")) {
        const char* home = std::getenv("HOME");
        if (home) {
            raw_cfg_path = fmt::format("{}{}", home, raw_cfg_path.substr(1));
        }
    }
    std::filesystem::path cfg_path(raw_cfg_path);

    // Retain the original bytes loaded so that we can hexdump them later
    // if YAML Parse fails.
    // Related: https://github.com/redpanda-data/redpanda/issues/3798
    auto yaml_raw_bytes = read_fully_tmpbuf(cfg_path).get();
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
    }).get();

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
    /// Special case scenario with Rack ID being set to ''
    /// Redpanda supplied ansible scripts are setting "rack: ''" which has
    /// caused issues with some of our customers.  This hacky work around is an
    /// alternative to using validation in node config to stop Redpanda from
    /// starting if rack is supplied with an empty string.
    auto& rack_id = config::node().rack.value();
    if (rack_id.has_value() && *rack_id == model::rack_id{""}) {
        vlog(
          _log.warn,
          "redpanda.rack specified as empty string.  Please remove "
          "`redpanda.rack = ''` from your node config as in the future this "
          "may result in Redpanda failing to start");
        config::node().rack.set_value(std::nullopt);
    }

    // load ID overrides
    if (!cfg["node-id-overrides"].empty()) {
        ss::smp::invoke_on_all([&cfg] {
            config::node().node_id_overrides.set_value(
              cfg["node-id-overrides"]
                .as<std::vector<config::node_id_override>>());
        }).get();
    }

    // This includes loading from local bootstrap file or legacy
    // config file on first-start or upgrade cases.
    _config_preload = cluster::config_manager::preload(config).get();

    vlog(_log.info, "Cluster configuration properties:");
    vlog(_log.info, "(use `rpk cluster config edit` to change)");
    config_printer("redpanda", config::shard_local_cfg());

    vlog(_log.info, "Node configuration properties:");
    vlog(_log.info, "(use `rpk redpanda config set <cfg> <value>` to change)");
    config_printer("redpanda", config::node());

    if (config["pandaproxy"]) {
        _proxy_config.emplace(config["pandaproxy"]);
        for (const auto& e : _proxy_config->errors()) {
            vlog(
              _log.warn,
              "Pandaproxy property '{}' validation error: {}",
              e.first,
              e.second);
        }
        if (_proxy_config->errors().size() > 0) {
            throw std::invalid_argument(
              "Validation errors in pandaproxy config");
        }
        if (config["pandaproxy_client"]) {
            _proxy_client_config.emplace(config["pandaproxy_client"]);
        } else {
            set_local_kafka_client_config(_proxy_client_config, config::node());
        }
        set_pp_kafka_client_defaults(*_proxy_config, *_proxy_client_config);
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
    /// Auditing will be toggled via cluster config settings, internal audit
    /// client options can be configured via local config properties
    if (config["audit_log_client"]) {
        _audit_log_client_config.emplace(config["audit_log_client"]);
    } else {
        set_local_kafka_client_config(_audit_log_client_config, config::node());
    }
    set_auditing_kafka_client_defaults(*_audit_log_client_config);
    config_printer("audit_log_client", *_audit_log_client_config);
}

void application::check_environment() {
    static constexpr std::string_view fips_enabled_file
      = "/proc/sys/crypto/fips_enabled";
    syschecks::systemd_message("checking environment (CPU, Mem)").get();
    syschecks::cpu();
    syschecks::memory(config::node().developer_mode());
    memory_groups().log_memory_group_allocations(_log);
    storage::directories::initialize(
      config::node().data_directory().as_sstring())
      .get();
    cloud_io::cache::initialize(config::node().cloud_storage_cache_path())
      .get();

    if (config::shard_local_cfg().storage_strict_data_init()) {
        // Look for the special file that indicates a user intends
        // for the found data directory to be the one we use.
        auto strict_data_dir_file
          = config::node().strict_data_dir_file_path().string();
        auto file_exists = ss::file_exists(strict_data_dir_file).get();

        if (!file_exists) {
            throw std::invalid_argument(
              ssx::sformat(
                "Data directory not in expected state: {} not found, is the "
                "expected filesystem mounted?",
                strict_data_dir_file));
        }
    }

    if (config::fips_mode_enabled(config::node().fips_mode())) {
        if (!ss::file_exists(fips_enabled_file).get()) {
            if (config::node().fips_mode() == config::fips_mode_flag::enabled) {
                throw std::runtime_error(
                  fmt::format(
                    "File '{}' does not exist.  Redpanda cannot start in FIPS "
                    "mode",
                    fips_enabled_file));
            } else if (
              config::node().fips_mode()
              == config::fips_mode_flag::permissive) {
                vlog(
                  _log.warn,
                  "File '{}' does not exist.  Redpanda will start in FIPS mode "
                  "but this is not a support configuration",
                  fips_enabled_file);
            } else {
                vassert(
                  false,
                  "Should not be performing environment check for FIPS when "
                  "fips_mode flag is {}",
                  config::node().fips_mode());
            }
        } else {
            auto fd = ss::file_desc::open(fips_enabled_file.data(), O_RDONLY);
            char buf[1];
            fd.read(buf, 1);
            if (buf[0] != '1') {
                auto msg = fmt::format(
                  "File '{}' not reporting '1'.  Redpanda cannot start in FIPS "
                  "mode",
                  fips_enabled_file);
                if (
                  config::node().fips_mode()
                  == config::fips_mode_flag::enabled) {
                    throw std::runtime_error(msg);
                } else if (
                  config::node().fips_mode()
                  == config::fips_mode_flag::permissive) {
                    vlog(_log.warn, "{}", msg);
                } else {
                    vassert(
                      false,
                      "Should not be performing environment check for FIPS "
                      "when fips_mode flag is {}",
                      config::node().fips_mode());
                }
            }
        }
        syschecks::systemd_message("Starting Redpanda in FIPS mode").get();
    }
}

void application::init_crashtracker(::stop_signal& app_signal) {
    _crash_tracker_service = std::make_unique<crash_tracker::service>();
    _crash_tracker_service->start(app_signal.abort_source()).get();
    crash_tracker::install_sighandlers();
}

void application::schedule_crash_tracker_file_cleanup() {
    // Schedule a deletion of the tracker file. On a clean shutdown,
    // the tracker file should be deleted thus reseting the crash count on the
    // next run. In case of an unclean shutdown, we already bumped
    // the crash count and that should be taken into account in the
    // next run.
    // We emplace it in the front to make it the last task to run.
    _deferred.emplace_front([&] {
        if (_crash_tracker_service) {
            _crash_tracker_service->stop().get();
        }
    });
}

ss::future<> application::set_proxy_config(ss::sstring name, std::any val) {
    return _proxy->set_config(std::move(name), std::move(val));
}

bool application::requires_cloud_io() {
    return archival_storage_enabled() || datalake_enabled();
}

bool application::archival_storage_enabled() {
    const auto& cfg = config::shard_local_cfg();
    return cfg.cloud_storage_enabled();
}

bool application::wasm_data_transforms_enabled() {
    return config::shard_local_cfg().data_transforms_enabled.value()
           && !config::node().emergency_disable_data_transforms.value();
}

bool application::datalake_enabled() {
    return config::shard_local_cfg().iceberg_enabled()
           && !config::node().recovery_mode_enabled();
}

ss::shared_ptr<kafka::datalake_usage_api>
application::make_datalake_usage_aggregator() {
    if (datalake_enabled()) {
        return ss::make_shared<datalake::default_datalake_usage_api_impl>(
          controller.get(),
          &controller->get_topics_state(),
          &_datalake_coordinator_fe);
    }
    return ss::make_shared<datalake::disabled_datalake_usage_api_impl>(
      controller.get());
}

ss::future<>
application::set_proxy_client_config(ss::sstring name, std::any val) {
    return _proxy->set_client_config(std::move(name), std::move(val));
}

void application::trigger_abort_source() {
    _as
      .invoke_on_all([](auto& local_as) {
          local_as.request_abort_ex(ssx::shutdown_requested_exception{});
      })
      .get();
}
