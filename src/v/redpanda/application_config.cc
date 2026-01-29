// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/vlog.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "kafka/client/configuration.h"
#include "pandaproxy/rest/configuration.h"
#include "resource_mgmt/scheduling_groups_probe.h"
#include "storage/api.h"
#include "storage/backlog_controller.h"
#include "storage/log_manager.h"
#include "utils/human.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/conversions.hh>

#include <boost/program_options/variables_map.hpp>
#include <sys/resource.h>

#include <algorithm>
#include <chrono>

using namespace std::chrono_literals;

void set_local_kafka_client_config(
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

void set_pp_kafka_client_defaults(
  pandaproxy::rest::configuration& proxy_config,
  kafka::client::configuration& client_config) {
    // override pandaparoxy_client.consumer_session_timeout_ms with
    // pandaproxy.consumer_instance_timeout_ms
    client_config.consumer_session_timeout.set_value(
      proxy_config.consumer_instance_timeout.value());

    if (!client_config.client_identifier.is_overriden()) {
        client_config.client_identifier.set_value(
          std::make_optional<ss::sstring>("pandaproxy_client"));
    }
}

void set_sr_kafka_client_defaults(kafka::client::configuration& client_config) {
    if (!client_config.produce_batch_delay.is_overriden()) {
        client_config.produce_batch_delay.set_value(0ms);
    }
    if (!client_config.produce_batch_record_count.is_overriden()) {
        client_config.produce_batch_record_count.set_value(int32_t(0));
    }
    if (!client_config.produce_batch_size_bytes.is_overriden()) {
        client_config.produce_batch_size_bytes.set_value(int32_t(0));
    }
    if (!client_config.client_identifier.is_overriden()) {
        client_config.client_identifier.set_value(
          std::make_optional<ss::sstring>("schema_registry_client"));
    }
}

void set_auditing_kafka_client_defaults(
  kafka::client::configuration& client_config) {
    if (!client_config.produce_batch_delay.is_overriden()) {
        client_config.produce_batch_delay.set_value(0ms);
    }
    if (!client_config.produce_batch_record_count.is_overriden()) {
        client_config.produce_batch_record_count.set_value(int32_t(0));
    }
    if (!client_config.produce_batch_size_bytes.is_overriden()) {
        client_config.produce_batch_size_bytes.set_value(int32_t(0));
    }
    if (!client_config.client_identifier.is_overriden()) {
        client_config.client_identifier.set_value(
          std::make_optional<ss::sstring>("audit_log_client"));
    }
    if (!client_config.produce_compression_type.is_overriden()) {
        client_config.produce_compression_type.set_value("zstd");
    }
    if (!client_config.produce_ack_level.is_overriden()) {
        client_config.produce_ack_level.set_value(int16_t(1));
    }
    if (!client_config.produce_shutdown_delay.is_overriden()) {
        client_config.produce_shutdown_delay.set_value(3000ms);
    }
    /// explicity override the scram details as the client will need to use
    /// broker generated ephemeral credentials
    client_config.scram_password.reset();
    client_config.scram_username.reset();
    client_config.sasl_mechanism.reset();
}

void log_system_resources(
  ss::logger& log, const boost::program_options::variables_map& cfg) {
    const auto shard_mem = ss::memory::stats();
    auto total_mem = shard_mem.total_memory() * ss::smp::count;
    /**
     * IMPORTANT: copied out of seastar `resources.cc`, if logic in seastar will
     * change we have to change our logic in here.
     */
    const size_t default_reserve_memory = std::max<size_t>(
      1536_MiB, 0.07 * total_mem);
    auto reserve = cfg.contains("reserve-memory")
                     ? ss::parse_memory_size(
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

std::optional<storage::file_sanitize_config> read_file_sanitizer_config() {
    std::optional<storage::file_sanitize_config> file_config = std::nullopt;
    if (config::node().storage_failure_injection_config_path()) {
        file_config
          = storage::make_finjector_file_config(
              config::node().storage_failure_injection_config_path().value())
              .get();
    }

    return file_config;
}

storage::kvstore_config kvstore_config_from_global_config(
  std::optional<storage::file_sanitize_config> sanitizer_config) {
    /*
     * The key-value store is rooted at the configured data directory,
     * and the internal kvstore topic-namespace results in a storage
     * layout of:
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
      sanitizer_config);
}

storage::log_config manager_config_from_global_config(
  scheduling_groups& sgs,
  std::optional<storage::file_sanitize_config> sanitizer_config) {
    return storage::log_config(
      config::node().data_directory().as_sstring(),
      config::shard_local_cfg().log_segment_size.bind(),
      config::shard_local_cfg().compacted_log_segment_size.bind(),
      config::shard_local_cfg().max_compacted_log_segment_size.bind(),
      storage::jitter_percents(
        config::shard_local_cfg().log_segment_size_jitter_percent()),
      config::shard_local_cfg().retention_bytes.bind(),
      config::shard_local_cfg().log_compaction_interval_ms.bind(),
      config::shard_local_cfg().log_retention_ms.bind(),
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
      sgs.compaction_sg(),
      std::move(sanitizer_config));
}

storage::backlog_controller_config
compaction_controller_config(ss::scheduling_group sg, uint64_t fs_avail) {
    auto backlog_size_function = [fs_avail] {
        /**
         * By default we set desired compaction backlog size to 10% of disk
         * availability.
         */
        static const int64_t backlog_avail_percents = 10;
        return config::shard_local_cfg()
          .compaction_ctrl_backlog_size()
          .value_or((fs_avail / 100) * backlog_avail_percents / ss::smp::count);
    };

    /**
     * We normalize internals using disk availability to make controller
     * settings independent from disk space. After normalization all values
     * equal to disk availability will be represented in the controller with
     * value equal 1000.
     *
     * Set point = 10% of disk availability will always be equal to 100.
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
    auto normalization = fs_avail / (1000 * ss::smp::count);

    return storage::backlog_controller_config(
      config::shard_local_cfg().compaction_ctrl_p_coeff.bind(),
      config::shard_local_cfg().compaction_ctrl_i_coeff.bind(),
      config::shard_local_cfg().compaction_ctrl_d_coeff.bind(),
      normalization,
      std::move(backlog_size_function),
      200,
      config::shard_local_cfg().compaction_ctrl_update_interval_ms.bind(),
      sg,
      config::shard_local_cfg().compaction_ctrl_min_shares.bind(),
      config::shard_local_cfg().compaction_ctrl_max_shares.bind());
}

storage::backlog_controller_config
make_upload_controller_config(ss::scheduling_group sg, uint64_t fs_avail) {
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

    auto setpoint_function = []() { return 0; };
    int64_t normalization = static_cast<int64_t>(fs_avail)
                            / (1000 * ss::smp::count);
    return {
      config::shard_local_cfg().cloud_storage_upload_ctrl_p_coeff.bind(),
      config::mock_binding(0.0),
      config::shard_local_cfg().cloud_storage_upload_ctrl_d_coeff.bind(),
      normalization,
      std::move(setpoint_function),
      static_cast<int>(sg.get_shares()),
      config::shard_local_cfg()
        .cloud_storage_upload_ctrl_update_interval_ms.bind(),
      sg,
      config::shard_local_cfg().cloud_storage_upload_ctrl_min_shares.bind(),
      config::shard_local_cfg().cloud_storage_upload_ctrl_max_shares.bind()};
}
