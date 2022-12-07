/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/service.h"

#include "archival/logger.h"
#include "archival/ntp_archiver_service.h"
#include "archival/types.h"
#include "cloud_roles/signature.h"
#include "cloud_storage/topic_manifest.h"
#include "cloud_storage/types.h"
#include "cluster/partition_manager.h"
#include "cluster/topic_table.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "config/property.h"
#include "features/feature_table.h"
#include "http/client.h"
#include "likely.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "s3/client.h"
#include "s3/error.h"
#include "storage/disk_log_impl.h"
#include "storage/fs_utils.h"
#include "storage/log.h"
#include "storage/ntp_config.h"
#include "utils/gate_guard.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/with_scheduling_group.hh>

#include <boost/iterator/counting_iterator.hpp>

#include <algorithm>
#include <exception>
#include <optional>
#include <stdexcept>
#include <tuple>

using namespace std::chrono_literals;

namespace archival::internal {

static cloud_storage::manifest_topic_configuration convert_topic_configuration(
  const cluster::topic_configuration& cfg,
  cluster::replication_factor replication_factor) {
    cloud_storage::manifest_topic_configuration result {
        .tp_ns = cfg.tp_ns,
        .partition_count = cfg.partition_count,
        .replication_factor = replication_factor,
        .properties = {
            .compression = cfg.properties.compression,
            .cleanup_policy_bitflags = cfg.properties.cleanup_policy_bitflags,
            .compaction_strategy = cfg.properties.compaction_strategy,
            .timestamp_type = cfg.properties.timestamp_type,
            .segment_size = cfg.properties.segment_size,
            .retention_bytes = cfg.properties.retention_bytes,
            .retention_duration = cfg.properties.retention_duration,
        },
    };
    return result;
}

static ss::sstring get_value_or_throw(
  const config::property<std::optional<ss::sstring>>& prop, const char* name) {
    auto opt = prop.value();
    if (!opt) {
        vlog(
          archival_log.error,
          "Configuration property {} is required to enable archival storage",
          name);
        throw std::runtime_error(
          fmt::format("configuration property {} is not set", name));
    }
    return *opt;
}

/// Use shard-local configuration to generate configuration
ss::future<archival::configuration>
scheduler_service_impl::get_archival_service_config(
  ss::scheduling_group sg, ss::io_priority_class p) {
    vlog(archival_log.debug, "Generating archival configuration");
    auto disable_metrics = net::metrics_disabled(
      config::shard_local_cfg().disable_metrics());

    auto time_limit = config::shard_local_cfg()
                        .cloud_storage_segment_max_upload_interval_sec.value();
    if (time_limit and time_limit.value() == 0s) {
        vlog(
          archival_log.error,
          "Configuration property "
          "cloud_storage_segment_max_upload_interval_sec can't be 0");
        throw std::runtime_error(
          "cloud_storage_segment_max_upload_interval_sec is invalid");
    }
    auto time_limit_opt = time_limit ? std::make_optional(
                            segment_time_limit(*time_limit))
                                     : std::nullopt;
    archival::configuration cfg{
      .bucket_name = s3::bucket_name(get_value_or_throw(
        config::shard_local_cfg().cloud_storage_bucket,
        "cloud_storage_bucket")),
      .reconciliation_interval
      = config::shard_local_cfg().cloud_storage_reconciliation_ms.value(),
      .cloud_storage_initial_backoff
      = config::shard_local_cfg().cloud_storage_initial_backoff_ms.value(),
      .segment_upload_timeout
      = config::shard_local_cfg()
          .cloud_storage_segment_upload_timeout_ms.value(),
      .manifest_upload_timeout
      = config::shard_local_cfg()
          .cloud_storage_manifest_upload_timeout_ms.value(),
      .upload_loop_initial_backoff
      = config::shard_local_cfg()
          .cloud_storage_upload_loop_initial_backoff_ms.value(),
      .upload_loop_max_backoff
      = config::shard_local_cfg()
          .cloud_storage_upload_loop_max_backoff_ms.value(),
      .svc_metrics_disabled = service_metrics_disabled(
        static_cast<bool>(disable_metrics)),
      .ntp_metrics_disabled = per_ntp_metrics_disabled(
        static_cast<bool>(disable_metrics)),
      .time_limit = time_limit_opt,
      .upload_scheduling_group = sg,
      .upload_io_priority = p};
    vlog(archival_log.debug, "Archival configuration generated: {}", cfg);
    co_return cfg;
}

scheduler_service_impl::scheduler_service_impl(
  const configuration& conf,
  ss::sharded<cloud_storage::remote>& remote,
  ss::sharded<cluster::partition_manager>& pm,
  ss::sharded<cluster::topic_table>& tt,
  ss::sharded<features::feature_table>& ft)
  : _conf(conf)
  , _partition_manager(pm)
  , _topic_table(tt)
  , _feature_table(ft)
  , _jitter(conf.reconciliation_interval, 1ms)
  , _rtclog(archival_log, _rtcnode)
  , _probe(conf.svc_metrics_disabled)
  , _remote(remote)
  , _topic_manifest_upload_timeout(conf.manifest_upload_timeout)
  , _initial_backoff(conf.cloud_storage_initial_backoff)
  , _upload_sg(conf.upload_scheduling_group) {}

scheduler_service_impl::scheduler_service_impl(
  ss::sharded<cloud_storage::remote>& remote,
  ss::sharded<cluster::partition_manager>& pm,
  ss::sharded<cluster::topic_table>& tt,
  ss::sharded<archival::configuration>& config,
  ss::sharded<features::feature_table>& ft)
  : scheduler_service_impl(config.local(), remote, pm, tt, ft) {}

void scheduler_service_impl::rearm_timer() {
    ssx::background = ssx::spawn_with_gate_then(_gate, [this] {
                          return ss::with_scheduling_group(_upload_sg, [this] {
                              return reconcile_archivers().finally([this] {
                                  if (_gate.is_closed()) {
                                      return;
                                  }
                                  _timer.rearm(_jitter());
                              });
                          });
                      }).handle_exception([this](std::exception_ptr e) {
        vlog(_rtclog.info, "Error in timer callback: {}", e);
    });
}
ss::future<> scheduler_service_impl::start() {
    _timer.set_callback([this] { rearm_timer(); });
    _timer.rearm(_jitter());
    return ss::now();
}

ss::future<> scheduler_service_impl::stop() {
    if (_stopped) {
        vlog(_rtclog.info, "Scheduler service is already stopped");
        co_return;
    }
    _stopped = true;
    vlog(_rtclog.info, "Scheduler service stop");
    _timer.cancel();
    co_await _gate.close();
    for (auto& it : _archivers) {
        co_await it.second->stop();
    }
}

ss::future<> scheduler_service_impl::upload_topic_manifest(
  model::topic_namespace topic_ns, model::initial_revision_id rev) {
    gate_guard gg(_gate);
    auto cfg = _topic_table.local().get_topic_cfg(topic_ns);
    auto replication_factor = _topic_table.local().get_topic_replication_factor(
      topic_ns);
    if (!cfg || !replication_factor) {
        co_return;
    }
    try {
        bool uploaded = false;
        while (!uploaded && !_gate.is_closed()) {
            // This runs asynchronously so we can just retry indefinetly
            retry_chain_node fib(
              _topic_manifest_upload_timeout, _initial_backoff, &_rtcnode);
            retry_chain_logger ctxlog(archival_log, fib);
            vlog(ctxlog.info, "Uploading topic manifest {}", topic_ns);
            cloud_storage::topic_manifest tm(
              convert_topic_configuration(*cfg, replication_factor.value()),
              rev);
            auto key = tm.get_manifest_path();
            vlog(ctxlog.debug, "Topic manifest object key is '{}'", key);
            auto tags = cloud_storage::remote::get_manifest_tags(topic_ns, rev);
            auto res = co_await _remote.local().upload_manifest(
              _conf.bucket_name, tm, fib, std::move(tags));
            uploaded = res == cloud_storage::upload_result::success;
            if (!uploaded) {
                vlog(ctxlog.warn, "Topic manifest upload timed out: {}", key);
            }
        }
    } catch (const ss::gate_closed_exception& err) {
        vlog(
          _rtclog.error,
          "Topic manifest upload for {} failed, {}",
          topic_ns,
          err);
    } catch (const ss::abort_requested_exception& err) {
        vlog(
          _rtclog.error,
          "Topic manifest upload for {} failed, {}",
          topic_ns,
          err);
    }
}

ss::future<> scheduler_service_impl::add_ntp_archiver(
  ss::lw_shared_ptr<ntp_archiver> archiver) {
    vassert(
      !_archivers.contains(archiver->get_ntp()),
      "archiver for ntp {} already added!",
      archiver->get_ntp());

    if (_gate.is_closed()) {
        co_return;
    }

    ss::gate::holder gh(_gate);

    _archivers.emplace(archiver->get_ntp(), archiver);
    auto ntp = archiver->get_ntp();
    auto part = _partition_manager.local().get(ntp);
    if (!part) {
        co_return;
    }

    vlog(_rtclog.info, "Starting archiver for partition {}", ntp);

    if (part->get_ntp_config().is_read_replica_mode_enabled()) {
        archiver->run_sync_manifest_loop();
    } else {
        /**
         * In Redpanda 22.3 (cluster logical version 7), S3 segment naming
         * and partition manifest format change.  Therefore we must not
         * write anything into the new format while any nodes are still
         * on the old version (i.e. before feature is active).  This is
         * a simpler alternative than teaching Redpanda to conditionally
         * write the old format.
         */
        if (!_feature_table.local().is_active(
              features::feature::cloud_retention)) {
            vlog(
              _rtclog.info,
              "Upgrade in progress, delaying archiver startup for partition {}",
              ntp);

            // This function is called from a reconciliation loop, so rather
            // than sleeping on await_feature, we can simply return and let it
            // retry later.
            _archivers.erase(archiver->get_ntp());
            co_return;
        }

        if (ntp.tp.partition == 0) {
            // Upload manifest once per topic. GCS has strict
            // limits for single object updates.
            ssx::background = upload_topic_manifest(
              model::topic_namespace(ntp.ns, ntp.tp.topic),
              archiver->get_revision_id());
        }

        _probe.start_archiving_ntp();
        archiver->run_upload_loop();
    }

    co_return;
}

ss::future<>
scheduler_service_impl::create_archivers(std::vector<model::ntp> to_create) {
    gate_guard g(_gate);
    // add_ntp_archiver can potentially use two connections
    auto concurrency = std::max(1UL, _remote.local().concurrency() / 2);
    co_await ss::max_concurrent_for_each(
      std::move(to_create), concurrency, [this](const model::ntp& ntp) {
          auto log = _partition_manager.local().log(ntp);
          auto part = _partition_manager.local().get(ntp);
          if (!log.has_value() || !part || !part->is_elected_leader()) {
              return ss::now();
          }
          if (
            part->get_ntp_config().is_archival_enabled()
            || part->get_ntp_config().is_read_replica_mode_enabled()) {
              auto archiver = ss::make_lw_shared<ntp_archiver>(
                log->config(),
                _partition_manager.local(),
                _conf,
                _remote.local(),
                part);
              return add_ntp_archiver(archiver);
          } else {
              return ss::now();
          }
      });
}

ss::future<>
scheduler_service_impl::remove_archivers(std::vector<model::ntp> to_remove) {
    gate_guard g(_gate);
    return ss::parallel_for_each(
             to_remove,
             [this](const model::ntp& ntp) -> ss::future<> {
                 vlog(_rtclog.info, "removing archiver for {}", ntp.path());
                 auto archiver = _archivers.at(ntp);
                 return archiver->stop().finally([this, ntp] {
                     vlog(_rtclog.info, "archiver stopped {}", ntp.path());
                     _archivers.erase(ntp);
                     _probe.stop_archiving_ntp();
                 });
             })
      .finally([g = std::move(g)] {});
}

ss::future<> scheduler_service_impl::reconcile_archivers() {
    gate_guard g(_gate);
    cluster::partition_manager& pm = _partition_manager.local();

    std::vector<model::ntp> to_remove;
    // find archivers that have already stopped
    for (const auto& [ntp, archiver] : _archivers) {
        auto p = pm.get(ntp);
        if (!p || archiver->is_loop_stopped()) {
            to_remove.push_back(ntp);
        }
    }

    std::vector<model::ntp> to_create;
    // find new ntps that present in the snapshot only
    for (const auto& [ntp, p] : pm.partitions()) {
        // archival_metadata_stm is only created for topics in kafka namespace
        // if we ever uploaded some topics from kafka_internal by accident these
        // topics won't have an STM and the snapshot so it's safe to limit
        // creation of archivers by 'kafka' namespace for now.
        // we will change this in the future when we will add DR
        if (
          ntp.ns == model::kafka_namespace && !_archivers.contains(ntp)
          && p->is_elected_leader()) {
            to_create.push_back(ntp);
        }
    }

    // epxect to_create & to_remove be empty most of the time
    if (unlikely(!to_remove.empty() || !to_create.empty())) {
        // run in parallel
        ss::future<> fremove = ss::now();
        if (!to_remove.empty()) {
            fremove = remove_archivers(std::move(to_remove));
        }
        ss::future<> fcreate = ss::now();
        if (!to_create.empty()) {
            fcreate = create_archivers(std::move(to_create));
        }
        auto [remove_fut, create_fut] = co_await ss::when_all(
          std::move(fremove), std::move(fcreate));
        if (remove_fut.failed()) {
            vlog(_rtclog.error, "Failed to remove archivers");
            remove_fut.ignore_ready_future();
        }
        if (create_fut.failed()) {
            vlog(_rtclog.error, "Failed to create archivers");
            create_fut.ignore_ready_future();
        }
    }
}

cloud_storage::remote& scheduler_service_impl::get_remote() {
    return _remote.local();
}

s3::bucket_name scheduler_service_impl::get_bucket() const {
    return _conf.bucket_name;
}

uint64_t scheduler_service_impl::estimate_backlog_size() {
    uint64_t size = 0;
    for (const auto& [ntp, archiver] : _archivers) {
        std::ignore = ntp;
        size += archiver->estimate_backlog_size();
    }
    return size;
}

ss::future<std::optional<cloud_storage::partition_manifest>>
scheduler_service_impl::maybe_truncate_manifest(const model::ntp& ntp) {
    if (auto it = _archivers.find(ntp); it != _archivers.end()) {
        co_return co_await it->second->maybe_truncate_manifest(_rtcnode);
    }
    co_return std::nullopt;
}

} // namespace archival::internal
