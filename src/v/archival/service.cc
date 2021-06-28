/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/service.h"

#include "archival/logger.h"
#include "archival/ntp_archiver_service.h"
#include "archival/types.h"
#include "cloud_storage/types.h"
#include "cluster/partition_manager.h"
#include "cluster/topic_table.h"
#include "config/configuration.h"
#include "config/property.h"
#include "http/client.h"
#include "likely.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "s3/client.h"
#include "s3/error.h"
#include "s3/signature.h"
#include "storage/disk_log_impl.h"
#include "storage/fs_utils.h"
#include "storage/log.h"
#include "utils/gate_guard.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/when_all.hh>

#include <boost/iterator/counting_iterator.hpp>

#include <algorithm>
#include <exception>
#include <stdexcept>

using namespace std::chrono_literals;

namespace archival::internal {

ntp_upload_queue::iterator ntp_upload_queue::begin() {
    return _archivers.begin();
}

ntp_upload_queue::iterator ntp_upload_queue::end() { return _archivers.end(); }

void ntp_upload_queue::insert(ntp_upload_queue::value archiver) {
    auto key = archiver->get_ntp();
    auto [it, ok] = _archivers.insert(
      std::make_pair(key, upload_queue_item{.archiver = archiver}));
    if (ok) {
        // We only need to insert new element into the queue if the insert
        // into the map is actually happend. We don't need to overwrite any
        // existing archiver.
        _upload_queue.push_back(it->second);
    }
}

void ntp_upload_queue::erase(const ntp_upload_queue::key& ntp) {
    _archivers.erase(ntp);
}

bool ntp_upload_queue::contains(const key& ntp) const {
    return _archivers.contains(ntp);
}

size_t ntp_upload_queue::size() const noexcept { return _archivers.size(); }

ntp_upload_queue::value ntp_upload_queue::get_upload_candidate() {
    auto& candidate = _upload_queue.front();
    candidate._upl_hook.unlink();
    _upload_queue.push_back(candidate);
    return candidate.archiver;
}

ntp_upload_queue::value ntp_upload_queue::operator[](const key& ntp) const {
    auto it = _archivers.find(ntp);
    if (it == _archivers.end()) {
        return nullptr;
    }
    return it->second.archiver;
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
scheduler_service_impl::get_archival_service_config() {
    vlog(archival_log.debug, "Generating archival configuration");
    auto secret_key = s3::private_key_str(get_value_or_throw(
      config::shard_local_cfg().cloud_storage_secret_key,
      "cloud_storage_secret_key"));
    auto access_key = s3::public_key_str(get_value_or_throw(
      config::shard_local_cfg().cloud_storage_access_key,
      "cloud_storage_access_key"));
    auto region = s3::aws_region_name(get_value_or_throw(
      config::shard_local_cfg().cloud_storage_region, "cloud_storage_region"));
    auto disable_metrics = rpc::metrics_disabled(
      config::shard_local_cfg().disable_metrics());

    // Set default overrides
    s3::default_overrides overrides;
    if (auto optep
        = config::shard_local_cfg().cloud_storage_api_endpoint.value();
        optep.has_value()) {
        overrides.endpoint = s3::endpoint_url(*optep);
    }
    overrides.disable_tls = config::shard_local_cfg().cloud_storage_disable_tls;
    if (auto cert = config::shard_local_cfg().cloud_storage_trust_file.value();
        cert.has_value()) {
        overrides.trust_file = s3::ca_trust_file(std::filesystem::path(*cert));
    }
    overrides.port = config::shard_local_cfg().cloud_storage_api_endpoint_port;

    auto s3_conf = co_await s3::configuration::make_configuration(
      access_key, secret_key, region, overrides, disable_metrics);
    archival::configuration cfg{
      .client_config = std::move(s3_conf),
      .bucket_name = s3::bucket_name(get_value_or_throw(
        config::shard_local_cfg().cloud_storage_bucket,
        "cloud_storage_bucket")),
      .interval
      = config::shard_local_cfg().cloud_storage_reconciliation_ms.value(),
      .connection_limit = s3_connection_limit(
        config::shard_local_cfg().cloud_storage_max_connections.value()),
      .initial_backoff
      = config::shard_local_cfg().cloud_storage_initial_backoff_ms.value(),
      .segment_upload_timeout
      = config::shard_local_cfg()
          .cloud_storage_segment_upload_timeout_ms.value(),
      .manifest_upload_timeout
      = config::shard_local_cfg()
          .cloud_storage_manifest_upload_timeout_ms.value(),
      .svc_metrics_disabled = service_metrics_disabled(
        static_cast<bool>(disable_metrics)),
      .ntp_metrics_disabled = per_ntp_metrics_disabled(
        static_cast<bool>(disable_metrics)),
    };
    vlog(archival_log.debug, "Archival configuration generated: {}", cfg);
    co_return cfg;
}

scheduler_service_impl::scheduler_service_impl(
  const configuration& conf,
  ss::sharded<storage::api>& api,
  ss::sharded<cluster::partition_manager>& pm,
  ss::sharded<cluster::topic_table>& tt)
  : _conf(conf)
  , _partition_manager(pm)
  , _topic_table(tt)
  , _storage_api(api)
  , _jitter(conf.interval, 1ms)
  , _stop_limit(conf.connection_limit())
  , _rtcnode(_as)
  , _rtclog(archival_log, _rtcnode)
  , _probe(conf.svc_metrics_disabled)
  , _remote(conf.connection_limit, conf.client_config, _probe)
  , _topic_manifest_upload_timeout(conf.manifest_upload_timeout)
  , _initial_backoff(conf.initial_backoff) {}

scheduler_service_impl::scheduler_service_impl(
  ss::sharded<storage::api>& api,
  ss::sharded<cluster::partition_manager>& pm,
  ss::sharded<cluster::topic_table>& tt,
  ss::sharded<archival::configuration>& config)
  : scheduler_service_impl(config.local(), api, pm, tt) {}

void scheduler_service_impl::rearm_timer() {
    (void)ss::with_gate(_gate, [this] {
        return reconcile_archivers()
          .finally([this] {
              if (_gate.is_closed()) {
                  return;
              }
              _timer.rearm(_jitter());
          })
          .handle_exception([this](std::exception_ptr e) {
              vlog(_rtclog.info, "Error in timer callback: {}", e);
          });
    });
}
ss::future<> scheduler_service_impl::start() {
    _timer.set_callback([this] { rearm_timer(); });
    _timer.rearm(_jitter());
    (void)run_uploads();
    return ss::now();
}

ss::future<> scheduler_service_impl::stop() {
    vlog(_rtclog.info, "Scheduler service stop");
    _timer.cancel();
    _as.request_abort(); // interrupt possible sleep
    return _remote.stop().then([this] {
        std::vector<ss::future<>> outstanding;
        for (auto& it : _queue) {
            auto fut = ss::with_semaphore(
              _stop_limit, 1, [it] { return it.second.archiver->stop(); });
            outstanding.emplace_back(std::move(fut));
        }
        return ss::do_with(
          std::move(outstanding),
          [this](std::vector<ss::future<>>& outstanding) {
              return ss::when_all_succeed(
                       outstanding.begin(), outstanding.end())
                .finally([this] { return _gate.close(); });
          });
    });
}

ss::lw_shared_ptr<ntp_archiver> scheduler_service_impl::get_upload_candidate() {
    return _queue.get_upload_candidate();
}

ss::future<> scheduler_service_impl::upload_topic_manifest(
  model::topic_namespace topic_ns, model::revision_id rev) {
    gate_guard gg(_gate);
    auto cfg = _topic_table.local().get_topic_cfg(topic_ns);
    if (!cfg) {
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
            cloud_storage::topic_manifest tm(*cfg, rev);
            auto key = tm.get_manifest_path();
            vlog(ctxlog.debug, "Topic manifest object key is '{}'", key);
            auto res = co_await _remote.upload_manifest(
              _conf.bucket_name, tm, fib);
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

ss::future<ss::stop_iteration> scheduler_service_impl::add_ntp_archiver(
  ss::lw_shared_ptr<ntp_archiver> archiver) {
    if (_gate.is_closed()) {
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::yes);
    }
    return archiver->download_manifest(_rtcnode).then(
      [this, archiver](cloud_storage::download_result result)
        -> ss::future<ss::stop_iteration> {
          auto ntp = archiver->get_ntp();
          switch (result) {
          case cloud_storage::download_result::success:
              _queue.insert(archiver);
              vlog(
                _rtclog.info,
                "Found manifest for partition {}",
                archiver->get_ntp());
              _probe.start_archiving_ntp();
              return ss::make_ready_future<ss::stop_iteration>(
                ss::stop_iteration::yes);
          case cloud_storage::download_result::notfound:
              _queue.insert(archiver);
              vlog(
                _rtclog.info,
                "Start archiving new partition {}",
                archiver->get_ntp());
              // Start topic manifest upload
              // asynchronously
              if (ntp.tp.partition == 0) {
                  // Upload manifest once per topic. GCS has strict
                  // limits for single object updates.
                  (void)upload_topic_manifest(
                    model::topic_namespace(ntp.ns, ntp.tp.topic),
                    archiver->get_revision_id());
              }
              _probe.start_archiving_ntp();
              return ss::make_ready_future<ss::stop_iteration>(
                ss::stop_iteration::yes);
          case cloud_storage::download_result::failed:
          case cloud_storage::download_result::timedout:
              vlog(_rtclog.warn, "Manifest download failed");
              return ss::make_exception_future<ss::stop_iteration>(
                ss::timed_out_error());
          }
          return ss::make_ready_future<ss::stop_iteration>(
            ss::stop_iteration::yes);
      });
}

ss::future<>
scheduler_service_impl::create_archivers(std::vector<model::ntp> to_create) {
    return ss::with_gate(
      _gate, [this, to_create = std::move(to_create)]() mutable {
          return ss::do_with(
            std::move(to_create), [this](std::vector<model::ntp>& to_create) {
                return ss::parallel_for_each(
                  to_create, [this](const model::ntp& ntp) {
                      storage::api& api = _storage_api.local();
                      storage::log_manager& lm = api.log_mgr();
                      auto log = lm.get(ntp);
                      if (!log.has_value()) {
                          return ss::now();
                      }
                      auto svc = ss::make_lw_shared<ntp_archiver>(
                        log->config(), _conf, _remote, _probe);
                      return ss::repeat([this, svc = std::move(svc)] {
                          return add_ntp_archiver(svc);
                      });
                  });
            });
      });
}

ss::future<>
scheduler_service_impl::remove_archivers(std::vector<model::ntp> to_remove) {
    gate_guard g(_gate);
    return ss::parallel_for_each(
             to_remove,
             [this](const model::ntp& ntp) -> ss::future<> {
                 vlog(_rtclog.info, "removing archiver for {}", ntp.path());
                 auto archiver = _queue[ntp];
                 return ss::with_semaphore(
                          _stop_limit,
                          1,
                          [archiver] { return archiver->stop(); })
                   .finally([this, ntp] {
                       vlog(_rtclog.info, "archiver stopped {}", ntp.path());
                       _queue.erase(ntp);
                       _probe.stop_archiving_ntp();
                   });
             })
      .finally([g = std::move(g)] {});
}

std::optional<model::offset>
scheduler_service_impl::get_high_watermark(const model::ntp& ntp) const {
    cluster::partition_manager& pm = _partition_manager.local();
    auto p = pm.get(ntp);
    if (p) {
        return p->high_watermark();
    }
    return std::nullopt;
}

ss::future<> scheduler_service_impl::reconcile_archivers() {
    gate_guard g(_gate);
    cluster::partition_manager& pm = _partition_manager.local();
    storage::api& api = _storage_api.local();
    storage::log_manager& lm = api.log_mgr();
    auto snapshot = lm.get_all_ntps();
    std::vector<model::ntp> to_remove;
    std::vector<model::ntp> to_create;
    // find ntps that exist in _svc_per_ntp but no longer present in log_manager
    _queue.copy_if(
      std::back_inserter(to_remove), [&snapshot, &pm](const model::ntp& ntp) {
          auto p = pm.get(ntp);
          return !snapshot.contains(ntp) || !p || !p->is_leader();
      });
    // find new ntps that present in the snapshot only
    std::copy_if(
      snapshot.begin(),
      snapshot.end(),
      std::back_inserter(to_create),
      [this, &pm](const model::ntp& ntp) {
          auto p = pm.get(ntp);
          return ntp.ns != model::redpanda_ns && !_queue.contains(ntp) && p
                 && p->is_leader();
      });
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

ss::future<> scheduler_service_impl::run_uploads() {
    gate_guard g(_gate);
    try {
        static constexpr ss::lowres_clock::duration initial_backoff = 100ms;
        static constexpr ss::lowres_clock::duration max_backoff = 10s;
        ss::lowres_clock::duration backoff = initial_backoff;
        while (!_gate.is_closed()) {
            int quota = _queue.size();
            std::vector<ss::future<ntp_archiver::batch_result>> flist;

            std::transform(
              boost::make_counting_iterator(0),
              boost::make_counting_iterator(quota),
              std::back_inserter(flist),
              [this](int) {
                  using result_t = ntp_archiver::batch_result;
                  auto archiver = _queue.get_upload_candidate();
                  storage::api& api = _storage_api.local();
                  storage::log_manager& lm = api.log_mgr();
                  vlog(
                    _rtclog.debug,
                    "Checking {} for S3 upload candidates",
                    archiver->get_ntp());
                  auto hwm = get_high_watermark(archiver->get_ntp());
                  if (hwm) {
                      return archiver->upload_next_candidates(
                        lm, *hwm, _rtcnode);
                  }
                  return ss::make_ready_future<result_t>(result_t{});
              });

            auto results = co_await ss::when_all_succeed(
              flist.begin(), flist.end());

            auto total = std::accumulate(
              results.begin(),
              results.end(),
              ntp_archiver::batch_result(),
              [](
                const ntp_archiver::batch_result& lhs,
                const ntp_archiver::batch_result& rhs) {
                  return ntp_archiver::batch_result{
                    .num_succeded = lhs.num_succeded + rhs.num_succeded,
                    .num_failed = lhs.num_failed + rhs.num_failed};
              });

            _probe.successful_upload(total.num_succeded);
            _probe.failed_upload(total.num_failed);

            if (total.num_failed != 0) {
                vlog(
                  _rtclog.error,
                  "Failed to upload {} segments out of {}",
                  total.num_failed,
                  total.num_succeded + total.num_failed);
            } else if (total.num_succeded != 0) {
                vlog(
                  _rtclog.debug,
                  "Successfuly upload {} segments",
                  total.num_succeded);
            }

            if (total.num_succeded == 0) {
                // The backoff algorithm here is used to prevent high CPU
                // utilization when redpanda is not receiving any data and there
                // is nothing to update. Also, we want to limit amount of
                // logging if nothing is uploaded because of bad configuration
                // or some other problem.
                //
                // We want to limit max backoff duration
                // to some reasonable value (e.g. 5s) because otherwise it can
                // grow very large disabling the archival storage
                vlog(
                  _rtclog.trace,
                  "Nothing to upload, applying backoff algorithm");
                co_await ss::sleep_abortable(
                  backoff + _backoff.next_jitter_duration(), _as);
                backoff *= 2;
                if (backoff > max_backoff) {
                    backoff = max_backoff;
                }
                continue;
            }
        }
    } catch (const ss::sleep_aborted&) {
        vlog(_rtclog.debug, "Upload loop aborted");
    } catch (const ss::gate_closed_exception&) {
        vlog(_rtclog.debug, "Upload loop aborted (gate closed)");
    } catch (const ss::abort_requested_exception&) {
        vlog(_rtclog.debug, "Upload loop aborted (abort requested)");
    } catch (...) {
        vlog(_rtclog.error, "Upload loop error: {}", std::current_exception());
        throw;
    }
}

} // namespace archival::internal
