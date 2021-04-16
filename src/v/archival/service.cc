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
#include "cluster/partition_manager.h"
#include "cluster/topic_table.h"
#include "config/configuration.h"
#include "config/property.h"
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
        config::shard_local_cfg().cloud_storage_max_connections.value())};
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
  , _gc_jitter(conf.gc_interval, 1ms)
  , _conn_limit(conf.connection_limit())
  , _stop_limit(conf.connection_limit()) {}

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
          .handle_exception([](std::exception_ptr e) {
              vlog(archival_log.info, "Error in timer callback: {}", e);
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
    vlog(archival_log.info, "Scheduler service stop");
    _timer.cancel();
    _as.request_abort();
    std::vector<ss::future<>> outstanding;
    for (auto& it : _queue) {
        auto fut = ss::with_semaphore(
          _stop_limit, 1, [it] { return it.second.archiver->stop(); });
        outstanding.emplace_back(std::move(fut));
    }
    return ss::do_with(
      std::move(outstanding), [this](std::vector<ss::future<>>& outstanding) {
          return ss::when_all_succeed(outstanding.begin(), outstanding.end())
            .then([this] { return _gate.close(); });
      });
}

ss::lw_shared_ptr<ntp_archiver> scheduler_service_impl::get_upload_candidate() {
    return _queue.get_upload_candidate();
}

ss::future<> scheduler_service_impl::upload_topic_manifest(
  model::topic_namespace_view view, model::revision_id rev) {
    gate_guard gg(_gate);
    auto cfg = _topic_table.local().get_topic_cfg(view);
    if (cfg) {
        try {
            auto units = co_await ss::get_units(_conn_limit, 1);
            vlog(archival_log.info, "Uploading topic manifest {}", view);
            s3::client client(_conf.client_config, _as);
            topic_manifest tm(*cfg, rev);
            auto [istr, size_bytes] = tm.serialize();
            auto key = tm.get_manifest_path();
            vlog(archival_log.debug, "Topic manifest object key is '{}'", key);
            std::vector<s3::object_tag> tags = {{"rp-type", "topic-manifest"}};
            co_await client.put_object(
              _conf.bucket_name,
              s3::object_key(key),
              size_bytes,
              std::move(istr),
              tags);
            co_await client.shutdown();
        } catch (const s3::rest_error_response& err) {
            vlog(
              archival_log.error,
              "REST API error occured during topic manifest upload: "
              "{}, code: {}, request-id: {}, resource: {}",
              err.message(),
              err.code(),
              err.request_id(),
              err.resource());
        } catch (...) {
            vlog(
              archival_log.error,
              "Exception occured during topic manifest upload: "
              "{}",
              std::current_exception());
        }
    }
}

ss::future<>
scheduler_service_impl::create_archivers(std::vector<model::ntp> to_create) {
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
                  log->config(), _conf);
                return ss::repeat([this, svc, ntp] {
                    return svc->download_manifest()
                      .then(
                        [this, svc](download_manifest_result result)
                          -> ss::future<ss::stop_iteration> {
                            switch (result) {
                            case download_manifest_result::success:
                                _queue.insert(svc);
                                vlog(
                                  archival_log.info,
                                  "Found manifest for partition {}",
                                  svc->get_ntp());
                                return ss::make_ready_future<
                                  ss::stop_iteration>(ss::stop_iteration::yes);
                            case download_manifest_result::notfound:
                                _queue.insert(svc);
                                vlog(
                                  archival_log.info,
                                  "Start archiving new partition {}",
                                  svc->get_ntp());
                                (void)upload_topic_manifest(
                                  model::topic_namespace_view(svc->get_ntp()),
                                  svc->get_revision_id());
                                return ss::make_ready_future<
                                  ss::stop_iteration>(ss::stop_iteration::yes);
                            case download_manifest_result::backoff:
                                vlog(
                                  archival_log.trace,
                                  "Manifest download exponential backoff");
                                return ss::sleep_abortable(
                                         _backoff.next_jitter_duration(), _as)
                                  .then([] {
                                      return ss::make_ready_future<
                                        ss::stop_iteration>(
                                        ss::stop_iteration::no);
                                  })
                                  .handle_exception_type([](const ss::
                                                              sleep_aborted&) {
                                      vlog(
                                        archival_log.debug,
                                        "Reconciliation loop abroted (sleep)");
                                      return ss::make_ready_future<
                                        ss::stop_iteration>(
                                        ss::stop_iteration::yes);
                                  });
                            }
                            return ss::make_ready_future<ss::stop_iteration>(
                              ss::stop_iteration::yes);
                        })
                      .handle_exception_type(
                        [ntp](const s3::rest_error_response& err) {
                            vlog(
                              archival_log.error,
                              "Manifest download for partition {} failed, "
                              "message: {}, code: {}, request-id: {}, "
                              "resource: {}",
                              ntp,
                              err.message(),
                              err.code_string(),
                              err.request_id(),
                              err.resource());
                            return ss::make_ready_future<ss::stop_iteration>(
                              ss::stop_iteration::yes);
                        })
                      .handle_exception([ntp](const std::exception_ptr& eptr) {
                          vlog(
                            archival_log.error,
                            "Manifest download for partition {} failed, "
                            "error: {}",
                            ntp,
                            eptr);
                          return ss::make_ready_future<ss::stop_iteration>(
                            ss::stop_iteration::yes);
                      });
                });
            });
      });
} // namespace archival::internal

ss::future<>
scheduler_service_impl::remove_archivers(std::vector<model::ntp> to_remove) {
    return ss::parallel_for_each(
      to_remove, [this](const model::ntp& ntp) -> ss::future<> {
          vlog(archival_log.info, "removing archiver for {}", ntp.path());
          auto archiver = _queue[ntp];
          return ss::with_semaphore(
                   _conn_limit, 1, [archiver] { return archiver->stop(); })
            .finally([this, ntp] {
                vlog(archival_log.info, "archiver stopped {}", ntp.path());
                _queue.erase(ntp);
            });
      });
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
            vlog(archival_log.error, "Failed to remove archivers");
        }
        if (create_fut.failed()) {
            vlog(archival_log.error, "Failed to create archivers");
        }
    }
}

ss::future<> scheduler_service_impl::run_uploads() {
    gate_guard g(_gate);
    try {
        const ss::lowres_clock::duration initial_backoff = 10ms;
        const ss::lowres_clock::duration max_backoff = 5s;
        ss::lowres_clock::duration backoff = initial_backoff;
        while (!_gate.is_closed()) {
            int quota = _queue.size();
            std::vector<ss::future<ntp_archiver::batch_result>> flist;

            std::transform(
              boost::make_counting_iterator(0),
              boost::make_counting_iterator(quota),
              std::back_inserter(flist),
              [this](int) {
                  auto archiver = _queue.get_upload_candidate();
                  storage::api& api = _storage_api.local();
                  storage::log_manager& lm = api.log_mgr();
                  vlog(
                    archival_log.debug,
                    "Checking {} for S3 upload candidates",
                    archiver->get_ntp());
                  return archiver->upload_next_candidates(_conn_limit, lm);
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

            if (total.num_succeded == 0 && total.num_failed == 0) {
                // The backoff algorithm here is used to prevent high CPU
                // utilization when redpanda is not receiving any data and there
                // is nothing to update. We want to limit max backoff duration
                // to some reasonable value (e.g. 5s) because otherwise it can
                // grow very large disabling the archival storage
                vlog(
                  archival_log.trace,
                  "Nothing to upload, applying backoff algorithm");
                co_await ss::sleep_abortable(
                  backoff + _backoff.next_jitter_duration(), _as);
                backoff *= 2;
                if (backoff > max_backoff) {
                    backoff = max_backoff;
                }
                continue;
            } else if (total.num_failed != 0) {
                vlog(
                  archival_log.error,
                  "Failed to upload {} segments out of {}",
                  total.num_failed,
                  total.num_succeded);
            } else {
                vlog(
                  archival_log.debug,
                  "Successfuly upload {} segments",
                  total.num_succeded);
            }
            // TODO: use probe to report num_succeded and num_failed
        }
    } catch (const ss::sleep_aborted&) {
        vlog(archival_log.debug, "Upload loop aborted");
    } catch (const ss::gate_closed_exception&) {
        vlog(archival_log.debug, "Upload loop aborted (gate closed)");
    } catch (const ss::abort_requested_exception&) {
        vlog(archival_log.debug, "Upload loop aborted (abort requested)");
    } catch (...) {
        vlog(
          archival_log.error,
          "Upload loop error: {}",
          std::current_exception());
        throw;
    }
}

} // namespace archival::internal
