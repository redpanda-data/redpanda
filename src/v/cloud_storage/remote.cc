/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/remote.h"

#include "bytes/iostream.h"
#include "cloud_storage/base_manifest.h"
#include "cloud_storage/logger.h"
#include "cloud_storage/materialized_resources.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/client_pool.h"
#include "cloud_storage_clients/types.h"
#include "cloud_storage_clients/util.h"
#include "model/metadata.h"
#include "ssx/future-util.h"
#include "ssx/semaphore.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/weak_ptr.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <boost/beast/http/error.hpp>
#include <boost/beast/http/field.hpp>
#include <boost/range/irange.hpp>
#include <fmt/chrono.h>

#include <exception>
#include <iterator>
#include <utility>
#include <variant>

namespace {
// Holds the key and associated retry_chain_node to make it
// easier to keep objects on heap using do_with, as retry_chain_node
// cannot be copied or moved.
struct key_and_node {
    cloud_storage_clients::object_key key;
    std::unique_ptr<retry_chain_node> node;
};

template<typename R>
requires std::ranges::range<R>
size_t num_chunks(const R& r, size_t max_batch_size) {
    const auto range_size = std::distance(r.begin(), r.end());

    if (range_size % max_batch_size == 0) {
        return range_size / max_batch_size;
    } else {
        return range_size / max_batch_size + 1;
    }
}
} // namespace

namespace cloud_storage {

using namespace std::chrono_literals;

remote::remote(
  ss::sharded<cloud_storage_clients::client_pool>& clients,
  const cloud_storage_clients::client_configuration& conf,
  model::cloud_credentials_source cloud_credentials_source)
  : _pool(clients)
  , _auth_refresh_bg_op{_gate, _as, conf, cloud_credentials_source}
  , _materialized(std::make_unique<materialized_resources>())
  , _probe(
      remote_metrics_disabled(static_cast<bool>(
        std::visit([](auto&& cfg) { return cfg.disable_metrics; }, conf))),
      remote_metrics_disabled(static_cast<bool>(std::visit(
        [](auto&& cfg) { return cfg.disable_public_metrics; }, conf))),
      *_materialized)
  , _azure_shared_key_binding(
      config::shard_local_cfg().cloud_storage_azure_shared_key.bind())
  , _cloud_storage_backend{
      cloud_storage_clients::infer_backend_from_configuration(
        conf, cloud_credentials_source)} {
    vlog(
      cst_log.info,
      "remote initialized with backend {}",
      _cloud_storage_backend);
    // If the credentials source is from config file, bypass the background
    // op to refresh credentials periodically, and load pool with static
    // credentials right now.
    if (_auth_refresh_bg_op.is_static_config()) {
        _pool.local().load_credentials(
          _auth_refresh_bg_op.build_static_credentials());
    }

    _azure_shared_key_binding.watch([this] {
        auto current_config = _auth_refresh_bg_op.get_client_config();
        if (!std::holds_alternative<cloud_storage_clients::abs_configuration>(
              current_config)) {
            vlog(
              cst_log.warn,
              "Attempt to set cloud_storage_azure_shared_key for cluster using "
              "S3 detected");
            return;
        }

        vlog(
          cst_log.info,
          "cloud_storage_azure_shared_key was updated. Refreshing "
          "credentials.");

        auto new_shared_key = _azure_shared_key_binding();
        if (!new_shared_key) {
            vlog(
              cst_log.info,
              "cloud_storage_azure_shared_key was unset. Will continue "
              "using the previous value until restart.");

            return;
        }

        auto& abs_config = std::get<cloud_storage_clients::abs_configuration>(
          current_config);
        abs_config.shared_key = cloud_roles::private_key_str{*new_shared_key};
        _auth_refresh_bg_op.set_client_config(std::move(current_config));

        _pool.local().load_credentials(
          _auth_refresh_bg_op.build_static_credentials());
    });
}

remote::remote(
  ss::sharded<cloud_storage_clients::client_pool>& pool,
  const configuration& conf)
  : remote(pool, conf.client_config, conf.cloud_credentials_source) {}

remote::~remote() {
    // This is declared in the .cc to avoid header trying to
    // link with destructors for unique_ptr wrapped members
}

ss::future<> remote::start() {
    if (!_auth_refresh_bg_op.is_static_config()) {
        // Launch background operation to fetch credentials on
        // auth_refresh_shard_id, and copy them to other shards. We do not wait
        // for this operation here, the wait is done in client_pool::acquire to
        // avoid delaying application startup.
        _auth_refresh_bg_op.maybe_start_auth_refresh_op(
          [this](auto credentials) {
              return propagate_credentials(credentials);
          });
    }

    co_await _materialized->start();
}

ss::future<> remote::stop() {
    cst_log.debug("Stopping remote...");
    _as.request_abort();
    co_await _materialized->stop();
    co_await _gate.close();
    co_await _auth_refresh_bg_op.stop();
    cst_log.debug("Stopped remote...");
}

size_t remote::concurrency() const { return _pool.local().max_size(); }

model::cloud_storage_backend remote::backend() const {
    return _cloud_storage_backend;
}

bool remote::is_batch_delete_supported() const {
    return delete_objects_max_keys() > 1;
}

int remote::delete_objects_max_keys() const {
    switch (_cloud_storage_backend) {
    case model::cloud_storage_backend::aws:
        [[fallthrough]];
    case model::cloud_storage_backend::minio:
        // https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
        return 1000;
    case model::cloud_storage_backend::google_s3_compat:
        [[fallthrough]];
    case model::cloud_storage_backend::azure:
        // Will be supported once azurite supports batch blob delete
        [[fallthrough]];
    case model::cloud_storage_backend::unknown:
        return 1;
    }
}

ss::future<download_result> remote::download_manifest(
  const cloud_storage_clients::bucket_name& bucket,
  const std::pair<manifest_format, remote_manifest_path>& format_key,
  base_manifest& manifest,
  retry_chain_node& parent) {
    return do_download_manifest(bucket, format_key, manifest, parent);
}

ss::future<download_result> remote::download_manifest(
  const cloud_storage_clients::bucket_name& bucket,
  const remote_manifest_path& key,
  base_manifest& manifest,
  retry_chain_node& parent) {
    auto fk = std::pair{manifest_format::json, key};
    co_return co_await do_download_manifest(
      bucket, fk, manifest, parent, false);
}
ss::future<download_result> remote::maybe_download_manifest(
  const cloud_storage_clients::bucket_name& bucket,
  const remote_manifest_path& key,
  base_manifest& manifest,
  retry_chain_node& parent) {
    auto fk = std::pair{manifest_format::json, key};
    co_return co_await do_download_manifest(bucket, fk, manifest, parent, true);
}

ss::future<std::pair<download_result, manifest_format>>
remote::try_download_partition_manifest(
  const cloud_storage_clients::bucket_name& bucket,
  partition_manifest& manifest,
  retry_chain_node& parent,
  bool expect_missing) {
    vassert(
      manifest.get_ntp() != model::ntp{}
        && manifest.get_revision_id() != model::initial_revision_id{},
      "partition manifest must have ntp");

    // first try to download the serde format
    auto format_path = manifest.get_manifest_format_and_path();
    auto serde_result = co_await do_download_manifest(
      bucket, format_path, manifest, parent, expect_missing);
    if (serde_result != download_result::notfound) {
        // propagate success, timedout and failed to caller
        co_return std::pair{serde_result, manifest_format::serde};
    }
    // fallback to json format
    format_path = manifest.get_legacy_manifest_format_and_path();
    co_return std::pair{
      co_await do_download_manifest(
        bucket, format_path, manifest, parent, expect_missing),
      manifest_format::json};
}

ss::future<download_result> remote::do_download_manifest(
  const cloud_storage_clients::bucket_name& bucket,
  const std::pair<manifest_format, remote_manifest_path>& format_key,
  base_manifest& manifest,
  retry_chain_node& parent,
  bool expect_missing) {
    auto guard = _gate.hold();
    retry_chain_node fib(&parent);
    retry_chain_logger ctxlog(cst_log, fib);
    auto path = cloud_storage_clients::object_key(format_key.second().native());
    auto lease = co_await _pool.local().acquire(fib.root_abort_source());
    auto retry_permit = fib.retry();
    std::optional<download_result> result;
    vlog(ctxlog.debug, "Download manifest {}", format_key.second());
    while (!_gate.is_closed() && retry_permit.is_allowed
           && !result.has_value()) {
        notify_external_subscribers(
          api_activity_notification{
            .type = api_activity_type::manifest_download,
            .is_retry = fib.retry_count() > 1},
          parent);
        auto resp = co_await lease.client->get_object(
          bucket, path, fib.get_timeout(), expect_missing);

        if (resp) {
            vlog(ctxlog.debug, "Receive OK response from {}", path);
            try {
                co_await manifest.update(
                  format_key.first, resp.value()->as_input_stream());

                switch (manifest.get_manifest_type()) {
                case manifest_type::partition:
                    _probe.partition_manifest_download();
                    break;
                case manifest_type::topic:
                    _probe.topic_manifest_download();
                    break;
                case manifest_type::tx_range:
                    _probe.txrange_manifest_download();
                    break;
                case manifest_type::cluster_metadata:
                    _probe.cluster_metadata_manifest_download();
                    break;
                case manifest_type::spillover:
                    _probe.spillover_manifest_download();
                    break;
                }
                co_return download_result::success;
            } catch (...) {
                // Draining the response stream may throw I/O errors: convert
                // those into an error outcome.
                resp
                  = cloud_storage_clients::util::handle_client_transport_error(
                    std::current_exception(), cst_log);
            }
        }

        lease.client->shutdown();

        switch (resp.error()) {
        case cloud_storage_clients::error_outcome::retry:
            vlog(
              ctxlog.debug,
              "Downloading manifest from {}, {} backoff required",
              bucket,
              std::chrono::duration_cast<std::chrono::milliseconds>(
                retry_permit.delay));
            _probe.manifest_download_backoff();
            co_await ss::sleep_abortable(
              retry_permit.delay, fib.root_abort_source());
            retry_permit = fib.retry();
            break;
        case cloud_storage_clients::error_outcome::fail:
            result = download_result::failed;
            vlog(
              ctxlog.warn,
              "Failed downloading manifest from {} {}, manifest at {}",
              bucket,
              *result,
              path);
            break;
        case cloud_storage_clients::error_outcome::key_not_found:
            result = download_result::notfound;
            vlog(
              ctxlog.debug,
              "Manifest from {} {}, manifest at {} not found",
              bucket,
              *result,
              path);
            break;
        }
    }
    _probe.failed_manifest_download();
    if (!result) {
        vlog(
          ctxlog.warn,
          "Downloading manifest from {}, backoff quota exceded, manifest at {} "
          "not available",
          bucket,
          path);
        result = download_result::timedout;
    }
    co_return *result;
}

ss::future<upload_result> remote::upload_manifest(
  const cloud_storage_clients::bucket_name& bucket,
  const base_manifest& manifest,
  retry_chain_node& parent) {
    auto key = manifest.get_manifest_path();
    co_return co_await upload_manifest(bucket, manifest, key, parent);
}

ss::future<upload_result> remote::upload_manifest(
  const cloud_storage_clients::bucket_name& bucket,
  const base_manifest& manifest,
  const remote_manifest_path& key,
  retry_chain_node& parent) {
    auto guard = _gate.hold();
    retry_chain_node fib(&parent);
    retry_chain_logger ctxlog(cst_log, fib);
    auto path = cloud_storage_clients::object_key(key());
    auto lease = co_await _pool.local().acquire(fib.root_abort_source());
    auto permit = fib.retry();
    vlog(ctxlog.debug, "Uploading manifest {} to the {}", path, bucket());
    std::optional<upload_result> result;
    while (!_gate.is_closed() && permit.is_allowed && !result.has_value()) {
        notify_external_subscribers(
          api_activity_notification{
            .type = api_activity_type::manifest_upload,
            .is_retry = fib.retry_count() > 1},
          parent);
        auto [is, size] = co_await manifest.serialize();
        const auto res = co_await lease.client->put_object(
          bucket, path, size, std::move(is), fib.get_timeout());

        if (res) {
            vlog(
              ctxlog.debug,
              "Successfuly uploaded {} manifest to {}",
              manifest.get_manifest_type(),
              path);
            switch (manifest.get_manifest_type()) {
            case manifest_type::partition:
                _probe.partition_manifest_upload();
                break;
            case manifest_type::topic:
                _probe.topic_manifest_upload();
                break;
            case manifest_type::tx_range:
                _probe.txrange_manifest_upload();
                break;
            case manifest_type::cluster_metadata:
                _probe.cluster_metadata_manifest_upload();
                break;
            case manifest_type::spillover:
                _probe.spillover_manifest_upload();
                break;
            }
            _probe.register_upload_size(size);
            co_return upload_result::success;
        }

        lease.client->shutdown();

        switch (res.error()) {
        case cloud_storage_clients::error_outcome::retry:
            vlog(
              ctxlog.debug,
              "Uploading manifest {} to {}, {} backoff required",
              path,
              bucket,
              std::chrono::duration_cast<std::chrono::milliseconds>(
                permit.delay));
            _probe.manifest_upload_backoff();
            co_await ss::sleep_abortable(permit.delay, fib.root_abort_source());
            permit = fib.retry();
            break;
        case cloud_storage_clients::error_outcome::key_not_found:
            // not expected during upload
            [[fallthrough]];
        case cloud_storage_clients::error_outcome::fail:
            result = upload_result::failed;
            break;
        }
    }
    _probe.failed_manifest_upload();
    if (!result) {
        vlog(
          ctxlog.warn,
          "Uploading manifest {} to {}, backoff quota exceded, manifest not "
          "uploaded",
          path,
          bucket);
        result = upload_result::timedout;
    } else {
        vlog(
          ctxlog.warn,
          "Uploading manifest {} to {}, {}, manifest not uploaded",
          path,
          *result,
          bucket);
    }
    co_return *result;
}

void remote::notify_external_subscribers(
  api_activity_notification event, const retry_chain_node& caller) {
    const auto* caller_root = caller.get_root();

    for (auto& flt : _filters) {
        if (flt._events_to_ignore.contains(event.type)) {
            continue;
        }

        if (flt._sources_to_ignore.contains(caller_root)) {
            continue;
        }

        // Invariant: the filter._promise is always initialized
        // by the 'subscribe' method.
        vassert(
          flt._promise.has_value(),
          "Filter object is not initialized properly");
        flt._promise->set_value(event);
        flt._promise = std::nullopt;
        // NOTE: the filter object can be reused by the owner
    }

    _filters.remove_if(
      [](const event_filter& f) { return !f._promise.has_value(); });
}

ss::future<upload_result> remote::upload_controller_snapshot(
  const cloud_storage_clients::bucket_name& bucket,
  const remote_segment_path& remote_path,
  const ss::file& file,
  retry_chain_node& parent,
  lazy_abort_source& lazy_abort_source) {
    auto reset_str = [&file] {
        using provider_t = std::unique_ptr<storage::stream_provider>;
        ss::file_input_stream_options opts;
        return ss::make_ready_future<provider_t>(
          std::make_unique<storage::segment_reader_handle>(
            ss::make_file_input_stream(file, opts)));
    };
    auto file_size = co_await file.size();
    co_return co_await upload_stream(
      bucket,
      remote_path,
      file_size,
      reset_str,
      parent,
      lazy_abort_source,
      "controller snapshot",
      api_activity_type::controller_snapshot_upload,
      [this] { _probe.controller_snapshot_failed_upload(); },
      [this] { _probe.controller_snapshot_successful_upload(); },
      [this] { _probe.controller_snapshot_upload_backoff(); },
      std::nullopt);
}

template<
  typename FailedUploadMetricFn,
  typename SuccessfulUploadMetricFn,
  typename UploadBackoffMetricFn>
ss::future<upload_result> remote::upload_stream(
  const cloud_storage_clients::bucket_name& bucket,
  const remote_segment_path& segment_path,
  uint64_t content_length,
  const reset_input_stream& reset_str,
  retry_chain_node& parent,
  lazy_abort_source& lazy_abort_source,
  const std::string_view stream_label,
  api_activity_type event_type,
  FailedUploadMetricFn failed_upload_metric,
  SuccessfulUploadMetricFn successful_upload_metric,
  UploadBackoffMetricFn upload_backoff_metric,
  std::optional<size_t> max_retries) {
    auto guard = _gate.hold();
    retry_chain_node fib(&parent);
    retry_chain_logger ctxlog(cst_log, fib);
    auto permit = fib.retry();
    vlog(
      ctxlog.debug,
      "Uploading {} to path {}, length {}",
      stream_label,
      segment_path,
      content_length);
    std::optional<upload_result> result;
    while (!_gate.is_closed() && permit.is_allowed && !result
           && max_retries.value_or(1) > 0) {
        if (max_retries.has_value()) {
            max_retries = max_retries.value() - 1;
        }
        auto lease = co_await _pool.local().acquire(fib.root_abort_source());
        notify_external_subscribers(
          api_activity_notification{
            .type = event_type, .is_retry = fib.retry_count() > 1},
          parent);

        // Client acquisition can take some time. Do a check before starting
        // the upload if we can still continue.
        if (lazy_abort_source.abort_requested()) {
            vlog(
              ctxlog.warn,
              "{}: cancelled uploading {} to {}",
              lazy_abort_source.abort_reason(),
              segment_path,
              bucket);
            failed_upload_metric();
            co_return upload_result::cancelled;
        }

        auto reader_handle = co_await reset_str();
        auto path = cloud_storage_clients::object_key(segment_path());
        // Segment upload attempt
        auto res = co_await lease.client->put_object(
          bucket,
          path,
          content_length,
          reader_handle->take_stream(),
          fib.get_timeout());

        // `put_object` closed the encapsulated input_stream, but we must
        // call close() on the segment_reader_handle to release the FD.
        co_await reader_handle->close();

        if (res) {
            successful_upload_metric();
            _probe.register_upload_size(content_length);
            co_return upload_result::success;
        }

        lease.client->shutdown();
        switch (res.error()) {
        case cloud_storage_clients::error_outcome::retry:
            vlog(
              ctxlog.debug,
              "Uploading {} {} to {}, {} backoff required",
              stream_label,
              path,
              bucket,
              std::chrono::duration_cast<std::chrono::milliseconds>(
                permit.delay));
            upload_backoff_metric();
            if (!lazy_abort_source.abort_requested()) {
                co_await ss::sleep_abortable(
                  permit.delay, fib.root_abort_source());
            }
            permit = fib.retry();
            break;
        case cloud_storage_clients::error_outcome::key_not_found:
            // not expected during upload
            [[fallthrough]];
        case cloud_storage_clients::error_outcome::fail:
            result = upload_result::failed;
            break;
        }
    }

    failed_upload_metric();

    if (!result) {
        vlog(
          ctxlog.warn,
          "Uploading {} {} to {}, backoff quota exceded, {} not "
          "uploaded",
          stream_label,
          segment_path,
          bucket,
          stream_label);
    } else {
        vlog(
          ctxlog.warn,
          "Uploading {} {} to {}, {}, segment not uploaded",
          stream_label,
          segment_path,
          bucket,
          *result);
    }
    co_return upload_result::timedout;
}

ss::future<upload_result> remote::upload_segment(
  const cloud_storage_clients::bucket_name& bucket,
  const remote_segment_path& segment_path,
  uint64_t content_length,
  const reset_input_stream& reset_str,
  retry_chain_node& parent,
  lazy_abort_source& lazy_abort_source,
  std::optional<size_t> max_retries) {
    return upload_stream(
      bucket,
      segment_path,
      content_length,
      reset_str,
      parent,
      lazy_abort_source,
      "segment",
      api_activity_type::segment_upload,
      [this] { _probe.failed_upload(); },
      [this] { _probe.successful_upload(); },
      [this] { _probe.upload_backoff(); },
      max_retries);
}

ss::future<download_result> remote::download_stream(
  const cloud_storage_clients::bucket_name& bucket,
  const remote_segment_path& path,
  const try_consume_stream& cons_str,
  retry_chain_node& parent,
  const std::string_view stream_label,
  const download_metrics& metrics,
  std::optional<cloud_storage_clients::http_byte_range> byte_range) {
    return download_stream(
      bucket,
      path,
      cons_str,
      parent,
      stream_label,
      false,
      [&metrics] { return metrics.download_latency_measurement(); },
      [&metrics] { metrics.failed_download_metric(); },
      [&metrics] { metrics.download_backoff_metric(); },
      byte_range);
}

ss::future<download_result> remote::download_segment(
  const cloud_storage_clients::bucket_name& bucket,
  const remote_segment_path& segment_path,
  const try_consume_stream& cons_str,
  retry_chain_node& parent,
  std::optional<cloud_storage_clients::http_byte_range> byte_range) {
    return download_stream(
      bucket,
      segment_path,
      cons_str,
      parent,
      "segment",
      true,
      [this] { return _probe.segment_download(); },
      [this] { _probe.failed_download(); },
      [this] { _probe.download_backoff(); },
      byte_range);
}

template<
  typename DownloadLatencyMeasurementFn,
  typename FailedDownloadMetricFn,
  typename DownloadBackoffMetricFn>
ss::future<download_result> remote::download_stream(
  const cloud_storage_clients::bucket_name& bucket,
  const remote_segment_path& segment_path,
  const try_consume_stream& cons_str,
  retry_chain_node& parent,
  const std::string_view stream_label,
  bool acquire_hydration_units,
  DownloadLatencyMeasurementFn download_latency_measurement,
  FailedDownloadMetricFn failed_download_metric,
  DownloadBackoffMetricFn download_backoff_metric,
  std::optional<cloud_storage_clients::http_byte_range> byte_range) {
    auto guard = _gate.hold();
    retry_chain_node fib(&parent);
    retry_chain_logger ctxlog(cst_log, fib);
    auto path = cloud_storage_clients::object_key(segment_path());

    ssx::semaphore_units hu;
    if (acquire_hydration_units) {
        hu = co_await _materialized->get_hydration_units(1);
    }

    auto lease = co_await [this, &fib] {
        auto m = _probe.client_acquisition();
        return _pool.local().acquire(fib.root_abort_source());
    }();

    auto permit = fib.retry();
    vlog(ctxlog.debug, "Download {} {}", stream_label, path);
    std::optional<download_result> result;
    while (!_gate.is_closed() && permit.is_allowed && !result) {
        notify_external_subscribers(
          api_activity_notification{
            .type = api_activity_type::segment_download,
            .is_retry = fib.retry_count() > 1},
          parent);

        auto download_latency_measure = download_latency_measurement();
        auto resp = co_await lease.client->get_object(
          bucket, path, fib.get_timeout(), false, byte_range);

        if (resp) {
            vlog(ctxlog.debug, "Receive OK response from {}", path);
            auto length = boost::lexical_cast<uint64_t>(
              resp.value()->get_headers().at(
                boost::beast::http::field::content_length));
            try {
                auto underlying_st = resp.value()->as_input_stream();
                auto throttled_st = _materialized->throttle_download(
                  std::move(underlying_st), _as);
                uint64_t content_length = co_await cons_str(
                  length, std::move(throttled_st));
                _probe.successful_download();
                _probe.register_download_size(content_length);
                co_return download_result::success;
            } catch (...) {
                const auto ex = std::current_exception();
                vlog(
                  ctxlog.debug,
                  "unexpected error when consuming stream {}",
                  ex);
                resp
                  = cloud_storage_clients::util::handle_client_transport_error(
                    ex, cst_log);
            }
        }

        download_latency_measure.reset();

        lease.client->shutdown();

        switch (resp.error()) {
        case cloud_storage_clients::error_outcome::retry:
            vlog(
              ctxlog.debug,
              "Downloading {} from {}, {} backoff required",
              stream_label,
              bucket,
              std::chrono::duration_cast<std::chrono::milliseconds>(
                permit.delay));
            download_backoff_metric();
            co_await ss::sleep_abortable(permit.delay, fib.root_abort_source());
            permit = fib.retry();
            break;
        case cloud_storage_clients::error_outcome::fail:
            result = download_result::failed;
            break;
        case cloud_storage_clients::error_outcome::key_not_found:
            result = download_result::notfound;
            break;
        }
    }
    failed_download_metric();
    if (!result) {
        vlog(
          ctxlog.warn,
          "Downloading {} from {}, backoff quota exceded, segment at {} "
          "not available",
          stream_label,
          bucket,
          path);
        result = download_result::timedout;
    } else {
        vlog(
          ctxlog.warn,
          "Downloading {} from {}, {}, segment at {} not available",
          stream_label,
          bucket,
          *result,
          path);
    }
    co_return *result;
}

ss::future<download_result> remote::download_index(
  const cloud_storage_clients::bucket_name& bucket,
  const remote_segment_path& index_path,
  offset_index& ix,
  retry_chain_node& parent) {
    iobuf buffer;
    const auto dl_result = co_await download_object(
      {.transfer_details = {
         .bucket = bucket,
         .key = cloud_storage_clients::object_key{index_path},
         .parent_rtc = parent,
         .success_cb = [](auto& probe) { probe.index_download(); },
         .failure_cb = [](auto& probe) { probe.failed_index_download(); },
         .backoff_cb = [](auto& probe) { probe.download_backoff(); }},
       .type = download_type::segment_index,
       .payload = buffer});
    if (dl_result == download_result::success) {
        ix.from_iobuf(std::move(buffer));
    }
    co_return dl_result;
}

ss::future<download_result>
remote::download_object(cloud_storage::download_request download_request) {
    auto guard = _gate.hold();
    auto& transfer_details = download_request.transfer_details;
    retry_chain_node fib(&transfer_details.parent_rtc);
    retry_chain_logger ctxlog(cst_log, fib);

    const auto path = transfer_details.key;
    const auto bucket = transfer_details.bucket;
    const auto object_type = download_request.type;

    auto lease = co_await _pool.local().acquire(fib.root_abort_source());

    auto permit = fib.retry();
    vlog(ctxlog.debug, "Downloading {} from {}", object_type, path);

    std::optional<download_result> result;
    while (!_gate.is_closed() && permit.is_allowed && !result) {
        notify_external_subscribers(
          api_activity_notification{
            .type = api_activity_type::object_download,
            .is_retry = fib.retry_count() > 1},
          transfer_details.parent_rtc);
        auto resp = co_await lease.client->get_object(
          bucket, path, fib.get_timeout());

        if (resp) {
            vlog(ctxlog.debug, "Receive OK response from {}", path);
            auto buffer
              = co_await cloud_storage_clients::util::drain_response_stream(
                resp.value());
            download_request.payload.append_fragments(std::move(buffer));
            transfer_details.on_success(_probe);
            co_return download_result::success;
        }

        lease.client->shutdown();

        switch (resp.error()) {
        case cloud_storage_clients::error_outcome::retry:
            vlog(
              ctxlog.debug,
              "Downloading {} from {}, {} backoff required",
              object_type,
              bucket,
              std::chrono::duration_cast<std::chrono::milliseconds>(
                permit.delay));
            transfer_details.on_backoff(_probe);
            co_await ss::sleep_abortable(permit.delay, fib.root_abort_source());
            permit = fib.retry();
            break;
        case cloud_storage_clients::error_outcome::fail:
            result = download_result::failed;
            break;
        case cloud_storage_clients::error_outcome::key_not_found:
            result = download_result::notfound;
            break;
        }
    }
    transfer_details.on_failure(_probe);
    if (!result) {
        vlog(
          ctxlog.warn,
          "Downloading {} from {}, backoff quota exceded, {} at {} "
          "not available",
          object_type,
          bucket,
          object_type,
          path);
        result = download_result::timedout;
    } else {
        vlog(
          ctxlog.warn,
          "Downloading {} from {}, {}, {} at {} not available",
          object_type,
          bucket,
          *result,
          object_type,
          path);
    }
    co_return *result;
}

ss::future<download_result> remote::object_exists(
  const cloud_storage_clients::bucket_name& bucket,
  const cloud_storage_clients::object_key& path,
  retry_chain_node& parent,
  existence_check_type object_type) {
    ss::gate::holder gh{_gate};
    retry_chain_node fib(&parent);
    retry_chain_logger ctxlog(cst_log, fib);
    auto lease = co_await _pool.local().acquire(fib.root_abort_source());
    auto permit = fib.retry();
    vlog(ctxlog.debug, "Check {} {}", object_type, path);
    std::optional<download_result> result;
    while (!_gate.is_closed() && permit.is_allowed && !result) {
        auto resp = co_await lease.client->head_object(
          bucket, path, fib.get_timeout());
        if (resp) {
            vlog(
              ctxlog.debug,
              "Receive OK HeadObject response from {}, object size: {}, etag: "
              "{}",
              path,
              resp.value().object_size,
              resp.value().etag);
            co_return download_result::success;
        }

        // Error path
        lease.client->shutdown();
        switch (resp.error()) {
        case cloud_storage_clients::error_outcome::retry:
            vlog(
              ctxlog.debug,
              "HeadObject from {}, {} backoff required",
              bucket,
              std::chrono::duration_cast<std::chrono::milliseconds>(
                permit.delay));
            co_await ss::sleep_abortable(permit.delay, fib.root_abort_source());
            permit = fib.retry();
            break;
        case cloud_storage_clients::error_outcome::fail:
            result = download_result::failed;
            break;
        case cloud_storage_clients::error_outcome::key_not_found:
            result = download_result::notfound;
            break;
        }
    }
    if (!result) {
        vlog(
          ctxlog.warn,
          "HeadObject from {}, backoff quota exceded, {} at {} "
          "not available",
          bucket,
          object_type,
          path);
        result = download_result::timedout;
    } else {
        vlog(
          ctxlog.warn,
          "HeadObject from {}, {}, {} at {} not available",
          bucket,
          *result,
          object_type,
          path);
    }
    co_return *result;
}

ss::future<download_result> remote::segment_exists(
  const cloud_storage_clients::bucket_name& bucket,
  const remote_segment_path& segment_path,
  retry_chain_node& parent) {
    co_return co_await object_exists(
      bucket,
      cloud_storage_clients::object_key{segment_path},
      parent,
      existence_check_type::segment);
}

ss::future<remote::partition_manifest_existence>
remote::partition_manifest_exists(
  const cloud_storage_clients::bucket_name& bucket,
  model::ntp ntp,
  model::initial_revision_id rev_id,
  retry_chain_node& parent) {
    // first check serde and exit early if it exists
    auto serde_res = co_await object_exists(
      bucket,
      cloud_storage_clients::object_key{generate_partition_manifest_path(
        ntp, rev_id, manifest_format::serde)()},
      parent,
      existence_check_type::manifest);

    switch (serde_res) {
    case download_result::success:
        co_return partition_manifest_existence{
          download_result::success, manifest_format::serde};
    case download_result::notfound: {
        auto json_res = co_await object_exists(
          bucket,
          cloud_storage_clients::object_key{generate_partition_manifest_path(
            ntp, rev_id, manifest_format::json)()},
          parent,
          existence_check_type::manifest);
        co_return partition_manifest_existence{json_res, manifest_format::json};
    }
    case download_result::failed:
    case download_result::timedout:
        // do not try to check for json in case of failures
        co_return partition_manifest_existence{serde_res, {}};
    }
}

ss::future<upload_result> remote::delete_object(
  const cloud_storage_clients::bucket_name& bucket,
  const cloud_storage_clients::object_key& path,
  retry_chain_node& parent) {
    ss::gate::holder gh{_gate};
    retry_chain_node fib(&parent);
    retry_chain_logger ctxlog(cst_log, fib);
    auto lease = co_await _pool.local().acquire(fib.root_abort_source());
    auto permit = fib.retry();
    vlog(ctxlog.debug, "Delete object {}", path);
    std::optional<upload_result> result;
    while (!_gate.is_closed() && permit.is_allowed && !result) {
        notify_external_subscribers(
          api_activity_notification{
            .type = api_activity_type::segment_delete,
            .is_retry = fib.retry_count() > 1},
          parent);
        // NOTE: DeleteObject in S3 doesn't return an error
        // if the object doesn't exist. Because of that we're
        // using 'upload_result' type as a return type. No need
        // to handle NoSuchKey error. The 'upload_result' represents
        // any mutable operation.
        auto res = co_await lease.client->delete_object(
          bucket, path, fib.get_timeout());

        if (res) {
            co_return upload_result::success;
        }

        lease.client->shutdown();

        switch (res.error()) {
        case cloud_storage_clients::error_outcome::retry:
            vlog(
              ctxlog.debug,
              "DeleteObject {}, {} backoff required",
              bucket,
              std::chrono::duration_cast<std::chrono::milliseconds>(
                permit.delay));
            co_await ss::sleep_abortable(permit.delay, fib.root_abort_source());
            permit = fib.retry();
            break;
        case cloud_storage_clients::error_outcome::fail:
            result = upload_result::failed;
            break;
        case cloud_storage_clients::error_outcome::key_not_found:
            vassert(
              false,
              "Unexpected notfound outcome received when deleting object {} "
              "from bucket {}",
              path,
              bucket);
            break;
        }
    }
    if (!result) {
        vlog(
          ctxlog.warn,
          "DeleteObject {}, {}, backoff quota exceded, object not deleted",
          path,
          bucket);
        result = upload_result::timedout;
    } else {
        vlog(
          ctxlog.warn,
          "DeleteObject {}, {}, object not deleted, error: {}",
          path,
          bucket,
          *result);
    }
    co_return *result;
}

template<typename R>
requires std::ranges::range<R>
         && std::same_as<
           std::ranges::range_value_t<R>,
           cloud_storage_clients::object_key>
ss::future<upload_result> remote::delete_objects(
  const cloud_storage_clients::bucket_name& bucket,
  R keys,
  retry_chain_node& parent) {
    ss::gate::holder gh{_gate};
    retry_chain_logger ctxlog(cst_log, parent);

    if (keys.empty()) {
        vlog(ctxlog.info, "No keys to delete, returning");
        co_return upload_result::success;
    }

    vlog(ctxlog.debug, "Deleting objects count {}", keys.size());

    if (!is_batch_delete_supported()) {
        co_return co_await delete_objects_sequentially(
          bucket, std::forward<R>(keys), parent);
    }

    const auto batches_to_delete = num_chunks(keys, delete_objects_max_keys());
    std::vector<upload_result> results;
    results.reserve(batches_to_delete);

    co_await ss::max_concurrent_for_each(
      boost::irange(batches_to_delete),
      concurrency(),
      [this, bucket, &keys, &parent, &results](auto chunk_ix) -> ss::future<> {
          auto chunk_start_offset = (chunk_ix * delete_objects_max_keys());

          auto chunk_begin = keys.begin();
          std::advance(chunk_begin, chunk_start_offset);

          auto chunk_end = chunk_begin;
          if (
            delete_objects_max_keys()
            < std::distance(chunk_begin, keys.end())) {
              std::advance(chunk_end, delete_objects_max_keys());
          } else {
              chunk_end = keys.end();
          }

          std::vector<cloud_storage_clients::object_key> key_batch;
          key_batch.insert(
            key_batch.end(),
            std::make_move_iterator(chunk_begin),
            std::make_move_iterator(chunk_end));

          vassert(
            key_batch.size() > 0,
            "The chunking logic must always produce non-empty batches.");

          return delete_object_batch(bucket, std::move(key_batch), parent)
            .then([&results](auto result) { results.push_back(result); });
      });

    if (results.empty()) {
        vlog(ctxlog.error, "No keys were deleted");
        co_return upload_result::failed;
    }

    co_return std::reduce(
      results.begin(),
      results.end(),
      upload_result::success,
      [](auto res_a, auto res_b) {
          if (res_a != upload_result::success) {
              return res_a;
          }

          return res_b;
      });
}

ss::future<upload_result> remote::delete_object_batch(
  const cloud_storage_clients::bucket_name& bucket,
  std::vector<cloud_storage_clients::object_key> keys,
  retry_chain_node& parent) {
    ss::gate::holder gh{_gate};

    retry_chain_node fib(&parent);
    retry_chain_logger ctxlog(cst_log, fib);
    auto lease = co_await _pool.local().acquire(fib.root_abort_source());
    auto permit = fib.retry();
    vlog(ctxlog.debug, "Deleting a batch of size {}", keys.size());
    std::optional<upload_result> result;
    while (!_gate.is_closed() && permit.is_allowed && !result) {
        notify_external_subscribers(
          api_activity_notification{
            .type = api_activity_type::segment_delete,
            .is_retry = fib.retry_count() > 1},
          parent);
        auto res = co_await lease.client->delete_objects(
          bucket, keys, fib.get_timeout());

        if (res) {
            if (!res.value().undeleted_keys.empty()) {
                vlog(
                  ctxlog.debug,
                  "{} objects were not deleted by plural delete; first "
                  "failure: {{key: {}, reason:{}}}",
                  res.value().undeleted_keys.size(),
                  res.value().undeleted_keys.front().key,
                  res.value().undeleted_keys.front().reason);

                co_return upload_result::failed;
            }

            co_return upload_result::success;
        }

        lease.client->shutdown();

        switch (res.error()) {
        case cloud_storage_clients::error_outcome::retry:
            vlog(
              ctxlog.debug,
              "DeleteObjects {}, {} backoff required",
              bucket,
              std::chrono::duration_cast<std::chrono::milliseconds>(
                permit.delay));
            co_await ss::sleep_abortable(permit.delay, _as);
            permit = fib.retry();
            break;
        case cloud_storage_clients::error_outcome::fail:
            result = upload_result::failed;
            break;
        case cloud_storage_clients::error_outcome::key_not_found:
            vassert(
              false,
              "Unexpected notfound outcome received when deleting objects {} "
              "from bucket {}",
              keys.size(),
              bucket);
            break;
        }
    }
    if (!result) {
        vlog(
          ctxlog.warn,
          "DeleteObjects (batch size={}, bucket={}), backoff quota exceded, "
          "objects "
          "not deleted",
          keys.size(),
          bucket);
        result = upload_result::timedout;
    } else {
        vlog(
          ctxlog.warn,
          "DeleteObjects (batch size={}, bucket={}), objects not deleted, "
          "error: {}",
          keys.size(),
          bucket,
          *result);
    }
    co_return *result;
}

template ss::future<upload_result>
remote::delete_objects<std::vector<cloud_storage_clients::object_key>>(
  const cloud_storage_clients::bucket_name& bucket,
  std::vector<cloud_storage_clients::object_key> keys,
  retry_chain_node& parent);

template ss::future<upload_result>
remote::delete_objects<std::deque<cloud_storage_clients::object_key>>(
  const cloud_storage_clients::bucket_name& bucket,
  std::deque<cloud_storage_clients::object_key> keys,
  retry_chain_node& parent);

template<typename R>
requires std::ranges::range<R>
         && std::same_as<
           std::ranges::range_value_t<R>,
           cloud_storage_clients::object_key>
ss::future<upload_result> remote::delete_objects_sequentially(
  const cloud_storage_clients::bucket_name& bucket,
  R keys,
  retry_chain_node& parent) {
    retry_chain_logger ctxlog(cst_log, parent);

    vlog(
      cst_log.debug,
      "Backend {} does not support batch delete, falling back to "
      "sequential deletes",
      _cloud_storage_backend);

    std::vector<key_and_node> key_nodes;
    key_nodes.reserve(keys.size());

    std::transform(
      std::make_move_iterator(keys.begin()),
      std::make_move_iterator(keys.end()),
      std::back_inserter(key_nodes),
      [&parent](auto&& key) {
          return key_and_node{
            .key = std::forward<cloud_storage_clients::object_key>(key),
            .node = std::make_unique<retry_chain_node>(&parent)};
      });

    std::vector<upload_result> results;
    results.reserve(key_nodes.size());
    auto fut = co_await ss::coroutine::as_future(ss::max_concurrent_for_each(
      key_nodes.begin(),
      key_nodes.end(),
      concurrency(),
      [this, &bucket, &results, ctxlog](auto& kn) -> ss::future<> {
          vlog(ctxlog.trace, "Deleting key {}", kn.key);
          return delete_object(bucket, kn.key, *kn.node)
            .then([&results](auto result) { results.push_back(result); });
      }));
    if (fut.failed()) {
        std::exception_ptr eptr = fut.get_exception();
        if (ssx::is_shutdown_exception(eptr)) {
            vlog(
              ctxlog.debug, "Failed to delete keys during shutdown: {}", eptr);
        } else {
            vlog(ctxlog.error, "Failed to delete keys: {}", eptr);
        }
        co_return upload_result::failed;
    }

    if (results.empty()) {
        vlog(ctxlog.error, "No keys were deleted");
        co_return upload_result::failed;
    }

    // This is not ideal, we lose all non-failures but the first one, but
    // returning a single result for multiple operations will lose
    // information.
    co_return std::reduce(
      results.begin(),
      results.end(),
      upload_result::success,
      [](auto res_a, auto res_b) {
          if (res_a != upload_result::success) {
              return res_a;
          }

          return res_b;
      });
}

ss::future<remote::list_result> remote::list_objects(
  const cloud_storage_clients::bucket_name& bucket,
  retry_chain_node& parent,
  std::optional<cloud_storage_clients::object_key> prefix,
  std::optional<char> delimiter,
  std::optional<cloud_storage_clients::client::item_filter> item_filter,
  std::optional<size_t> max_keys,
  std::optional<ss::sstring> continuation_token) {
    ss::gate::holder gh{_gate};
    retry_chain_node fib(&parent);
    retry_chain_logger ctxlog(cst_log, fib);
    auto lease = co_await _pool.local().acquire(fib.root_abort_source());
    auto permit = fib.retry();
    vlog(ctxlog.debug, "List objects {}", bucket);
    std::optional<list_result> result;

    bool items_remaining = true;

    // Gathers the items from a series of successful ListObjectsV2 calls
    cloud_storage_clients::client::list_bucket_result list_bucket_result;

    const auto caller_handle_truncation = max_keys.has_value();

    if (caller_handle_truncation) {
        vassert(max_keys.value() > 0, "Max keys must be greater than 0.");
    }

    // Keep iterating while the ListObjectsV2 calls has more items to return
    while (!_gate.is_closed() && permit.is_allowed && !result) {
        auto res = co_await lease.client->list_objects(
          bucket,
          prefix,
          std::nullopt,
          max_keys,
          continuation_token,
          fib.get_timeout(),
          delimiter,
          item_filter);

        if (res) {
            auto list_result = res.value();
            // Successful call, prepare for future calls by getting
            // continuation_token if result was truncated
            items_remaining = list_result.is_truncated;
            continuation_token.emplace(list_result.next_continuation_token);
            std::copy(
              std::make_move_iterator(list_result.contents.begin()),
              std::make_move_iterator(list_result.contents.end()),
              std::back_inserter(list_bucket_result.contents));

            // Move common prefixes to the result, only if they have not been
            // copied yet. These values will remain the same during pagination
            // of list call results, so they should only be copied once.
            if (
              list_bucket_result.common_prefixes.empty()
              && !list_result.common_prefixes.empty()) {
                std::copy(
                  std::make_move_iterator(list_result.common_prefixes.begin()),
                  std::make_move_iterator(list_result.common_prefixes.end()),
                  std::back_inserter(list_bucket_result.common_prefixes));
            }

            list_bucket_result.prefix = list_result.prefix;

            // Continue to list the remaining items
            if (items_remaining) {
                // But, return early if max_keys was specified (caller will
                // handle truncation)
                if (caller_handle_truncation) {
                    list_bucket_result.is_truncated = true;
                    list_bucket_result.next_continuation_token
                      = continuation_token.value();
                    co_return list_bucket_result;
                }
                continue;
            }

            co_return list_bucket_result;
        }

        lease.client->shutdown();

        switch (res.error()) {
        case cloud_storage_clients::error_outcome::retry:
            vlog(
              ctxlog.debug,
              "ListObjectsV2 {}, {} backoff required",
              bucket,
              std::chrono::duration_cast<std::chrono::milliseconds>(
                permit.delay));
            co_await ss::sleep_abortable(permit.delay, _as);
            permit = fib.retry();
            break;
        case cloud_storage_clients::error_outcome::fail:
            result = cloud_storage_clients::error_outcome::fail;
            break;
        case cloud_storage_clients::error_outcome::key_not_found:
            vassert(
              false,
              "Unexpected key_not_found outcome received when listing bucket "
              "{}",
              bucket);
        }
    }

    if (!result) {
        vlog(ctxlog.warn, "ListObjectsV2 {}, backoff quota exceeded", bucket);
        result = cloud_storage_clients::error_outcome::fail;
    } else {
        vlog(
          ctxlog.warn,
          "ListObjectsV2 {}, unexpected error: {}",
          bucket,
          result->error());
    }
    co_return *result;
}

ss::future<upload_result> remote::upload_object(upload_request upload_request) {
    auto guard = _gate.hold();

    auto& transfer_details = upload_request.transfer_details;
    retry_chain_node fib(&transfer_details.parent_rtc);
    retry_chain_logger ctxlog(cst_log, fib);
    auto permit = fib.retry();

    auto content_length = upload_request.payload.size_bytes();
    auto path = cloud_storage_clients::object_key(transfer_details.key());
    auto upload_type = upload_request.type;

    std::optional<upload_result> result;
    while (!_gate.is_closed() && permit.is_allowed && !result) {
        auto lease = co_await _pool.local().acquire(fib.root_abort_source());

        vlog(
          ctxlog.debug,
          "Uploading {} to path {}, length {}",
          upload_type,
          path,
          content_length);

        notify_external_subscribers(
          api_activity_notification{
            .type = api_activity_type::object_upload,
            .is_retry = fib.retry_count() > 1},
          transfer_details.parent_rtc);

        auto to_upload = upload_request.payload.copy();
        auto res = co_await lease.client->put_object(
          transfer_details.bucket,
          path,
          content_length,
          make_iobuf_input_stream(std::move(to_upload)),
          fib.get_timeout());

        if (res) {
            transfer_details.on_success(_probe);
            co_return upload_result::success;
        }

        lease.client->shutdown();
        switch (res.error()) {
        case cloud_storage_clients::error_outcome::retry:
            vlog(
              ctxlog.debug,
              "Uploading {} {} to {}, {} backoff required",
              upload_type,
              path,
              transfer_details.bucket,
              std::chrono::duration_cast<std::chrono::milliseconds>(
                permit.delay));
            transfer_details.on_backoff(_probe);
            co_await ss::sleep_abortable(permit.delay, _as);
            permit = fib.retry();
            break;
        case cloud_storage_clients::error_outcome::key_not_found:
            [[fallthrough]];
        case cloud_storage_clients::error_outcome::fail:
            result = upload_result::failed;
            break;
        }
    }

    transfer_details.on_failure(_probe);
    if (!result) {
        vlog(
          ctxlog.warn,
          "Uploading {} {} to {}, backoff quota exceded, {} not "
          "uploaded",
          upload_type,
          path,
          transfer_details.bucket,
          upload_type);
    } else {
        vlog(
          ctxlog.warn,
          "Uploading {} {} to {}, {}, {} not uploaded",
          upload_type,
          path,
          transfer_details.bucket,
          *result,
          upload_type);
    }
    co_return upload_result::timedout;
}

ss::sstring lazy_abort_source::abort_reason() const { return _abort_reason; }

bool lazy_abort_source::abort_requested() {
    auto maybe_abort = _predicate();
    if (maybe_abort.has_value()) {
        _abort_reason = *maybe_abort;
        return true;
    } else {
        return false;
    }
}

ss::future<>
remote::propagate_credentials(cloud_roles::credentials credentials) {
    return container().invoke_on_all(
      [c = std::move(credentials)](remote& svc) mutable {
          svc._pool.local().load_credentials(std::move(c));
      });
}

auth_refresh_bg_op::auth_refresh_bg_op(
  ss::gate& gate,
  ss::abort_source& as,
  cloud_storage_clients::client_configuration client_conf,
  model::cloud_credentials_source cloud_credentials_source)
  : _gate(gate)
  , _as(as)
  , _client_conf(std::move(client_conf))
  , _cloud_credentials_source(cloud_credentials_source) {}

void auth_refresh_bg_op::maybe_start_auth_refresh_op(
  cloud_roles::credentials_update_cb_t credentials_update_cb) {
    if (ss::this_shard_id() == auth_refresh_shard_id) {
        do_start_auth_refresh_op(std::move(credentials_update_cb));
    }
}

cloud_storage_clients::client_configuration
auth_refresh_bg_op::get_client_config() const {
    return _client_conf;
}

void auth_refresh_bg_op::set_client_config(
  cloud_storage_clients::client_configuration conf) {
    _client_conf = std::move(conf);
}

void auth_refresh_bg_op::do_start_auth_refresh_op(
  cloud_roles::credentials_update_cb_t credentials_update_cb) {
    if (is_static_config()) {
        // If credentials are static IE not changing, we just need to set the
        // credential object once on all cores with static strings.
        vlog(
          cst_log.info,
          "creating static credentials based on credentials source {}",
          _cloud_credentials_source);

        // Send the credentials to the client pool in a fiber
        ssx::spawn_with_gate(
          _gate,
          [creds = build_static_credentials(),
           fn = std::move(credentials_update_cb)] { return fn(creds); });
    } else {
        // Create an implementation of refresh_credentials based on the setting
        // cloud_credentials_source.
        try {
            auto region_name = ss::visit(
              _client_conf,
              [](cloud_storage_clients::s3_configuration const& cfg) {
                  // S3 needs a region name to compose requests, this extracts
                  // it from s3_configuration
                  return cloud_roles::aws_region_name{cfg.region};
              },
              [](cloud_storage_clients::abs_configuration const&) {
                  // Azure Blob Storage does not need a region name to compose
                  // the requests, so this value is defaulted since it's ignored
                  // downstream
                  return cloud_roles::aws_region_name{};
              });
            _refresh_credentials.emplace(cloud_roles::make_refresh_credentials(
              _cloud_credentials_source,
              _as,
              std::move(credentials_update_cb),
              region_name));

            vlog(
              cst_log.info,
              "created credentials refresh implementation based on credentials "
              "source {}: {}",
              _cloud_credentials_source,
              *_refresh_credentials);
            _refresh_credentials->start();
        } catch (const std::exception& ex) {
            vlog(
              cst_log.error,
              "failed to initialize cloud storage authentication system: {}",
              ex.what());
        }
    }
}

bool auth_refresh_bg_op::is_static_config() const {
    return _cloud_credentials_source
           == model::cloud_credentials_source::config_file;
}

cloud_roles::credentials auth_refresh_bg_op::build_static_credentials() const {
    return ss::visit(
      _client_conf,
      [](cloud_storage_clients::s3_configuration const& cfg)
        -> cloud_roles::credentials {
          return cloud_roles::aws_credentials{
            cfg.access_key.value(),
            cfg.secret_key.value(),
            std::nullopt,
            cfg.region};
      },
      [](cloud_storage_clients::abs_configuration const& cfg)
        -> cloud_roles::credentials {
          return cloud_roles::abs_credentials{
            cfg.storage_account_name, cfg.shared_key.value()};
      });
}

ss::future<> auth_refresh_bg_op::stop() {
    if (
      ss::this_shard_id() == auth_refresh_shard_id
      && _refresh_credentials.has_value()) {
        co_await _refresh_credentials.value().stop();
    }
}

ss::future<api_activity_notification>
remote::subscribe(remote::event_filter& filter) {
    vassert(filter._hook.is_linked() == false, "Filter is already in use");
    filter._hook = {};
    _filters.push_back(filter);
    filter._promise.emplace();
    return filter._promise->get_future();
}

} // namespace cloud_storage
