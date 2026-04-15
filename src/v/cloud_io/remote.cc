/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_io/remote.h"

#include "bytes/iostream.h"
#include "cloud_io/logger.h"
#include "cloud_io/provider.h"
#include "cloud_io/transfer_details.h"
#include "cloud_storage_clients/bucket_name_parts.h"
#include "cloud_storage_clients/client_pool.h"
#include "cloud_storage_clients/configuration.h"
#include "cloud_storage_clients/types.h"
#include "cloud_storage_clients/util.h"
#include "container/chunked_vector.h"
#include "model/metadata.h"
#include "ssx/future-util.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>

#include <boost/beast/http/field.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/range/irange.hpp>

#include <exception>
#include <iterator>
#include <utility>

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

static constexpr auto gcs_scheme = "gs";
static constexpr auto s3_scheme = "s3";

cloud_io::provider infer_provider(
  model::cloud_storage_backend backend,
  const cloud_storage_clients::client_configuration& conf) {
    // The behavior of this function for existing backends MUST NOT change.
    // They are likely already used by i.e. Iceberg tables and changing the
    // behavior could break existing deployments.
    switch (backend) {
    case model::cloud_storage_backend::unknown:
        // NOTE: treat unknown cloud storage backend as a valid case
        // in which we're assuming S3 compatible storage.
    case model::cloud_storage_backend::aws:
    case model::cloud_storage_backend::minio:
    case model::cloud_storage_backend::oracle_s3_compat:
    case model::cloud_storage_backend::linode_s3_compat:
        return cloud_io::s3_compat_provider{s3_scheme};
    case model::cloud_storage_backend::google_s3_compat:
        return cloud_io::s3_compat_provider{gcs_scheme};
    case model::cloud_storage_backend::azure: {
        auto abs = std::get<cloud_storage_clients::abs_configuration>(conf);
        return cloud_io::abs_provider{
          .account_name = abs.storage_account_name(),
        };
    }
    }
}

template<typename ErrT>
ErrT throw_if_not_timeout(const std::exception_ptr& e, ErrT on_timeout) {
    try {
        std::rethrow_exception(e);
    } catch (const ss::timed_out_error&) {
        return on_timeout;
    } catch (...) {
        throw;
    }
}

/// \brief Multipart upload state wrapper that holds a client lease
///
/// This wrapper holds a client lease for the entire duration of the
/// multipart upload operation, ensuring the client remains available
/// and is not returned to the pool prematurely.
class multipart_upload_state_with_lease final
  : public cloud_storage_clients::multipart_upload_state {
public:
    multipart_upload_state_with_lease(
      cloud_storage_clients::client_pool::client_lease lease,
      ss::shared_ptr<cloud_storage_clients::multipart_upload_state> inner)
      : _lease(std::move(lease))
      , _inner(std::move(inner)) {}

    ss::future<> initialize_multipart() override {
        return _inner->initialize_multipart();
    }

    ss::future<> upload_part(size_t part_num, iobuf data) override {
        return _inner->upload_part(part_num, std::move(data));
    }

    ss::future<> complete_multipart_upload() override {
        return _inner->complete_multipart_upload();
    }

    ss::future<> abort_multipart_upload() override {
        return _inner->abort_multipart_upload();
    }

    ss::future<> upload_as_single_object(iobuf data) override {
        return _inner->upload_as_single_object(std::move(data));
    }

    bool is_multipart_initialized() const override {
        return _inner->is_multipart_initialized();
    }

    ss::sstring upload_id() const override { return _inner->upload_id(); }

private:
    cloud_storage_clients::client_pool::client_lease _lease;
    ss::shared_ptr<cloud_storage_clients::multipart_upload_state> _inner;
};

} // namespace

namespace cloud_io {

using namespace std::chrono_literals;

remote::remote(
  ss::sharded<cloud_storage_clients::client_pool>& clients,
  const cloud_storage_clients::client_configuration& conf,
  model::cloud_credentials_source cloud_credentials_source,
  ss::scheduling_group sg)
  : _pool(clients)
  , _resources(std::make_unique<io_resources>(sg))
  , _cloud_storage_backend{cloud_storage_clients::
                             infer_backend_from_configuration(
                               conf, cloud_credentials_source)}
  , _provider(infer_provider(_cloud_storage_backend, conf))
  , _lease_timeout(
      config::shard_local_cfg().cloud_storage_client_lease_timeout_ms.bind()) {
    vlog(
      log.info, "remote initialized with backend {}", _cloud_storage_backend);
}

remote::~remote() {
    // This is declared in the .cc to avoid header trying to
    // link with destructors for unique_ptr wrapped members
}

ss::future<> remote::start() { co_await _resources->start(); }

void remote::request_stop() {
    vlog(log.debug, "Requesting stop of remote...");
    _as.request_abort();
}
ss::future<> remote::stop() {
    vlog(log.debug, "Stopping remote...");
    if (!_as.abort_requested()) {
        _as.request_abort();
    }
    co_await _resources->stop();
    co_await _gate.close();
    vlog(log.debug, "Stopped remote...");
}

size_t remote::concurrency() const { return _pool.local().capacity(); }

const provider& remote::provider() const { return _provider; }

bool remote::is_batch_delete_supported() const {
    return delete_objects_max_keys() > 1;
}

int remote::delete_objects_max_keys() const {
    switch (_cloud_storage_backend) {
    case model::cloud_storage_backend::aws:
        [[fallthrough]];
    case model::cloud_storage_backend::oracle_s3_compat:
        // https://docs.oracle.com/en-us/iaas/api/#/en/s3objectstorage/20160918/Object/BulkDelete
        [[fallthrough]];
    case model::cloud_storage_backend::linode_s3_compat:
        // Empirically verified, no public docs found.
        [[fallthrough]];
    case model::cloud_storage_backend::minio:
        // https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
        return 1000;
    case model::cloud_storage_backend::azure:
        // https://learn.microsoft.com/en-us/rest/api/storageservices/blob-batch
        return 256;
    case model::cloud_storage_backend::google_s3_compat:
        return 100;
    case model::cloud_storage_backend::unknown:
        return 1;
    }
}

ss::future<upload_result> remote::upload_stream(
  transfer_details transfer_details,
  uint64_t content_length,
  const reset_input_stream& reset_str,
  lazy_abort_source& lazy_abort_source,
  const std::string_view stream_label,
  std::optional<size_t> max_retries) {
    const auto& path = transfer_details.key;
    const auto& bucket = transfer_details.bucket;
    const auto bucket_parts = cloud_storage_clients::parse_bucket_name(bucket);
    if (!bucket_parts) {
        vlog(
          log.warn,
          "Failed to parse bucket name {}: {}",
          bucket,
          bucket_parts.error());
        co_return upload_result::failed;
    }
    auto guard = _gate.hold();
    retry_chain_node fib(&transfer_details.parent_rtc);
    retry_chain_logger ctxlog(log, fib);
    auto permit = fib.retry();
    vlog(
      ctxlog.debug,
      "Uploading {} to path {}, length {}",
      stream_label,
      transfer_details.key,
      content_length);
    std::optional<upload_result> result;
    while (!_gate.is_closed() && permit.is_allowed && !result
           && (!max_retries.has_value() || max_retries.value() > 0)) {
        if (max_retries.has_value()) {
            max_retries = max_retries.value() - 1;
        }
        auto fut = co_await ss::coroutine::as_future(
          _pool.local().acquire_with_timeout(
            *bucket_parts, fib.root_abort_source(), _lease_timeout(), fib()));
        if (fut.failed()) {
            co_return throw_if_not_timeout(
              fut.get_exception(), upload_result::timedout);
        }
        auto lease = std::move(fut).get();
        transfer_details.on_request(fib.retry_count());

        // Client acquisition can take some time. Do a check before starting
        // the upload if we can still continue.
        if (lazy_abort_source.abort_requested()) {
            vlog(
              ctxlog.warn,
              "{}: cancelled uploading {} to {}",
              lazy_abort_source.abort_reason(),
              path,
              bucket);
            transfer_details.on_failure();
            co_return upload_result::cancelled;
        }

        auto reader_handle = co_await reset_str();
        // Segment upload attempt
        auto res = co_await lease.client->put_object(
          bucket_parts->name,
          path,
          content_length,
          reader_handle->take_stream(),
          fib.get_timeout());

        // `put_object` closed the encapsulated input_stream, but we must
        // call close() on the segment_reader_handle to release the FD.
        co_await reader_handle->close();

        if (res) {
            transfer_details.on_success_size(content_length);
            co_return upload_result::success;
        }

        lease.client->shutdown();
        {
            auto _ = std::move(lease);
        }
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
            transfer_details.on_backoff();
            if (!lazy_abort_source.abort_requested()) {
                co_await ss::sleep_abortable(
                  permit.delay, fib.root_abort_source());
            }
            permit = fib.retry();
            break;
        case cloud_storage_clients::error_outcome::operation_not_supported:
            [[fallthrough]];
        case cloud_storage_clients::error_outcome::key_not_found:
            // not expected during upload
            [[fallthrough]];
        case cloud_storage_clients::error_outcome::fail:
            result = upload_result::failed;
            break;
        case cloud_storage_clients::error_outcome::authentication_failed:
            result = upload_result::failed;
            break;
        }
    }

    transfer_details.on_failure();

    if (!result) {
        vlog(
          log.warn,
          "Uploading {} {} to {}, backoff quota exceded, {} not uploaded",
          stream_label,
          path,
          bucket,
          stream_label);
        result = upload_result::timedout;
    } else {
        vlog(
          log.warn,
          "Uploading {} {} to {}, {}, {} not uploaded",
          stream_label,
          path,
          bucket,
          *result,
          stream_label);
    }
    co_return *result;
}

ss::future<download_result> remote::download_stream(
  transfer_details transfer_details,
  const try_consume_stream& cons_str,
  const std::string_view stream_label,
  [[maybe_unused]] bool acquire_hydration_units,
  std::optional<cloud_storage_clients::http_byte_range> byte_range,
  std::function<void(size_t)> throttle_metric_ms_cb) {
    const auto& path = transfer_details.key;
    const auto& bucket = transfer_details.bucket;
    const auto bucket_parts = cloud_storage_clients::parse_bucket_name(bucket);
    if (!bucket_parts) {
        vlog(
          log.warn,
          "Failed to parse bucket name {}: {}",
          bucket,
          bucket_parts.error());
        co_return download_result::failed;
    }

    auto guard = _gate.hold();
    retry_chain_node fib(&transfer_details.parent_rtc);
    retry_chain_logger ctxlog(log, fib);

    auto permit = fib.retry();
    vlog(ctxlog.debug, "Download {} {}", stream_label, path);
    std::optional<download_result> result;
    while (!_gate.is_closed() && permit.is_allowed && !result) {
        auto fut = co_await [this, &fib, &transfer_details, &bucket_parts] {
            transfer_details.on_client_acquire();
            return ss::coroutine::as_future(_pool.local().acquire_with_timeout(
              *bucket_parts, fib.root_abort_source(), _lease_timeout(), fib()));
        }();
        if (fut.failed()) {
            co_return throw_if_not_timeout(
              fut.get_exception(), download_result::timedout);
        }
        auto lease = std::move(fut).get();
        transfer_details.on_request(fib.retry_count());

        auto download_latency_measure
          = transfer_details.scoped_latency_measurement();
        auto resp = co_await lease.client->get_object(
          bucket_parts->name, path, fib.get_timeout(), false, byte_range);

        if (resp) {
            vlog(ctxlog.debug, "Receive OK response from {}", path);
            auto length = boost::lexical_cast<uint64_t>(
              resp.value()->get_headers().at(
                boost::beast::http::field::content_length));
            try {
                auto underlying_st = resp.value()->as_input_stream();
                auto throttled_st = _resources->throttle_download(
                  std::move(underlying_st), _as, throttle_metric_ms_cb);
                uint64_t content_length = co_await cons_str(
                  length, std::move(throttled_st));
                transfer_details.on_success_size(content_length);
                co_return download_result::success;
            } catch (...) {
                const auto ex = std::current_exception();
                vlog(
                  ctxlog.debug,
                  "unexpected error when consuming stream {}",
                  ex);
                resp
                  = cloud_storage_clients::util::handle_client_transport_error(
                    ex, log);
            }
        }

        download_latency_measure.reset();

        lease.client->shutdown();
        {
            auto _ = std::move(lease);
        }

        switch (resp.error()) {
        case cloud_storage_clients::error_outcome::retry:
            if (_as.abort_requested()) {
                // When the remote is stopped we may still get the 'timeout'
                // error from the http client. This error is interpreted
                // as a retriable error by the cloud_storage_client.
                // In order to distinguish between the two cases (shutdown vs
                // timeout) the state of the abort_source is checked.
                vlog(
                  ctxlog.debug,
                  "Downloading {} from {}, skipping backoff because of the "
                  "shutdown",
                  stream_label,
                  bucket);
                result = download_result::timedout;
                break;
            } else {
                vlog(
                  ctxlog.debug,
                  "Downloading {} from {}, {} backoff required",
                  stream_label,
                  bucket,
                  std::chrono::duration_cast<std::chrono::milliseconds>(
                    permit.delay));
                transfer_details.on_backoff();
                co_await ss::sleep_abortable(
                  permit.delay, fib.root_abort_source());
                permit = fib.retry();
            }
            break;
        case cloud_storage_clients::error_outcome::operation_not_supported:
            [[fallthrough]];
        case cloud_storage_clients::error_outcome::fail:
            result = download_result::failed;
            break;
        case cloud_storage_clients::error_outcome::key_not_found:
            result = download_result::notfound;
            break;
        case cloud_storage_clients::error_outcome::authentication_failed:
            result = download_result::failed;
            break;
        }
    }
    transfer_details.on_failure();
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

ss::future<download_result>
remote::download_object(download_request download_request) {
    auto guard = _gate.hold();
    auto& transfer_details = download_request.transfer_details;
    retry_chain_node fib(&transfer_details.parent_rtc);
    retry_chain_logger ctxlog(log, fib);

    const auto path = transfer_details.key;
    const auto bucket = transfer_details.bucket;
    const auto bucket_parts = cloud_storage_clients::parse_bucket_name(bucket);
    if (!bucket_parts) {
        vlog(
          log.warn,
          "Failed to parse bucket name {}: {}",
          bucket,
          bucket_parts.error());
        co_return download_result::failed;
    }
    const auto object_type = download_request.display_str;

    auto permit = fib.retry();
    vlog(ctxlog.debug, "Downloading {} from {}", object_type, path);

    std::optional<download_result> result;
    while (!_gate.is_closed() && permit.is_allowed && !result) {
        auto fut = co_await ss::coroutine::as_future(
          _pool.local().acquire_with_timeout(
            *bucket_parts, fib.root_abort_source(), _lease_timeout(), fib()));
        if (fut.failed()) {
            co_return throw_if_not_timeout(
              fut.get_exception(), download_result::timedout);
        }
        auto lease = std::move(fut).get();
        download_request.transfer_details.on_request(fib.retry_count());
        auto resp = co_await lease.client->get_object(
          bucket_parts->name,
          path,
          fib.get_timeout(),
          download_request.expect_missing);

        if (resp) {
            vlog(ctxlog.debug, "Receive OK response from {}", path);
            try {
                auto buffer = co_await http::drain(resp.value());
                download_request.payload.append_fragments(std::move(buffer));
                transfer_details.on_success();
                co_return download_result::success;
            } catch (...) {
                resp
                  = cloud_storage_clients::util::handle_client_transport_error(
                    std::current_exception(), ctxlog);
            }
        }

        lease.client->shutdown();
        {
            auto _ = std::move(lease);
        }

        switch (resp.error()) {
        case cloud_storage_clients::error_outcome::retry:
            vlog(
              ctxlog.debug,
              "Downloading {} from {}, {} backoff required",
              object_type,
              bucket,
              std::chrono::duration_cast<std::chrono::milliseconds>(
                permit.delay));
            transfer_details.on_backoff();
            co_await ss::sleep_abortable(permit.delay, fib.root_abort_source());
            permit = fib.retry();
            break;
        case cloud_storage_clients::error_outcome::operation_not_supported:
            [[fallthrough]];
        case cloud_storage_clients::error_outcome::fail:
            result = download_result::failed;
            break;
        case cloud_storage_clients::error_outcome::key_not_found:
            result = download_result::notfound;
            break;
        case cloud_storage_clients::error_outcome::authentication_failed:
            result = download_result::failed;
            break;
        }
    }
    transfer_details.on_failure();
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
  std::string_view object_type) {
    const auto bucket_parts = cloud_storage_clients::parse_bucket_name(bucket);
    if (!bucket_parts) {
        vlog(
          log.warn,
          "Failed to parse bucket name {}: {}",
          bucket,
          bucket_parts.error());
        co_return download_result::failed;
    }

    ss::gate::holder gh{_gate};
    retry_chain_node fib(&parent);
    retry_chain_logger ctxlog(log, fib);
    auto permit = fib.retry();
    vlog(ctxlog.debug, "Check {} {}", object_type, path);
    std::optional<download_result> result;
    while (!_gate.is_closed() && permit.is_allowed && !result) {
        auto fut = co_await ss::coroutine::as_future(
          _pool.local().acquire_with_timeout(
            *bucket_parts, fib.root_abort_source(), _lease_timeout(), fib()));
        if (fut.failed()) {
            co_return throw_if_not_timeout(
              fut.get_exception(), download_result::timedout);
        }
        auto lease = std::move(fut).get();
        auto resp = co_await lease.client->head_object(
          bucket_parts->name, path, fib.get_timeout());
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

        lease.client->shutdown();
        {
            auto _ = std::move(lease);
        }
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
        case cloud_storage_clients::error_outcome::operation_not_supported:
            [[fallthrough]];
        case cloud_storage_clients::error_outcome::fail:
            result = download_result::failed;
            break;
        case cloud_storage_clients::error_outcome::key_not_found:
            result = download_result::notfound;
            break;
        case cloud_storage_clients::error_outcome::authentication_failed:
            result = download_result::failed;
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

ss::future<upload_result>
remote::delete_object(transfer_details transfer_details) {
    const auto& bucket = transfer_details.bucket;
    const auto bucket_parts = cloud_storage_clients::parse_bucket_name(bucket);
    if (!bucket_parts) {
        vlog(
          log.warn,
          "Failed to parse bucket name {}: {}",
          bucket,
          bucket_parts.error());
        co_return upload_result::failed;
    }
    const auto& path = transfer_details.key;
    auto& parent = transfer_details.parent_rtc;
    ss::gate::holder gh{_gate};
    retry_chain_node fib(&parent);
    retry_chain_logger ctxlog(log, fib);
    auto permit = fib.retry();
    vlog(ctxlog.debug, "Delete object {}", path);
    std::optional<upload_result> result;
    while (!_gate.is_closed() && permit.is_allowed && !result) {
        auto fut = co_await ss::coroutine::as_future(
          _pool.local().acquire_with_timeout(
            *bucket_parts, fib.root_abort_source(), _lease_timeout(), fib()));
        if (fut.failed()) {
            co_return throw_if_not_timeout(
              fut.get_exception(), upload_result::timedout);
        }
        auto lease = std::move(fut).get();
        // NOTE: DeleteObject in S3 doesn't return an error
        // if the object doesn't exist. Because of that we're
        // using 'upload_result' type as a return type. No need
        // to handle NoSuchKey error. The 'upload_result'
        // represents any mutable operation.
        transfer_details.on_request(fib.retry_count());
        auto res = co_await lease.client->delete_object(
          bucket_parts->name, path, fib.get_timeout());

        if (res) {
            co_return upload_result::success;
        }

        lease.client->shutdown();
        {
            auto _ = std::move(lease);
        }

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
        case cloud_storage_clients::error_outcome::operation_not_supported:
            [[fallthrough]];
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
        case cloud_storage_clients::error_outcome::authentication_failed:
            result = upload_result::failed;
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
  retry_chain_node& parent,
  std::function<void(size_t)> req_cb) {
    ss::gate::holder gh{_gate};
    retry_chain_logger ctxlog(log, parent);

    if (keys.empty()) {
        vlog(ctxlog.info, "No keys to delete, returning");
        co_return upload_result::success;
    }

    vlog(ctxlog.debug, "Deleting objects count {}", keys.size());

    if (!is_batch_delete_supported()) {
        co_return co_await delete_objects_sequentially(
          bucket, std::forward<R>(keys), parent, std::move(req_cb));
    }

    const auto batches_to_delete = num_chunks(keys, delete_objects_max_keys());
    chunked_vector<upload_result> results;
    results.reserve(batches_to_delete);

    co_await ss::max_concurrent_for_each(
      boost::irange(batches_to_delete),
      concurrency(),
      [this, bucket, &keys, &parent, &results, cb = std::move(req_cb)](
        auto chunk_ix) -> ss::future<> {
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

          chunked_vector<cloud_storage_clients::object_key> key_batch;
          auto chunk_size = std::distance(chunk_begin, chunk_end);
          key_batch.reserve(chunk_size);
          std::move(chunk_begin, chunk_end, std::back_inserter(key_batch));

          vassert(
            key_batch.size() > 0,
            "The chunking logic must always produce non-empty batches.");

          return delete_object_batch(bucket, std::move(key_batch), parent, cb)
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
  chunked_vector<cloud_storage_clients::object_key> keys,
  retry_chain_node& parent,
  std::function<void(size_t)> req_cb) {
    const auto bucket_parts = cloud_storage_clients::parse_bucket_name(bucket);
    if (!bucket_parts) {
        vlog(
          log.warn,
          "Failed to parse bucket name {}: {}",
          bucket,
          bucket_parts.error());
        co_return upload_result::failed;
    }

    ss::gate::holder gh{_gate};

    retry_chain_node fib(&parent);
    retry_chain_logger ctxlog(log, fib);
    auto permit = fib.retry();
    vlog(ctxlog.debug, "Deleting a batch of size {}", keys.size());
    std::optional<upload_result> result;
    while (!_gate.is_closed() && permit.is_allowed && !result) {
        auto fut = co_await ss::coroutine::as_future(
          _pool.local().acquire_with_timeout(
            *bucket_parts, fib.root_abort_source(), _lease_timeout(), fib()));
        if (fut.failed()) {
            co_return throw_if_not_timeout(
              fut.get_exception(), upload_result::timedout);
        }
        auto lease = std::move(fut).get();
        req_cb(fib.retry_count());
        auto res = co_await lease.client->delete_objects(
          bucket_parts->name, keys, fib.get_timeout());

        if (res) {
            if (!res.value().undeleted_keys.empty()) {
                vlog(
                  ctxlog.info,
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
        {
            auto _ = std::move(lease);
        }

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
        case cloud_storage_clients::error_outcome::operation_not_supported:
            [[fallthrough]];
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
        case cloud_storage_clients::error_outcome::authentication_failed:
            result = upload_result::failed;
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
  retry_chain_node& parent,
  std::function<void(size_t)>);

template ss::future<upload_result>
remote::delete_objects<std::deque<cloud_storage_clients::object_key>>(
  const cloud_storage_clients::bucket_name& bucket,
  std::deque<cloud_storage_clients::object_key> keys,
  retry_chain_node& parent,
  std::function<void(size_t)>);

template ss::future<upload_result>
remote::delete_objects<chunked_vector<cloud_storage_clients::object_key>>(
  const cloud_storage_clients::bucket_name& bucket,
  chunked_vector<cloud_storage_clients::object_key> keys,
  retry_chain_node& parent,
  std::function<void(size_t)>);

template<typename R>
requires std::ranges::range<R>
         && std::same_as<
           std::ranges::range_value_t<R>,
           cloud_storage_clients::object_key>
ss::future<upload_result> remote::delete_objects_sequentially(
  const cloud_storage_clients::bucket_name& bucket,
  R keys,
  retry_chain_node& parent,
  std::function<void(size_t)> req_cb) {
    retry_chain_logger ctxlog(log, parent);

    vlog(
      log.debug,
      "Backend {} does not support batch delete, falling back to "
      "sequential deletes",
      _cloud_storage_backend);

    chunked_vector<key_and_node> key_nodes;
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

    chunked_vector<upload_result> results;
    results.reserve(key_nodes.size());
    auto fut = co_await ss::coroutine::as_future(
      ss::max_concurrent_for_each(
        key_nodes.begin(),
        key_nodes.end(),
        concurrency(),
        [this, &bucket, &results, ctxlog, req_cb = std::move(req_cb)](
          auto& kn) -> ss::future<> {
            vlog(ctxlog.trace, "Deleting key {}", kn.key);
            return delete_object({.bucket = bucket,
                                  .key = kn.key,
                                  .parent_rtc = *kn.node,
                                  .on_req_cb = req_cb})
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

ss::future<list_result> remote::list_objects(
  const cloud_storage_clients::bucket_name& bucket,
  retry_chain_node& parent,
  std::optional<cloud_storage_clients::object_key> prefix,
  std::optional<char> delimiter,
  std::optional<cloud_storage_clients::client::item_filter> item_filter,
  std::optional<size_t> max_keys,
  std::optional<ss::sstring> continuation_token) {
    const auto bucket_parts = cloud_storage_clients::parse_bucket_name(bucket);
    if (!bucket_parts) {
        vlog(
          log.warn,
          "Failed to parse bucket name {}: {}",
          bucket,
          bucket_parts.error());
        co_return cloud_storage_clients::error_outcome::fail;
    }

    ss::gate::holder gh{_gate};
    retry_chain_node fib(&parent);
    retry_chain_logger ctxlog(log, fib);
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
        auto fut = co_await ss::coroutine::as_future(
          _pool.local().acquire_with_timeout(
            *bucket_parts, fib.root_abort_source(), _lease_timeout(), fib()));
        if (fut.failed()) {
            co_return throw_if_not_timeout(
              fut.get_exception(), cloud_storage_clients::error_outcome::retry);
        }
        auto lease = std::move(fut).get();
        auto res = co_await lease.client->list_objects(
          bucket_parts->name,
          prefix,
          std::nullopt,
          max_keys,
          continuation_token,
          fib.get_timeout(),
          delimiter,
          item_filter);

        if (res) {
            auto list_result = std::move(res.value());
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
        {
            auto _ = std::move(lease);
        }

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
        case cloud_storage_clients::error_outcome::operation_not_supported:
            [[fallthrough]];
        case cloud_storage_clients::error_outcome::fail:
            result = cloud_storage_clients::error_outcome::fail;
            break;
        case cloud_storage_clients::error_outcome::key_not_found:
            result = cloud_storage_clients::error_outcome::fail;
            break;
        case cloud_storage_clients::error_outcome::authentication_failed:
            result = cloud_storage_clients::error_outcome::fail;
            break;
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
    co_return std::move(*result);
}

ss::future<upload_result> remote::upload_object(upload_request upload_request) {
    auto guard = _gate.hold();

    auto& transfer_details = upload_request.transfer_details;

    auto& bucket = transfer_details.bucket;
    const auto bucket_parts = cloud_storage_clients::parse_bucket_name(bucket);
    if (!bucket_parts) {
        vlog(
          log.warn,
          "Failed to parse bucket name {}: {}",
          bucket,
          bucket_parts.error());
        co_return upload_result::failed;
    }

    retry_chain_node fib(&transfer_details.parent_rtc);
    retry_chain_logger ctxlog(log, fib);
    auto permit = fib.retry();

    auto content_length = upload_request.payload.size_bytes();
    auto path = cloud_storage_clients::object_key(transfer_details.key());
    auto upload_type = upload_request.display_str;

    std::optional<upload_result> result;
    while (!_gate.is_closed() && permit.is_allowed && !result) {
        auto fut = co_await ss::coroutine::as_future(
          _pool.local().acquire_with_timeout(
            *bucket_parts, fib.root_abort_source(), _lease_timeout(), fib()));
        if (fut.failed()) {
            co_return throw_if_not_timeout(
              fut.get_exception(), upload_result::timedout);
        }
        auto lease = std::move(fut).get();

        vlog(
          ctxlog.debug,
          "Uploading {} to path {}, length {}",
          upload_type,
          path,
          content_length);
        upload_request.transfer_details.on_request(fib.retry_count());

        auto to_upload = upload_request.payload.copy();
        auto res = co_await lease.client->put_object(
          bucket_parts->name,
          path,
          content_length,
          make_iobuf_input_stream(std::move(to_upload)),
          fib.get_timeout(),
          upload_request.accept_no_content_response);

        if (res) {
            transfer_details.on_success();
            co_return upload_result::success;
        }

        lease.client->shutdown();
        {
            auto _ = std::move(lease);
        }
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
            transfer_details.on_backoff();
            co_await ss::sleep_abortable(permit.delay, _as);
            permit = fib.retry();
            break;
        case cloud_storage_clients::error_outcome::operation_not_supported:
            [[fallthrough]];
        case cloud_storage_clients::error_outcome::key_not_found:
            [[fallthrough]];
        case cloud_storage_clients::error_outcome::fail:
            result = upload_result::failed;
            break;
        case cloud_storage_clients::error_outcome::authentication_failed:
            result = upload_result::failed;
            break;
        }
    }

    transfer_details.on_failure();
    if (!result) {
        vlog(
          ctxlog.warn,
          "Uploading {} {} to {}, backoff quota exceded, {} not "
          "uploaded",
          upload_type,
          path,
          transfer_details.bucket,
          upload_type);
        result = upload_result::timedout;
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
    co_return *result;
}

ss::future<result<
  cloud_storage_clients::multipart_upload_ref,
  cloud_storage_clients::error_outcome>>
remote::initiate_multipart_upload(
  const cloud_storage_clients::bucket_name& bucket,
  const cloud_storage_clients::object_key& key,
  size_t part_size,
  ss::lowres_clock::duration timeout) {
    auto guard = _gate.hold();

    const auto bucket_parts = cloud_storage_clients::parse_bucket_name(bucket);
    if (!bucket_parts) {
        vlog(
          log.warn,
          "Failed to parse bucket name {}: {}",
          bucket,
          bucket_parts.error());
        co_return cloud_storage_clients::error_outcome::fail;
    }

    // Acquire a client lease from the pool
    auto fut = co_await ss::coroutine::as_future(
      _pool.local().acquire(*bucket_parts, _as));
    if (fut.failed()) {
        vlog(
          log.warn,
          "Failed to acquire client for multipart upload: {}",
          fut.get_exception());
        co_return cloud_storage_clients::error_outcome::fail;
    }
    auto lease = std::move(fut).get();

    // Get the multipart upload state from the client
    auto state_result = co_await lease.client->initiate_multipart_upload(
      bucket_parts->name, key, part_size, timeout);

    if (!state_result) {
        // Failed to initiate - lease will be automatically returned
        co_return state_result.error();
    }

    // Wrap the state with the lease to keep the client alive
    // for the entire duration of the upload operation
    auto wrapped_state = ss::make_shared<multipart_upload_state_with_lease>(
      std::move(lease), std::move(state_result.value()));

    // Create the multipart_upload with the wrapped state
    auto upload = ss::make_shared<cloud_storage_clients::multipart_upload>(
      std::move(wrapped_state), part_size, log);

    co_return upload;
}

} // namespace cloud_io
