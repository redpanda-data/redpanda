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
#include "cloud_io/transfer_details.h"
#include "cloud_storage_clients/client_pool.h"
#include "cloud_storage_clients/types.h"
#include "cloud_storage_clients/util.h"
#include "model/metadata.h"
#include "ssx/future-util.h"
#include "ssx/semaphore.h"
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

} // namespace

namespace cloud_io {

using namespace std::chrono_literals;

remote::remote(
  ss::sharded<cloud_storage_clients::client_pool>& clients,
  const cloud_storage_clients::client_configuration& conf,
  model::cloud_credentials_source cloud_credentials_source)
  : _pool(clients)
  , _auth_refresh_bg_op{_gate, _as, conf, cloud_credentials_source}
  , _resources(std::make_unique<io_resources>())
  , _azure_shared_key_binding(
      config::shard_local_cfg().cloud_storage_azure_shared_key.bind())
  , _cloud_storage_backend{
      cloud_storage_clients::infer_backend_from_configuration(
        conf, cloud_credentials_source)} {
    vlog(
      log.info, "remote initialized with backend {}", _cloud_storage_backend);
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
              log.warn,
              "Attempt to set cloud_storage_azure_shared_key for cluster using "
              "S3 detected");
            return;
        }

        vlog(
          log.info,
          "cloud_storage_azure_shared_key was updated. Refreshing "
          "credentials.");

        auto new_shared_key = _azure_shared_key_binding();
        if (!new_shared_key) {
            vlog(
              log.info,
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

    co_await _resources->start();
}

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
    co_await _auth_refresh_bg_op.stop();
    vlog(log.debug, "Stopeed remote...");
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
    case model::cloud_storage_backend::oracle_s3_compat:
        // https://docs.oracle.com/en-us/iaas/api/#/en/s3objectstorage/20160918/Object/BulkDelete
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

ss::future<upload_result> remote::upload_stream(
  transfer_details transfer_details,
  uint64_t content_length,
  const reset_input_stream& reset_str,
  lazy_abort_source& lazy_abort_source,
  const std::string_view stream_label,
  std::optional<size_t> max_retries) {
    const auto& path = transfer_details.key;
    const auto& bucket = transfer_details.bucket;
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
           && max_retries.value_or(1) > 0) {
        if (max_retries.has_value()) {
            max_retries = max_retries.value() - 1;
        }
        auto lease = co_await _pool.local().acquire(fib.root_abort_source());
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
          bucket,
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
        }
    }

    transfer_details.on_failure();

    if (!result) {
        vlog(
          log.warn,
          "Uploading {} {} to {}, backoff quota exceded, {} not "
          "uploaded",
          stream_label,
          path,
          bucket,
          stream_label);
    } else {
        vlog(
          log.warn,
          "Uploading {} {} to {}, {}, segment not uploaded",
          stream_label,
          path,
          bucket,
          *result);
    }
    co_return upload_result::timedout;
}

ss::future<download_result> remote::download_stream(
  transfer_details transfer_details,
  const try_consume_stream& cons_str,
  const std::string_view stream_label,
  bool acquire_hydration_units,
  std::optional<cloud_storage_clients::http_byte_range> byte_range,
  std::function<void(size_t)> throttle_metric_ms_cb) {
    const auto& path = transfer_details.key;
    const auto& bucket = transfer_details.bucket;

    auto guard = _gate.hold();
    retry_chain_node fib(&transfer_details.parent_rtc);
    retry_chain_logger ctxlog(log, fib);

    ssx::semaphore_units hu;
    if (acquire_hydration_units) {
        hu = co_await _resources->get_hydration_units(1);
    }

    auto lease = co_await [this, &fib, &transfer_details] {
        transfer_details.on_client_acquire();
        return _pool.local().acquire(fib.root_abort_source());
    }();

    auto permit = fib.retry();
    vlog(ctxlog.debug, "Download {} {}", stream_label, path);
    std::optional<download_result> result;
    while (!_gate.is_closed() && permit.is_allowed && !result) {
        transfer_details.on_request(fib.retry_count());

        auto download_latency_measure
          = transfer_details.scoped_latency_measurement();
        auto resp = co_await lease.client->get_object(
          bucket, path, fib.get_timeout(), false, byte_range);

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

        switch (resp.error()) {
        case cloud_storage_clients::error_outcome::retry:
            vlog(
              ctxlog.debug,
              "Downloading {} from {}, {} backoff required",
              stream_label,
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
    const auto object_type = download_request.display_str;

    auto lease = co_await _pool.local().acquire(fib.root_abort_source());

    auto permit = fib.retry();
    vlog(ctxlog.debug, "Downloading {} from {}", object_type, path);

    std::optional<download_result> result;
    while (!_gate.is_closed() && permit.is_allowed && !result) {
        download_request.transfer_details.on_request(fib.retry_count());
        auto resp = co_await lease.client->get_object(
          bucket, path, fib.get_timeout(), download_request.expect_missing);

        if (resp) {
            vlog(ctxlog.debug, "Receive OK response from {}", path);
            auto buffer
              = co_await cloud_storage_clients::util::drain_response_stream(
                resp.value());
            download_request.payload.append_fragments(std::move(buffer));
            transfer_details.on_success();
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
    ss::gate::holder gh{_gate};
    retry_chain_node fib(&parent);
    retry_chain_logger ctxlog(log, fib);
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
        case cloud_storage_clients::error_outcome::operation_not_supported:
            [[fallthrough]];
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

ss::future<upload_result>
remote::delete_object(transfer_details transfer_details) {
    const auto& bucket = transfer_details.bucket;
    const auto& path = transfer_details.key;
    auto& parent = transfer_details.parent_rtc;
    ss::gate::holder gh{_gate};
    retry_chain_node fib(&parent);
    retry_chain_logger ctxlog(log, fib);
    auto lease = co_await _pool.local().acquire(fib.root_abort_source());
    auto permit = fib.retry();
    vlog(ctxlog.debug, "Delete object {}", path);
    std::optional<upload_result> result;
    while (!_gate.is_closed() && permit.is_allowed && !result) {
        // NOTE: DeleteObject in S3 doesn't return an error
        // if the object doesn't exist. Because of that we're
        // using 'upload_result' type as a return type. No need
        // to handle NoSuchKey error. The 'upload_result'
        // represents any mutable operation.
        transfer_details.on_request(fib.retry_count());
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
    std::vector<upload_result> results;
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

          std::vector<cloud_storage_clients::object_key> key_batch;
          key_batch.insert(
            key_batch.end(),
            std::make_move_iterator(chunk_begin),
            std::make_move_iterator(chunk_end));

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
  std::vector<cloud_storage_clients::object_key> keys,
  retry_chain_node& parent,
  std::function<void(size_t)> req_cb) {
    ss::gate::holder gh{_gate};

    retry_chain_node fib(&parent);
    retry_chain_logger ctxlog(log, fib);
    auto lease = co_await _pool.local().acquire(fib.root_abort_source());
    auto permit = fib.retry();
    vlog(ctxlog.debug, "Deleting a batch of size {}", keys.size());
    std::optional<upload_result> result;
    while (!_gate.is_closed() && permit.is_allowed && !result) {
        req_cb(fib.retry_count());
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
    ss::gate::holder gh{_gate};
    retry_chain_node fib(&parent);
    retry_chain_logger ctxlog(log, fib);
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
        case cloud_storage_clients::error_outcome::operation_not_supported:
            [[fallthrough]];
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
    retry_chain_logger ctxlog(log, fib);
    auto permit = fib.retry();

    auto content_length = upload_request.payload.size_bytes();
    auto path = cloud_storage_clients::object_key(transfer_details.key());
    auto upload_type = upload_request.display_str;

    std::optional<upload_result> result;
    while (!_gate.is_closed() && permit.is_allowed && !result) {
        auto lease = co_await _pool.local().acquire(fib.root_abort_source());

        vlog(
          ctxlog.debug,
          "Uploading {} to path {}, length {}",
          upload_type,
          path,
          content_length);
        upload_request.transfer_details.on_request(fib.retry_count());

        auto to_upload = upload_request.payload.copy();
        auto res = co_await lease.client->put_object(
          transfer_details.bucket,
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

ss::future<>
remote::propagate_credentials(cloud_roles::credentials credentials) {
    return container().invoke_on_all(
      [c = std::move(credentials)](remote& svc) mutable {
          svc._pool.local().load_credentials(std::move(c));
      });
}

} // namespace cloud_io
