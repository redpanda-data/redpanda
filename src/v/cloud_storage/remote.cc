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
#include "cloud_storage/logger.h"
#include "cloud_storage/materialized_segments.h"
#include "cloud_storage/remote_segment.h"
#include "cloud_storage/types.h"
#include "model/metadata.h"
#include "net/connection.h"
#include "ssx/sformat.h"
#include "utils/intrusive_list_helpers.h"
#include "utils/retry_chain_node.h"
#include "utils/string_switch.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/weak_ptr.hh>

#include <boost/beast/http/error.hpp>
#include <boost/beast/http/field.hpp>
#include <fmt/chrono.h>

#include <exception>
#include <utility>
#include <variant>

namespace cloud_storage {

using namespace std::chrono_literals;

remote::remote(
  connection_limit limit,
  const cloud_storage_clients::client_configuration& conf,
  model::cloud_credentials_source cloud_credentials_source)
  : _pool(limit(), conf)
  , _auth_refresh_bg_op{_gate, _as, conf, cloud_credentials_source}
  , _materialized(std::make_unique<materialized_segments>())
  , _probe(
      remote_metrics_disabled(static_cast<bool>(
        std::visit([](auto&& cfg) { return cfg.disable_metrics; }, conf))),
      remote_metrics_disabled(static_cast<bool>(std::visit(
        [](auto&& cfg) { return cfg.disable_public_metrics; }, conf))),
      *_materialized) {
    // If the credentials source is from config file, bypass the background op
    // to refresh credentials periodically, and load pool with static
    // credentials right now.
    if (_auth_refresh_bg_op.is_static_config()) {
        _pool.load_credentials(_auth_refresh_bg_op.build_static_credentials());
    }
}

remote::remote(ss::sharded<configuration>& conf)
  : remote(
    conf.local().connection_limit,
    conf.local().client_config,
    conf.local().cloud_credentials_source) {}

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
    _as.request_abort();
    co_await _materialized->stop();
    co_await _pool.stop();
    co_await _gate.close();
}

void remote::shutdown_connections() { _pool.shutdown_connections(); }

size_t remote::concurrency() const { return _pool.max_size(); }

ss::future<download_result> remote::download_manifest(
  const cloud_storage_clients::bucket_name& bucket,
  const remote_manifest_path& key,
  base_manifest& manifest,
  retry_chain_node& parent) {
    return do_download_manifest(bucket, key, manifest, parent);
}

ss::future<download_result> remote::maybe_download_manifest(
  const cloud_storage_clients::bucket_name& bucket,
  const remote_manifest_path& key,
  base_manifest& manifest,
  retry_chain_node& parent) {
    return do_download_manifest(bucket, key, manifest, parent, true);
}

ss::future<download_result> remote::do_download_manifest(
  const cloud_storage_clients::bucket_name& bucket,
  const remote_manifest_path& key,
  base_manifest& manifest,
  retry_chain_node& parent,
  bool expect_missing) {
    gate_guard guard{_gate};
    retry_chain_node fib(&parent);
    retry_chain_logger ctxlog(cst_log, fib);
    auto path = cloud_storage_clients::object_key(key().native());
    auto lease = co_await _pool.acquire();
    auto retry_permit = fib.retry();
    std::optional<download_result> result;
    vlog(ctxlog.debug, "Download manifest {}", key());
    while (!_gate.is_closed() && retry_permit.is_allowed
           && !result.has_value()) {
        auto resp = co_await lease.client->get_object(
          bucket, path, fib.get_timeout(), expect_missing);

        if (resp) {
            vlog(ctxlog.debug, "Receive OK response from {}", path);
            co_await manifest.update(resp.value()->as_input_stream());
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
            }
            co_return download_result::success;
        }

        lease.client->shutdown();

        switch (resp.error()) {
        case cloud_storage_clients::error_outcome::none:
            vassert(
              false, "s3:error_outcome::none not expected on failure path");
        case cloud_storage_clients::error_outcome::retry_slowdown:
            [[fallthrough]];
        case cloud_storage_clients::error_outcome::retry:
            vlog(
              ctxlog.debug,
              "Downloading manifest from {}, {} backoff required",
              bucket,
              std::chrono::duration_cast<std::chrono::milliseconds>(
                retry_permit.delay));
            _probe.manifest_download_backoff();
            co_await ss::sleep_abortable(retry_permit.delay, _as);
            retry_permit = fib.retry();
            break;
        case cloud_storage_clients::error_outcome::bucket_not_found:
            [[fallthrough]];
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
  retry_chain_node& parent,
  const cloud_storage_clients::object_tag_formatter& tags) {
    gate_guard guard{_gate};
    retry_chain_node fib(&parent);
    retry_chain_logger ctxlog(cst_log, fib);
    auto key = manifest.get_manifest_path();
    auto path = cloud_storage_clients::object_key(key());
    auto lease = co_await _pool.acquire();
    auto permit = fib.retry();
    vlog(ctxlog.debug, "Uploading manifest {} to the {}", path, bucket());
    std::optional<upload_result> result;
    while (!_gate.is_closed() && permit.is_allowed && !result.has_value()) {
        auto [is, size] = manifest.serialize();
        const auto res = co_await lease.client->put_object(
          bucket, path, size, std::move(is), tags, fib.get_timeout());

        if (res) {
            vlog(ctxlog.debug, "Successfuly uploaded manifest to {}", path);
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
            }
            _probe.register_upload_size(size);
            co_return upload_result::success;
        }

        lease.client->shutdown();

        switch (res.error()) {
        case cloud_storage_clients::error_outcome::none:
            vassert(
              false, "s3:error_outcome::none not expected on failure path");
        case cloud_storage_clients::error_outcome::retry_slowdown:
            [[fallthrough]];
        case cloud_storage_clients::error_outcome::retry:
            vlog(
              ctxlog.debug,
              "Uploading manifest {} to {}, {} backoff required",
              path,
              bucket,
              std::chrono::duration_cast<std::chrono::milliseconds>(
                permit.delay));
            _probe.manifest_upload_backoff();
            co_await ss::sleep_abortable(permit.delay, _as);
            permit = fib.retry();
            break;
        case cloud_storage_clients::error_outcome::key_not_found:
            // not expected during upload
            [[fallthrough]];
        case cloud_storage_clients::error_outcome::bucket_not_found:
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

ss::future<upload_result> remote::upload_segment(
  const cloud_storage_clients::bucket_name& bucket,
  const remote_segment_path& segment_path,
  uint64_t content_length,
  const reset_input_stream& reset_str,
  retry_chain_node& parent,
  lazy_abort_source& lazy_abort_source,
  const cloud_storage_clients::object_tag_formatter& tags) {
    gate_guard guard{_gate};
    retry_chain_node fib(&parent);
    retry_chain_logger ctxlog(cst_log, fib);
    auto permit = fib.retry();
    vlog(
      ctxlog.debug,
      "Uploading segment to path {}, length {}",
      segment_path,
      content_length);
    std::optional<upload_result> result;
    while (!_gate.is_closed() && permit.is_allowed && !result) {
        auto lease = co_await _pool.acquire();

        // Client acquisition can take some time. Do a check before starting
        // the upload if we can still continue.
        if (lazy_abort_source.abort_requested()) {
            vlog(
              ctxlog.warn,
              "{}: cancelled uploading {} to {}",
              lazy_abort_source.abort_reason(),
              segment_path,
              bucket);
            _probe.failed_upload();
            co_return upload_result::cancelled;
        }

        auto reader_handle = co_await reset_str();
        auto path = cloud_storage_clients::object_key(segment_path());
        vlog(ctxlog.debug, "Uploading segment to path {}", segment_path);
        // Segment upload attempt
        auto res = co_await lease.client->put_object(
          bucket,
          path,
          content_length,
          reader_handle->take_stream(),
          tags,
          fib.get_timeout());

        // `put_object` closed the encapsulated input_stream, but we must
        // call close() on the segment_reader_handle to release the FD.
        co_await reader_handle->close();

        if (res) {
            _probe.successful_upload();
            _probe.register_upload_size(content_length);
            co_await reader_handle->close();
            co_return upload_result::success;
        }

        lease.client->shutdown();
        switch (res.error()) {
        case cloud_storage_clients::error_outcome::none:
            vassert(
              false, "s3:error_outcome::none not expected on failure path");
        case cloud_storage_clients::error_outcome::retry_slowdown:
            [[fallthrough]];
        case cloud_storage_clients::error_outcome::retry:
            vlog(
              ctxlog.debug,
              "Uploading segment {} to {}, {} backoff required",
              path,
              bucket,
              std::chrono::duration_cast<std::chrono::milliseconds>(
                permit.delay));
            _probe.upload_backoff();
            co_await ss::sleep_abortable(permit.delay, _as);
            permit = fib.retry();
            break;
        case cloud_storage_clients::error_outcome::key_not_found:
            // not expected during upload
            [[fallthrough]];
        case cloud_storage_clients::error_outcome::bucket_not_found:
            [[fallthrough]];
        case cloud_storage_clients::error_outcome::fail:
            result = upload_result::failed;
            break;
        }
    }

    _probe.failed_upload();

    if (!result) {
        vlog(
          ctxlog.warn,
          "Uploading segment {} to {}, backoff quota exceded, segment not "
          "uploaded",
          segment_path,
          bucket);
    } else {
        vlog(
          ctxlog.warn,
          "Uploading segment {} to {}, {}, segment not uploaded",
          segment_path,
          bucket,
          *result);
    }
    co_return upload_result::timedout;
}

ss::future<download_result> remote::download_segment(
  const cloud_storage_clients::bucket_name& bucket,
  const remote_segment_path& segment_path,
  const try_consume_stream& cons_str,
  retry_chain_node& parent) {
    gate_guard guard{_gate};
    retry_chain_node fib(&parent);
    retry_chain_logger ctxlog(cst_log, fib);
    auto path = cloud_storage_clients::object_key(segment_path());
    auto lease = co_await _pool.acquire();
    auto permit = fib.retry();
    vlog(ctxlog.debug, "Download segment {}", path);
    std::optional<download_result> result;
    while (!_gate.is_closed() && permit.is_allowed && !result) {
        auto resp = co_await lease.client->get_object(
          bucket, path, fib.get_timeout());

        if (resp) {
            vlog(ctxlog.debug, "Receive OK response from {}", path);
            auto length = boost::lexical_cast<uint64_t>(
              resp.value()->get_headers().at(
                boost::beast::http::field::content_length));
            uint64_t content_length = co_await cons_str(
              length, resp.value()->as_input_stream());
            _probe.successful_download();
            _probe.register_download_size(content_length);
            co_return download_result::success;
        }

        lease.client->shutdown();

        switch (resp.error()) {
        case cloud_storage_clients::error_outcome::none:
            vassert(
              false, "s3:error_outcome::none not expected on failure path");
        case cloud_storage_clients::error_outcome::retry_slowdown:
            [[fallthrough]];
        case cloud_storage_clients::error_outcome::retry:
            vlog(
              ctxlog.debug,
              "Downloading segment from {}, {} backoff required",
              bucket,
              std::chrono::duration_cast<std::chrono::milliseconds>(
                permit.delay));
            _probe.download_backoff();
            co_await ss::sleep_abortable(permit.delay, _as);
            permit = fib.retry();
            break;
        case cloud_storage_clients::error_outcome::bucket_not_found:
            [[fallthrough]];
        case cloud_storage_clients::error_outcome::fail:
            result = download_result::failed;
            break;
        case cloud_storage_clients::error_outcome::key_not_found:
            result = download_result::notfound;
            break;
        }
    }
    _probe.failed_download();
    if (!result) {
        vlog(
          ctxlog.warn,
          "Downloading segment from {}, backoff quota exceded, segment at {} "
          "not available",
          bucket,
          path);
        result = download_result::timedout;
    } else {
        vlog(
          ctxlog.warn,
          "Downloading segment from {}, {}, segment at {} not available",
          bucket,
          *result,
          path);
    }
    co_return *result;
}
ss::future<download_result> remote::segment_exists(
  const cloud_storage_clients::bucket_name& bucket,
  const remote_segment_path& segment_path,
  retry_chain_node& parent) {
    ss::gate::holder gh{_gate};
    retry_chain_node fib(&parent);
    retry_chain_logger ctxlog(cst_log, fib);
    auto path = cloud_storage_clients::object_key(segment_path());
    auto lease = co_await _pool.acquire();
    auto permit = fib.retry();
    vlog(ctxlog.debug, "Check segment {}", path);
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
        case cloud_storage_clients::error_outcome::none:
            vassert(
              false, "s3:error_outcome::none not expected on failure path");
        case cloud_storage_clients::error_outcome::retry_slowdown:
            [[fallthrough]];
        case cloud_storage_clients::error_outcome::retry:
            vlog(
              ctxlog.debug,
              "HeadObject from {}, {} backoff required",
              bucket,
              std::chrono::duration_cast<std::chrono::milliseconds>(
                permit.delay));
            co_await ss::sleep_abortable(permit.delay, _as);
            permit = fib.retry();
            break;
        case cloud_storage_clients::error_outcome::bucket_not_found:
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
          "HeadObject from {}, backoff quota exceded, segment at {} "
          "not available",
          bucket,
          path);
        result = download_result::timedout;
    } else {
        vlog(
          ctxlog.warn,
          "HeadObject from {}, {}, segment at {} not available",
          bucket,
          *result,
          path);
    }
    co_return *result;
}

ss::future<upload_result> remote::delete_object(
  const cloud_storage_clients::bucket_name& bucket,
  const cloud_storage_clients::object_key& path,
  retry_chain_node& parent) {
    ss::gate::holder gh{_gate};
    retry_chain_node fib(&parent);
    retry_chain_logger ctxlog(cst_log, fib);
    auto lease = co_await _pool.acquire();
    auto permit = fib.retry();
    vlog(ctxlog.debug, "Delete object {}", path);
    std::optional<upload_result> result;
    while (!_gate.is_closed() && permit.is_allowed && !result) {
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
        case cloud_storage_clients::error_outcome::none:
            vassert(
              false, "s3:error_outcome::none not expected on failure path");
        case cloud_storage_clients::error_outcome::retry_slowdown:
            [[fallthrough]];
        case cloud_storage_clients::error_outcome::retry:
            vlog(
              ctxlog.debug,
              "DeleteObject {}, {} backoff required",
              bucket,
              std::chrono::duration_cast<std::chrono::milliseconds>(
                permit.delay));
            co_await ss::sleep_abortable(permit.delay, _as);
            permit = fib.retry();
            break;
        case cloud_storage_clients::error_outcome::bucket_not_found:
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

ss::future<upload_result> remote::delete_objects(
  const cloud_storage_clients::bucket_name& bucket,
  std::vector<cloud_storage_clients::object_key> keys,
  retry_chain_node& parent) {
    ss::gate::holder gh{_gate};
    retry_chain_node fib(&parent);
    retry_chain_logger ctxlog(cst_log, fib);
    auto lease = co_await _pool.acquire();
    auto permit = fib.retry();
    vlog(ctxlog.debug, "Delete objects count {}", keys.size());
    std::optional<upload_result> result;
    while (!_gate.is_closed() && permit.is_allowed && !result) {
        auto res = co_await lease.client->delete_objects(
          bucket, keys, fib.get_timeout());

        if (res) {
            co_return upload_result::success;
        }

        lease.client->shutdown();

        switch (res.error()) {
        case cloud_storage_clients::error_outcome::none:
            vassert(
              false, "s3:error_outcome::none not expected on failure path");
        case cloud_storage_clients::error_outcome::retry_slowdown:
            [[fallthrough]];
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
        case cloud_storage_clients::error_outcome::bucket_not_found:
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
          "DeleteObjects {}, {}, backoff quota exceded, objects not deleted",
          keys.size(),
          bucket);
        result = upload_result::timedout;
    } else {
        vlog(
          ctxlog.warn,
          "DeleteObjects {}, {}, objects not deleted, error: {}",
          keys.size(),
          bucket,
          *result);
    }
    co_return *result;
}

ss::future<remote::list_result> remote::list_objects(
  const cloud_storage_clients::bucket_name& bucket,
  retry_chain_node& parent,
  std::optional<cloud_storage_clients::object_key> prefix) {
    ss::gate::holder gh{_gate};
    retry_chain_node fib(&parent);
    retry_chain_logger ctxlog(cst_log, fib);
    auto lease = co_await _pool.acquire();
    auto permit = fib.retry();
    vlog(ctxlog.debug, "List objects {}", bucket);
    std::optional<list_result> result;

    bool items_remaining = true;
    std::optional<ss::sstring> continuation_token = std::nullopt;

    // Gathers the items from a series of successful ListObjectsV2 calls
    list_bucket_items items;

    // Keep iterating until the ListObjectsV2 calls has more items to return
    while (!_gate.is_closed() && permit.is_allowed && !result) {
        auto res = co_await lease.client->list_objects(
          bucket,
          prefix,
          std::nullopt,
          std::nullopt,
          continuation_token,
          fib.get_timeout());

        if (res) {
            auto list_result = res.value();
            // Successful call, prepare for future calls by getting
            // continuation_token if result was truncated
            items_remaining = list_result.is_truncated;
            continuation_token.emplace(list_result.next_continuation_token);
            std::copy(
              std::make_move_iterator(list_result.contents.begin()),
              std::make_move_iterator(list_result.contents.end()),
              std::back_inserter(items));

            // Continue to list the remaining items
            if (items_remaining) {
                continue;
            }

            co_return items;
        }

        lease.client->shutdown();

        switch (res.error()) {
        case cloud_storage_clients::error_outcome::none:
            vassert(
              false, "s3:error_outcome::none not expected on failure path");
        case cloud_storage_clients::error_outcome::retry_slowdown:
            [[fallthrough]];
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
        case cloud_storage_clients::error_outcome::bucket_not_found:
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

ss::future<upload_result> remote::upload_object(
  const cloud_storage_clients::bucket_name& bucket,
  const cloud_storage_clients::object_key& object_path,
  ss::sstring payload,
  retry_chain_node& parent) {
    gate_guard guard{_gate};
    retry_chain_node fib(&parent);
    retry_chain_logger ctxlog(cst_log, fib);
    auto permit = fib.retry();
    auto content_length = payload.size();
    vlog(
      ctxlog.debug,
      "Uploading object to path {}, length {}",
      object_path,
      content_length);
    std::optional<upload_result> result;
    while (!_gate.is_closed() && permit.is_allowed && !result) {
        auto lease = co_await _pool.acquire();

        auto path = cloud_storage_clients::object_key(object_path());
        vlog(ctxlog.debug, "Uploading object to path {}", object_path);

        iobuf buffer;
        buffer.append(payload.data(), payload.size());
        auto res = co_await lease.client->put_object(
          bucket,
          path,
          content_length,
          make_iobuf_input_stream(std::move(buffer)),
          {{"rp-type", "recovery-lock"}},
          fib.get_timeout());

        if (res) {
            co_return upload_result::success;
        }

        lease.client->shutdown();
        switch (res.error()) {
        case cloud_storage_clients::error_outcome::none:
            vassert(
              false, "s3:error_outcome::none not expected on failure path");
        case cloud_storage_clients::error_outcome::retry_slowdown:
            [[fallthrough]];
        case cloud_storage_clients::error_outcome::retry:
            vlog(
              ctxlog.debug,
              "Uploading object {} to {}, {} backoff required",
              path,
              bucket,
              std::chrono::duration_cast<std::chrono::milliseconds>(
                permit.delay));
            co_await ss::sleep_abortable(permit.delay, _as);
            permit = fib.retry();
            break;
        case cloud_storage_clients::error_outcome::key_not_found:
            [[fallthrough]];
        case cloud_storage_clients::error_outcome::bucket_not_found:
            [[fallthrough]];
        case cloud_storage_clients::error_outcome::fail:
            result = upload_result::failed;
            break;
        }
    }

    if (!result) {
        vlog(
          ctxlog.warn,
          "Uploading object {} to {}, backoff quota exceded, object not "
          "uploaded",
          object_path,
          bucket);
    } else {
        vlog(
          ctxlog.warn,
          "Uploading object {} to {}, {}, object not uploaded",
          object_path,
          bucket,
          *result);
    }
    co_return upload_result::timedout;
}

ss::sstring lazy_abort_source::abort_reason() const { return _abort_reason; }

bool lazy_abort_source::abort_requested() { return _predicate(*this); }
void lazy_abort_source::abort_reason(ss::sstring reason) {
    _abort_reason = std::move(reason);
}

ss::future<>
remote::propagate_credentials(cloud_roles::credentials credentials) {
    return container().invoke_on_all(
      [c = std::move(credentials)](remote& svc) mutable {
          svc._pool.load_credentials(std::move(c));
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
            auto region_name = std::visit(
              [](const auto& cfg) {
                  using cfg_type = std::decay_t<decltype(cfg)>;
                  if constexpr (std::is_same_v<
                                  cloud_storage_clients::s3_configuration,
                                  cfg_type>) {
                      return cloud_roles::aws_region_name{cfg.region};
                  } else {
                      static_assert(
                        cloud_storage_clients::always_false_v<cfg_type>,
                        "Unknown client type");
                      return cloud_roles::aws_region_name{};
                  }
              },
              _client_conf);
            _refresh_credentials.emplace(cloud_roles::make_refresh_credentials(
              _cloud_credentials_source,
              _gate,
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
    return std::visit(
      [](const auto& cfg) {
          using cfg_type = std::decay_t<decltype(cfg)>;
          if constexpr (std::is_same_v<
                          cloud_storage_clients::s3_configuration,
                          cfg_type>) {
              return cloud_roles::aws_credentials{
                cfg.access_key.value(),
                cfg.secret_key.value(),
                std::nullopt,
                cfg.region};
          } else {
              static_assert(
                cloud_storage_clients::always_false_v<cfg_type>,
                "Unknown client type");
              return cloud_roles::aws_credentials{};
          }
      },
      _client_conf);
}

cloud_storage_clients::object_tag_formatter
remote::make_partition_manifest_tags(
  const model::ntp& ntp, model::initial_revision_id rev) {
    auto tags = default_partition_manifest_tags;
    tags.add("rp-ns", ntp.ns());
    tags.add("rp-topic", ntp.tp.topic());
    tags.add("rp-part", ntp.tp.partition());
    tags.add("rp-rev", rev());
    return tags;
}

cloud_storage_clients::object_tag_formatter remote::make_topic_manifest_tags(
  const model::topic_namespace& tns, model::initial_revision_id rev) {
    auto tags = default_topic_manifest_tags;
    tags.add("rp-ns", tns.ns());
    tags.add("rp-topic", tns.tp());
    tags.add("rp-rev", rev());
    return tags;
}

cloud_storage_clients::object_tag_formatter remote::make_segment_tags(
  const model::ntp& ntp, model::initial_revision_id rev) {
    auto tags = default_segment_tags;
    tags.add("rp-ns", ntp.ns());
    tags.add("rp-topic", ntp.tp.topic());
    tags.add("rp-part", ntp.tp.partition());
    tags.add("rp-rev", rev());
    return tags;
}

cloud_storage_clients::object_tag_formatter remote::make_tx_manifest_tags(
  const model::ntp& ntp, model::initial_revision_id rev) {
    // Note: tx-manifest is related to segment (contains data which are used
    // to consume data from the segment). Because of that it has the same
    // tags as the segment.
    return make_segment_tags(ntp, rev);
}

const cloud_storage_clients::object_tag_formatter remote::default_segment_tags
  = {{"rp-type", "segment"}};
const cloud_storage_clients::object_tag_formatter
  remote::default_topic_manifest_tags
  = {{"rp-type", "topic-manifest"}};
const cloud_storage_clients::object_tag_formatter
  remote::default_partition_manifest_tags
  = {{"rp-type", "partition-manifest"}};

} // namespace cloud_storage
