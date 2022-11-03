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

#include "cloud_storage/logger.h"
#include "cloud_storage/materialized_segments.h"
#include "cloud_storage/remote_segment.h"
#include "cloud_storage/types.h"
#include "s3/client.h"
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
#include <gnutls/gnutls.h>

#include <exception>
#include <utility>
#include <variant>

namespace cloud_storage {

using namespace std::chrono_literals;

enum class error_outcome {
    /// Error condition that could be retried
    retry,
    /// The service asked us to retry (SlowDown response)
    retry_slowdown,
    /// Error condition that couldn't be retried
    fail,
    /// NotFound API error (only suitable for downloads)
    notfound
};

/**
 * Identify error cases that should be quickly retried, e.g.
 * TCP disconnects, timeouts. Network errors may also show up
 * indirectly as errors from the TLS layer.
 */
bool system_error_retryable(const std::system_error& e) {
    auto v = e.code().value();

    // The name() of seastar's gnutls_error_category class
    constexpr std::string_view gnutls_cateogry_name{"GnuTLS"};

    if (e.code().category().name() == gnutls_cateogry_name) {
        switch (v) {
        case GNUTLS_E_PUSH_ERROR:
        case GNUTLS_E_PULL_ERROR:
        case GNUTLS_E_PREMATURE_TERMINATION:
            return true;
        default:
            return false;
        }
    } else {
        switch (v) {
        case ECONNREFUSED:
        case ENETUNREACH:
        case ETIMEDOUT:
        case ECONNRESET:
        case EPIPE:
            return true;
        default:
            return false;
        }
    }
    __builtin_unreachable();
}

/// @brief Analyze exception
/// @return error outcome - retry, fail (with exception), or notfound (can only
/// be used with download)
///
/// There're several error scopes that we're trying to choose from:
/// - errors that can be retried
///   - some network errors (connection reset by peer)
///   - S3 service backpressure (SlowDown responses)
///   - Short read errors (appear when S3 throttles redpanda)
/// - errors for which retrying is not an effective solution
///   - NotFound error for downloads
///   - FS errors
///   - errors generated during graceful shutdown, etc
static error_outcome categorize_error(
  const std::exception_ptr& err,
  retry_chain_node& fib,
  const s3::bucket_name& bucket,
  const s3::object_key& path) {
    auto result = error_outcome::retry;
    retry_chain_logger ctxlog(cst_log, fib);
    try {
        std::rethrow_exception(err);
    } catch (const s3::rest_error_response& err) {
        if (err.code() == s3::s3_error_code::no_such_key) {
            // Unexpected 404s are logged elsewhere by the s3 client at warn
            // level, so only log at debug level here.
            vlog(ctxlog.debug, "NoSuchKey response received {}", path);
            result = error_outcome::notfound;
        } else if (
          err.code() == s3::s3_error_code::slow_down
          || err.code() == s3::s3_error_code::internal_error) {
            // This can happen when we're dealing with high request rate to
            // the manifest's prefix. Backoff algorithm should be applied.
            // In principle only slow_down should occur, but in practice
            // AWS S3 does return internal_error as well sometimes.
            vlog(ctxlog.warn, "{} response received {}", err.code(), path);
            result = error_outcome::retry_slowdown;
        } else {
            // Unexpected REST API error, we can't recover from this
            // because the issue is not temporary (e.g. bucket doesn't
            // exist)
            vlog(
              ctxlog.error,
              "Accessing {}, unexpected REST API error \"{}\" detected, "
              "code: "
              "{}, request_id: {}, resource: {}",
              bucket,
              err.message(),
              err.code_string(),
              err.request_id(),
              err.resource());
            result = error_outcome::fail;
        }
    } catch (const std::system_error& cerr) {
        // The system_error is type erased and not convenient for selective
        // handling. The following errors should be retried:
        // - connection refused, timed out or reset by peer
        // - network temporary unavailable
        // Shouldn't be retried
        // - any filesystem error
        // - broken-pipe
        // - any other network error (no memory, bad socket, etc)
        if (system_error_retryable(cerr)) {
            vlog(
              ctxlog.warn,
              "System error susceptible for retry {}",
              cerr.what());
        } else {
            vlog(ctxlog.error, "System error {}", cerr);
            result = error_outcome::fail;
        }
    } catch (const ss::timed_out_error& terr) {
        // This should happen when the connection pool was disconnected
        // from the S3 endpoint and subsequent connection attmpts failed.
        vlog(ctxlog.warn, "Connection timeout {}", terr.what());
    } catch (const boost::system::system_error& err) {
        if (err.code() != boost::beast::http::error::short_read) {
            vlog(cst_log.warn, "Connection failed {}", err.what());
            result = error_outcome::fail;
        } else {
            // This is a short read error that can be caused by the abrupt TLS
            // shutdown. The content of the received buffer is discarded in this
            // case and http client receives an empty buffer.
            vlog(
              ctxlog.info,
              "Server disconnected: '{}', retrying HTTP request",
              err.what());
        }
    } catch (const ss::abort_requested_exception&) {
        vlog(ctxlog.debug, "Abort requested");
        throw;
    } catch (...) {
        vlog(ctxlog.error, "Unexpected error {}", std::current_exception());
        result = error_outcome::fail;
    }
    return result;
}

remote::remote(
  s3_connection_limit limit,
  const s3::configuration& conf,
  model::cloud_credentials_source cloud_credentials_source)
  : _pool(limit(), conf)
  , _auth_refresh_bg_op{_gate, _as, conf, cloud_credentials_source}
  , _materialized(std::make_unique<materialized_segments>())
  , _probe(
      remote_metrics_disabled(static_cast<bool>(conf.disable_metrics)),
      remote_metrics_disabled(static_cast<bool>(conf.disable_public_metrics)),
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
  const s3::bucket_name& bucket,
  const remote_manifest_path& key,
  base_manifest& manifest,
  retry_chain_node& parent) {
    return do_download_manifest(bucket, key, manifest, parent);
}

ss::future<download_result> remote::maybe_download_manifest(
  const s3::bucket_name& bucket,
  const remote_manifest_path& key,
  base_manifest& manifest,
  retry_chain_node& parent) {
    return do_download_manifest(bucket, key, manifest, parent, true);
}

ss::future<download_result> remote::do_download_manifest(
  const s3::bucket_name& bucket,
  const remote_manifest_path& key,
  base_manifest& manifest,
  retry_chain_node& parent,
  bool expect_missing) {
    gate_guard guard{_gate};
    retry_chain_node fib(&parent);
    retry_chain_logger ctxlog(cst_log, fib);
    auto path = s3::object_key(key().native());
    auto lease = co_await _pool.acquire();
    auto retry_permit = fib.retry();
    std::optional<download_result> result;
    vlog(ctxlog.debug, "Download manifest {}", key());
    while (!_gate.is_closed() && retry_permit.is_allowed
           && !result.has_value()) {
        std::exception_ptr eptr = nullptr;
        try {
            auto resp = co_await lease.client->get_object(
              bucket, path, fib.get_timeout(), expect_missing);
            vlog(ctxlog.debug, "Receive OK response from {}", path);
            co_await manifest.update(resp->as_input_stream());
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
        } catch (...) {
            eptr = std::current_exception();
        }
        lease.client->shutdown();
        auto outcome = categorize_error(eptr, fib, bucket, path);
        switch (outcome) {
        case error_outcome::retry_slowdown:
            [[fallthrough]];
        case error_outcome::retry:
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
        case error_outcome::fail:
            result = download_result::failed;
            vlog(
              ctxlog.warn,
              "Failed downloading manifest from {} {}, manifest at {}",
              bucket,
              *result,
              path);
            break;
        case error_outcome::notfound:
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
  const s3::bucket_name& bucket,
  const base_manifest& manifest,
  retry_chain_node& parent) {
    gate_guard guard{_gate};
    retry_chain_node fib(&parent);
    retry_chain_logger ctxlog(cst_log, fib);
    auto key = manifest.get_manifest_path();
    auto path = s3::object_key(key());
    std::vector<s3::object_tag> tags = {{"rp-type", "partition-manifest"}};
    auto lease = co_await _pool.acquire();
    auto permit = fib.retry();
    vlog(ctxlog.debug, "Uploading manifest {} to the {}", path, bucket());
    std::optional<upload_result> result;
    while (!_gate.is_closed() && permit.is_allowed && !result.has_value()) {
        std::exception_ptr eptr = nullptr;
        try {
            auto [is, size] = manifest.serialize();
            co_await lease.client->put_object(
              bucket, path, size, std::move(is), tags, fib.get_timeout());
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
        } catch (...) {
            eptr = std::current_exception();
        }
        lease.client->shutdown();
        auto outcome = categorize_error(eptr, fib, bucket, path);
        switch (outcome) {
        case error_outcome::retry_slowdown:
            [[fallthrough]];
        case error_outcome::retry:
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
        case error_outcome::notfound:
            // not expected during upload
        case error_outcome::fail:
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
  const s3::bucket_name& bucket,
  const remote_segment_path& segment_path,
  uint64_t content_length,
  const reset_input_stream& reset_str,
  retry_chain_node& parent,
  lazy_abort_source& lazy_abort_source) {
    gate_guard guard{_gate};
    retry_chain_node fib(&parent);
    retry_chain_logger ctxlog(cst_log, fib);
    std::vector<s3::object_tag> tags = {{"rp-type", "segment"}};
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
        auto path = s3::object_key(segment_path());
        vlog(ctxlog.debug, "Uploading segment to path {}", segment_path);
        std::exception_ptr eptr = nullptr;
        try {
            // Segment upload attempt
            co_await lease.client->put_object(
              bucket,
              path,
              content_length,
              reader_handle->take_stream(),
              tags,
              fib.get_timeout());
            _probe.successful_upload();
            _probe.register_upload_size(content_length);
            co_await reader_handle->close();
            co_return upload_result::success;
        } catch (...) {
            eptr = std::current_exception();
        }

        // `put_object` closed the encapsulated input_stream, but we must
        // call close() on the segment_reader_handle to release the FD.
        co_await reader_handle->close();

        lease.client->shutdown();
        auto outcome = categorize_error(eptr, fib, bucket, path);
        switch (outcome) {
        case error_outcome::retry_slowdown:
            [[fallthrough]];
        case error_outcome::retry:
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
        case error_outcome::notfound:
            // not expected during upload
        case error_outcome::fail:
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
  const s3::bucket_name& bucket,
  const remote_segment_path& segment_path,
  const try_consume_stream& cons_str,
  retry_chain_node& parent) {
    gate_guard guard{_gate};
    retry_chain_node fib(&parent);
    retry_chain_logger ctxlog(cst_log, fib);
    auto path = s3::object_key(segment_path());
    auto lease = co_await _pool.acquire();
    auto permit = fib.retry();
    vlog(ctxlog.debug, "Download segment {}", path);
    std::optional<download_result> result;
    while (!_gate.is_closed() && permit.is_allowed && !result) {
        std::exception_ptr eptr = nullptr;
        try {
            auto resp = co_await lease.client->get_object(
              bucket, path, fib.get_timeout());
            vlog(ctxlog.debug, "Receive OK response from {}", path);
            auto length = boost::lexical_cast<uint64_t>(resp->get_headers().at(
              boost::beast::http::field::content_length));
            uint64_t content_length = co_await cons_str(
              length, resp->as_input_stream());
            _probe.successful_download();
            _probe.register_download_size(content_length);
            co_return download_result::success;
        } catch (...) {
            eptr = std::current_exception();
        }
        lease.client->shutdown();
        auto outcome = categorize_error(eptr, fib, bucket, path);
        switch (outcome) {
        case error_outcome::retry_slowdown:
            [[fallthrough]];
        case error_outcome::retry:
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
        case error_outcome::fail:
            result = download_result::failed;
            break;
        case error_outcome::notfound:
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
  const s3::bucket_name& bucket,
  const remote_segment_path& segment_path,
  retry_chain_node& parent) {
    ss::gate::holder gh{_gate};
    retry_chain_node fib(&parent);
    retry_chain_logger ctxlog(cst_log, fib);
    auto path = s3::object_key(segment_path());
    auto lease = co_await _pool.acquire();
    auto permit = fib.retry();
    vlog(ctxlog.debug, "Check segment {}", path);
    std::optional<download_result> result;
    while (!_gate.is_closed() && permit.is_allowed && !result) {
        std::exception_ptr eptr = nullptr;
        try {
            auto resp = co_await lease.client->head_object(
              bucket, path, fib.get_timeout());
            vlog(
              ctxlog.debug,
              "Receive OK HeadObject response from {}, object size: {}, etag: "
              "{}",
              path,
              resp.object_size,
              resp.etag);
            co_return download_result::success;
        } catch (...) {
            eptr = std::current_exception();
        }
        lease.client->shutdown();
        auto outcome = categorize_error(eptr, fib, bucket, path);
        switch (outcome) {
        case error_outcome::retry_slowdown:
            [[fallthrough]];
        case error_outcome::retry:
            vlog(
              ctxlog.debug,
              "HeadObject from {}, {} backoff required",
              bucket,
              std::chrono::duration_cast<std::chrono::milliseconds>(
                permit.delay));
            co_await ss::sleep_abortable(permit.delay, _as);
            permit = fib.retry();
            break;
        case error_outcome::fail:
            result = download_result::failed;
            break;
        case error_outcome::notfound:
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
  const s3::bucket_name& bucket,
  const s3::object_key& path,
  retry_chain_node& parent) {
    ss::gate::holder gh{_gate};
    retry_chain_node fib(&parent);
    retry_chain_logger ctxlog(cst_log, fib);
    auto lease = co_await _pool.acquire();
    auto permit = fib.retry();
    vlog(ctxlog.debug, "Delete object {}", path);
    std::optional<upload_result> result;
    while (!_gate.is_closed() && permit.is_allowed && !result) {
        std::exception_ptr eptr = nullptr;
        try {
            // NOTE: DeleteObject in S3 doesn't return an error
            // if the object doesn't exist. Because of that we're
            // using 'upload_result' type as a return type. No need
            // to handle NoSuchKey error. The 'upload_result' represents
            // any mutable operation.
            co_await lease.client->delete_object(
              bucket, path, fib.get_timeout());
            vlog(ctxlog.debug, "Receive OK DeleteObject {} response", path);
            co_return upload_result::success;
        } catch (...) {
            eptr = std::current_exception();
        }
        lease.client->shutdown();
        auto outcome = categorize_error(eptr, fib, bucket, path);
        switch (outcome) {
        case error_outcome::retry_slowdown:
            [[fallthrough]];
        case error_outcome::retry:
            vlog(
              ctxlog.debug,
              "DeleteObject {}, {} backoff required",
              bucket,
              std::chrono::duration_cast<std::chrono::milliseconds>(
                permit.delay));
            co_await ss::sleep_abortable(permit.delay, _as);
            permit = fib.retry();
            break;
        case error_outcome::fail:
            result = upload_result::failed;
            break;
        case error_outcome::notfound:
            vassert(
              false,
              "Unexpected NoSuchKey error response from DeleteObject {}",
              path);
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
  s3::configuration s3_conf,
  model::cloud_credentials_source cloud_credentials_source)
  : _gate(gate)
  , _as(as)
  , _s3_conf(std::move(s3_conf))
  , _region_name{_s3_conf.region}
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
            _refresh_credentials.emplace(cloud_roles::make_refresh_credentials(
              _cloud_credentials_source,
              _gate,
              _as,
              std::move(credentials_update_cb),
              _region_name));

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
    return cloud_roles::aws_credentials{
      _s3_conf.access_key.value(),
      _s3_conf.secret_key.value(),
      std::nullopt,
      _region_name};
}

} // namespace cloud_storage
