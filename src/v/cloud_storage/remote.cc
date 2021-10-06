/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/remote.h"

#include "cloud_storage/logger.h"
#include "cloud_storage/types.h"
#include "s3/client.h"
#include "ssx/sformat.h"
#include "utils/intrusive_list_helpers.h"
#include "utils/retry_chain_node.h"
#include "utils/string_switch.h"

#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/weak_ptr.hh>

#include <boost/beast/http/error.hpp>
#include <boost/beast/http/field.hpp>

#include <exception>
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
            vlog(ctxlog.warn, "NoSuchKey response received {}", path);
            result = error_outcome::notfound;
        } else if (err.code() == s3::s3_error_code::slow_down) {
            // This can happen when we're dealing with high request rate to
            // the manifest's prefix. Backoff algorithm should be applied.
            vlog(ctxlog.debug, "SlowDown response received {}", path);
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
        if (auto code = cerr.code();
            code.value() != ECONNREFUSED && code.value() != ENETUNREACH
            && code.value() != ETIMEDOUT && code.value() != ECONNRESET) {
            vlog(ctxlog.error, "System error {}", cerr);
            result = error_outcome::fail;
        } else {
            vlog(
              ctxlog.debug,
              "System error susceptible for retry {}",
              cerr.what());
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
            vlog(ctxlog.warn, "Short Read detected, retrying {}", err.what());
        }
    } catch (...) {
        vlog(ctxlog.error, "Unexpected error {}", std::current_exception());
        result = error_outcome::fail;
    }
    return result;
}

remote::remote(s3_connection_limit limit, const s3::configuration& conf)
  : _pool(limit(), conf)
  , _probe(remote_metrics_disabled(static_cast<bool>(conf.disable_metrics))) {}

remote::remote(ss::sharded<configuration>& conf)
  : remote(conf.local().connection_limit, conf.local().client_config) {}

ss::future<> remote::start() { return ss::now(); }

ss::future<> remote::stop() {
    _as.request_abort();
    co_await _pool.stop();
    co_await _gate.close();
}

size_t remote::concurrency() const { return _pool.max_size(); }

ss::future<download_result> remote::download_manifest(
  const s3::bucket_name& bucket,
  const remote_manifest_path& key,
  base_manifest& manifest,
  retry_chain_node& parent) {
    gate_guard guard{_gate};
    retry_chain_node fib(&parent);
    retry_chain_logger ctxlog(cst_log, fib);
    auto path = s3::object_key(key().native());
    auto [client, deleter] = co_await _pool.acquire();
    auto retry_permit = fib.retry();
    std::optional<download_result> result;
    vlog(ctxlog.debug, "Download manifest {}", key());
    while (!_gate.is_closed() && retry_permit.is_allowed
           && !result.has_value()) {
        std::exception_ptr eptr = nullptr;
        try {
            auto resp = co_await client->get_object(
              bucket, path, fib.get_timeout());
            vlog(ctxlog.debug, "Receive OK response from {}", path);
            co_await manifest.update(resp->as_input_stream());
            switch (manifest.get_manifest_type()) {
            case manifest_type::partition:
                _probe.partition_manifest_download();
                break;
            case manifest_type::topic:
                _probe.topic_manifest_download();
                break;
            }
            co_return download_result::success;
        } catch (...) {
            eptr = std::current_exception();
        }
        auto outcome = categorize_error(eptr, fib, bucket, path);
        switch (outcome) {
        case error_outcome::retry_slowdown:
            // We have to close the connection upon receiving the 'SlowDown'
            // response from S3. If we won't do this and just sleep the required
            // number of milliseconds the next request that will use this
            // connection will trigger a 'short read' error. The error means
            // that the S3 forcibly closes the connection. This doesn't happen
            // when we close and then reestablish the connection upon receiving
            // the 'SlowDown' response (if long enough backoff period is used).
            co_await client->shutdown();
            [[fallthrough]];
        case error_outcome::retry:
            vlog(
              ctxlog.debug,
              "Downloading manifest from {}, {}ms backoff required",
              bucket,
              retry_permit.delay.count());
            _probe.manifest_download_backoff();
            co_await ss::sleep_abortable(retry_permit.delay, _as);
            retry_permit = fib.retry();
            break;
        case error_outcome::fail:
            result = download_result::failed;
        case error_outcome::notfound:
            result = download_result::notfound;
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
    } else {
        vlog(
          ctxlog.warn,
          "Downloading manifest from {} {}, manifest at {} not available",
          bucket,
          *result,
          path);
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
    auto path = s3::object_key(key().string());
    std::vector<s3::object_tag> tags = {{"rp-type", "partition-manifest"}};
    auto [client, deleter] = co_await _pool.acquire();
    auto permit = fib.retry();
    vlog(ctxlog.debug, "Uploading manifest {} to the {}", path, bucket());
    std::optional<upload_result> result;
    while (!_gate.is_closed() && permit.is_allowed && !result.has_value()) {
        std::exception_ptr eptr = nullptr;
        try {
            auto [is, size] = manifest.serialize();
            co_await client->put_object(
              bucket, path, size, std::move(is), tags, fib.get_timeout());
            vlog(ctxlog.debug, "Successfuly uploaded manifest to {}", path);
            switch (manifest.get_manifest_type()) {
            case manifest_type::partition:
                _probe.partition_manifest_upload();
                break;
            case manifest_type::topic:
                _probe.topic_manifest_upload();
                break;
            }
            _probe.register_upload_size(size);
            co_return upload_result::success;
        } catch (...) {
            eptr = std::current_exception();
        }
        auto outcome = categorize_error(eptr, fib, bucket, path);
        switch (outcome) {
        case error_outcome::retry_slowdown:
            co_await client->shutdown();
            [[fallthrough]];
        case error_outcome::retry:
            vlog(
              ctxlog.debug,
              "Uploading manifest {} to {}, {}ms backoff required",
              path,
              bucket,
              permit.delay.count());
            _probe.manifest_upload_backoff();
            co_await ss::sleep_abortable(permit.delay, _as);
            permit = fib.retry();
            break;
        case error_outcome::notfound:
            // not expected during upload
        case error_outcome::fail:
            result = upload_result::failed;
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
  const segment_name& exposed_name,
  uint64_t content_length,
  const reset_input_stream& reset_str,
  manifest& manifest,
  retry_chain_node& parent) {
    gate_guard guard{_gate};
    retry_chain_node fib(&parent);
    retry_chain_logger ctxlog(cst_log, fib);
    auto s3path = manifest.get_remote_segment_path(exposed_name);
    std::vector<s3::object_tag> tags = {{"rp-type", "segment"}};
    auto permit = fib.retry();
    vlog(
      ctxlog.debug,
      "Uploading segment for {}, exposed name {}, length {}",
      manifest.get_ntp(),
      exposed_name,
      content_length);
    std::optional<upload_result> result;
    while (!_gate.is_closed() && permit.is_allowed && !result) {
        auto [client, deleter] = co_await _pool.acquire();
        auto stream = reset_str();
        auto path = s3::object_key(s3path().string());
        vlog(
          ctxlog.debug,
          "Uploading segment for {}, path {}",
          manifest.get_ntp(),
          s3path);
        std::exception_ptr eptr = nullptr;
        try {
            // Segment upload attempt
            co_await client->put_object(
              bucket,
              path,
              content_length,
              std::move(stream),
              tags,
              fib.get_timeout());
            _probe.successful_upload();
            _probe.register_upload_size(content_length);
            co_return upload_result::success;
        } catch (...) {
            eptr = std::current_exception();
        }
        auto outcome = categorize_error(eptr, fib, bucket, path);
        switch (outcome) {
        case error_outcome::retry_slowdown:
            co_await client->shutdown();
            [[fallthrough]];
        case error_outcome::retry:
            vlog(
              ctxlog.debug,
              "Uploading segment {} to {}, {}ms backoff required",
              path,
              bucket,
              permit.delay.count());
            _probe.upload_backoff();
            co_await ss::sleep_abortable(permit.delay, _as);
            permit = fib.retry();
            break;
        case error_outcome::notfound:
            // not expected during upload
        case error_outcome::fail:
            result = upload_result::failed;
        }
    }
    _probe.failed_upload();
    if (!result) {
        vlog(
          ctxlog.warn,
          "Uploading segment {} to {}, backoff quota exceded, segment not "
          "uploaded",
          s3path,
          bucket);
    } else {
        vlog(
          ctxlog.warn,
          "Uploading segment {} to {}, {}, segment not uploaded",
          s3path,
          *result,
          bucket);
    }
    co_return upload_result::timedout;
}

ss::future<download_result> remote::download_segment(
  const s3::bucket_name& bucket,
  const manifest::key& name,
  const manifest& manifest,
  const try_consume_stream& cons_str,
  retry_chain_node& parent) {
    gate_guard guard{_gate};
    retry_chain_node fib(&parent);
    retry_chain_logger ctxlog(cst_log, fib);
    auto s3path = manifest.get_remote_segment_path(name);
    auto path = s3::object_key(s3path().string());
    auto [client, deleter] = co_await _pool.acquire();
    auto permit = fib.retry();
    vlog(ctxlog.debug, "Download segment {}", path);
    std::optional<download_result> result;
    while (!_gate.is_closed() && permit.is_allowed && !result) {
        std::exception_ptr eptr = nullptr;
        try {
            auto resp = co_await client->get_object(
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
        auto outcome = categorize_error(eptr, fib, bucket, path);
        switch (outcome) {
        case error_outcome::retry_slowdown:
            co_await client->shutdown();
            [[fallthrough]];
        case error_outcome::retry:
            vlog(
              ctxlog.debug,
              "Downloading segment from {}, {}ms backoff required",
              bucket,
              permit.delay.count());
            _probe.download_backoff();
            co_await ss::sleep_abortable(permit.delay, _as);
            permit = fib.retry();
            break;
        case error_outcome::fail:
            result = download_result::failed;
        case error_outcome::notfound:
            result = download_result::notfound;
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

static const s3::object_key empty_object_key = s3::object_key();

ss::future<download_result> remote::list_objects(
  const list_objects_consumer& cons,
  const s3::bucket_name& bucket,
  const std::optional<s3::object_key>& prefix,
  const std::optional<size_t>& max_keys,
  retry_chain_node& parent) {
    gate_guard guard{_gate};
    retry_chain_node fib(&parent);
    retry_chain_logger ctxlog(cst_log, fib);
    auto permit = fib.retry();
    auto [client, deleter] = co_await _pool.acquire();
    std::optional<s3::object_key> last_key;
    s3::client::list_bucket_result res{
      .is_truncated = true, .prefix = {}, .contents = {}};
    while (res.is_truncated) {
        std::exception_ptr eptr;
        try {
            auto res = co_await client->list_objects_v2(
              bucket, prefix, last_key, max_keys, fib.get_timeout());
            vlog(
              ctxlog.debug,
              "ListObjectV2 response, is_truncated {}, prefix {}, num-keys {}",
              res.is_truncated,
              res.prefix,
              res.contents.size());
            ss::stop_iteration cons_res = ss::stop_iteration::no;
            for (auto&& it : res.contents) {
                vlog(ctxlog.debug, "Response object key {}", it.key);
                cons_res = cons(
                  std::move(it.key),
                  it.last_modified,
                  it.size_bytes,
                  std::move(it.etag));
                if (cons_res == ss::stop_iteration::yes) {
                    break;
                }
            }
            if (!res.contents.empty()) {
                last_key = s3::object_key(
                  std::filesystem::path(res.contents.back().key));
            }
            if (!res.is_truncated || cons_res == ss::stop_iteration::yes) {
                co_return download_result::success;
            }
        } catch (...) {
            eptr = std::current_exception();
        }
        auto outcome = categorize_error(eptr, fib, bucket, empty_object_key);
        switch (outcome) {
        case error_outcome::retry_slowdown:
            co_await client->shutdown();
            [[fallthrough]];
        case error_outcome::retry:
            vlog(
              ctxlog.debug,
              "Listing objects in {}, {}ms backoff required",
              bucket,
              permit.delay.count());
            _probe.download_backoff();
            co_await ss::sleep_abortable(permit.delay, _as);
            permit = fib.retry();
            break;
        case error_outcome::fail:
        case error_outcome::notfound:
            co_return download_result::failed;
        }
    }
    vlog(ctxlog.warn, "ListObjectsV2 backoff quota exceded");
    _probe.failed_download();
    co_return download_result::timedout;
}

} // namespace cloud_storage
