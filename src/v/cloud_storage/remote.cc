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
#include "cloud_io/transfer_details.h"
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
  ss::sharded<cloud_io::remote>& io,
  const cloud_storage_clients::client_configuration& conf)
  : _io(io)
  , _materialized(std::make_unique<materialized_resources>())
  , _probe(
      remote_metrics_disabled(static_cast<bool>(
        std::visit([](auto&& cfg) { return cfg.disable_metrics; }, conf))),
      remote_metrics_disabled(static_cast<bool>(std::visit(
        [](auto&& cfg) { return cfg.disable_public_metrics; }, conf))),
      *_materialized,
      _io.local().resources()) {}

remote::remote(ss::sharded<cloud_io::remote>& io, const configuration& conf)
  : remote(io, conf.client_config) {}

remote::~remote() {
    // This is declared in the .cc to avoid header trying to
    // link with destructors for unique_ptr wrapped members
}

ss::future<> remote::start() { co_await _materialized->start(); }

ss::future<> remote::stop() {
    cst_log.debug("Stopping remote...");
    co_await _materialized->stop();
    cst_log.debug("Stopped remote...");
}

size_t remote::concurrency() const { return io().concurrency(); }

bool remote::is_batch_delete_supported() const {
    return io().is_batch_delete_supported();
}

int remote::delete_objects_max_keys() const {
    return io().delete_objects_max_keys();
}

ss::future<download_result> remote::download_manifest(
  const cloud_storage_clients::bucket_name& bucket,
  const std::pair<manifest_format, remote_manifest_path>& format_key,
  base_manifest& manifest,
  retry_chain_node& parent) {
    return do_download_manifest(bucket, format_key, manifest, parent);
}

ss::future<download_result> remote::download_manifest_json(
  const cloud_storage_clients::bucket_name& bucket,
  const remote_manifest_path& key,
  base_manifest& manifest,
  retry_chain_node& parent) {
    auto fk = std::pair{manifest_format::json, key};
    co_return co_await do_download_manifest(
      bucket, fk, manifest, parent, false);
}
ss::future<download_result> remote::download_manifest_bin(
  const cloud_storage_clients::bucket_name& bucket,
  const remote_manifest_path& key,
  base_manifest& manifest,
  retry_chain_node& parent) {
    auto fk = std::pair{manifest_format::serde, key};
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

ss::future<download_result> remote::do_download_manifest(
  const cloud_storage_clients::bucket_name& bucket,
  const std::pair<manifest_format, remote_manifest_path>& format_key,
  base_manifest& manifest,
  retry_chain_node& parent,
  bool expect_missing) {
    auto path = cloud_storage_clients::object_key(format_key.second().native());
    iobuf buffer;
    const auto dl_result = co_await io().download_object(
      {.transfer_details = {
         .bucket = bucket,
         .key = path,
         .parent_rtc = parent,
         // NOTE: appropriate probe is only updated after parsing the manifest.
         .success_cb = std::nullopt,
         .failure_cb = [this]() { _probe.failed_manifest_download(); },
         .backoff_cb = [this]() { _probe.manifest_download_backoff(); },
         .on_req_cb = make_notify_cb(
           api_activity_type::manifest_download, parent),
       },
       .display_str = "manifest",
       .payload = buffer,
       .expect_missing = expect_missing});
    if (dl_result == download_result::success) {
        auto fut = co_await ss::coroutine::as_future(manifest.update(
          format_key.first, make_iobuf_input_stream(std::move(buffer))));
        if (fut.failed()) {
            const auto ex = fut.get_exception();
            auto res = download_result::failed;
            vlog(
              cst_log.warn,
              "Failed downloading manifest from {} {}, manifest at {}",
              bucket,
              ex,
              path);
            co_return res;
        }
        switch (manifest.get_manifest_type()) {
            using enum manifest_type;
        case partition:
            _probe.partition_manifest_download();
            break;
        case topic:
            _probe.topic_manifest_download();
            break;
        case tx_range:
            _probe.txrange_manifest_download();
            break;
        case cluster_metadata:
            _probe.cluster_metadata_manifest_download();
            break;
        case spillover:
            _probe.spillover_manifest_download();
            break;
        case topic_mount:
            _probe.topic_mount_manifest_download();
            break;
        }
    }
    co_return dl_result;
}

ss::future<upload_result> remote::upload_manifest(
  const cloud_storage_clients::bucket_name& bucket,
  const base_manifest& manifest,
  const remote_manifest_path& key,
  retry_chain_node& parent) {
    auto buf = co_await manifest.serialize_buf();
    auto success_cb = [this, t = manifest.get_manifest_type()]() {
        switch (t) {
            using enum manifest_type;
        case partition:
            _probe.partition_manifest_upload();
            break;
        case topic:
            _probe.topic_manifest_upload();
            break;
        case tx_range:
            _probe.txrange_manifest_upload();
            break;
        case cluster_metadata:
            _probe.cluster_metadata_manifest_upload();
            break;
        case spillover:
            _probe.spillover_manifest_upload();
            break;
        case topic_mount:
            _probe.topic_mount_manifest_upload();
            break;
        }
    };
    co_return co_await io().upload_object({
      .transfer_details = {
        .bucket = bucket,
        .key = cloud_storage_clients::object_key{key().native()},
        .parent_rtc = parent,
        .success_cb = std::move(success_cb),
        .failure_cb = [this] { _probe.failed_manifest_upload(); },
        .backoff_cb = [this] { _probe.manifest_upload_backoff(); },
        .on_req_cb = make_notify_cb(api_activity_type::manifest_upload, parent),
      },
      .display_str = to_string(upload_type::manifest),
      .payload = std::move(buf),
      .accept_no_content_response = false,
    });
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
        using provider_t = std::unique_ptr<stream_provider>;
        ss::file_input_stream_options opts;
        return ss::make_ready_future<provider_t>(
          std::make_unique<storage::segment_reader_handle>(
            ss::make_file_input_stream(file, opts)));
    };
    auto file_size = co_await file.size();
    co_return co_await io().upload_stream(
      {
        .bucket = bucket,
        .key = cloud_storage_clients::object_key{remote_path()},
        .parent_rtc = parent,
        .success_cb =
          [this] { _probe.controller_snapshot_successful_upload(); },
        // TODO: should use a different metric for controller snapshot size.
        .success_size_cb =
          [this](size_t sz) { _probe.register_upload_size(sz); },
        .failure_cb = [this] { _probe.controller_snapshot_failed_upload(); },
        .backoff_cb = [this] { _probe.controller_snapshot_upload_backoff(); },
        .on_req_cb = make_notify_cb(
          api_activity_type::controller_snapshot_upload, parent),
      },
      file_size,
      reset_str,
      lazy_abort_source,
      "controller snapshot",
      std::nullopt);
}

ss::future<upload_result> remote::upload_segment(
  const cloud_storage_clients::bucket_name& bucket,
  const remote_segment_path& segment_path,
  uint64_t content_length,
  const reset_input_stream& reset_str,
  retry_chain_node& parent,
  lazy_abort_source& lazy_abort_source,
  std::optional<size_t> max_retries) {
    return io().upload_stream(
      {
        .bucket = bucket,
        .key = cloud_storage_clients::object_key{segment_path()},
        .parent_rtc = parent,
        .success_cb = [this] { _probe.successful_upload(); },
        .success_size_cb =
          [this](size_t sz) { _probe.register_upload_size(sz); },
        .failure_cb = [this] { _probe.failed_upload(); },
        .backoff_cb = [this] { _probe.upload_backoff(); },
        .on_req_cb = make_notify_cb(api_activity_type::segment_upload, parent),
      },
      content_length,
      reset_str,
      lazy_abort_source,
      "segment",
      max_retries);
}

ss::future<upload_result> remote::upload_index(
  const cloud_storage_clients::bucket_name& bucket,
  const cloud_storage_clients::object_key& key,
  const offset_index& index,
  retry_chain_node& parent) {
    auto buf = index.to_iobuf();
    return io().upload_object({
      .transfer_details = {
        .bucket = bucket,
        .key = key,
        .parent_rtc = parent,
        .success_cb = [this] { _probe.index_upload(); },
        .failure_cb = [this] { _probe.failed_index_upload(); },
        .on_req_cb = make_notify_cb(api_activity_type::object_upload, parent),
      },
      .display_str = to_string(upload_type::segment_index),
      .payload = std::move(buf),
    });
}

ss::future<download_result> remote::download_stream(
  const cloud_storage_clients::bucket_name& bucket,
  const remote_segment_path& path,
  const try_consume_stream& cons_str,
  retry_chain_node& parent,
  const std::string_view stream_label,
  const download_metrics& metrics,
  std::optional<cloud_storage_clients::http_byte_range> byte_range) {
    return io().download_stream(
      {
        // TODO: these metrics are for segments. Though this method appears to
        // only be used for inventory downlaods.
        .bucket = bucket,
        .key = cloud_storage_clients::object_key{path()},
        .parent_rtc = parent,
        .success_cb = [this] { _probe.successful_download(); },
        .success_size_cb =
          [this](size_t sz) { _probe.register_download_size(sz); },
        .failure_cb = [&metrics] { metrics.failed_download_metric(); },
        .backoff_cb = [&metrics] { metrics.download_backoff_metric(); },
        .client_acquire_cb = [this] { _probe.client_acquisition(); },
        // TODO: pass type in as an argument.
        .on_req_cb = make_notify_cb(
          api_activity_type::segment_download, parent),
        .measure_latency_cb =
          [&metrics] { return metrics.download_latency_measurement(); },
      },
      cons_str,
      stream_label,
      false,
      byte_range,
      [this](size_t ms) {
          _materialized->get_read_path_probe().download_throttled(ms);
      });
}

ss::future<download_result> remote::download_segment(
  const cloud_storage_clients::bucket_name& bucket,
  const remote_segment_path& segment_path,
  const try_consume_stream& cons_str,
  retry_chain_node& parent,
  std::optional<cloud_storage_clients::http_byte_range> byte_range) {
    return io().download_stream(
      {
        .bucket = bucket,
        .key = cloud_storage_clients::object_key{segment_path()},
        .parent_rtc = parent,
        .success_cb = [this] { _probe.successful_download(); },
        .success_size_cb =
          [this](size_t sz) { _probe.register_download_size(sz); },
        .failure_cb = [this] { _probe.failed_download(); },
        .backoff_cb = [this] { _probe.download_backoff(); },
        .client_acquire_cb = [this] { _probe.client_acquisition(); },
        .on_req_cb = make_notify_cb(
          api_activity_type::segment_download, parent),
        .measure_latency_cb = [this] { return _probe.segment_download(); },
      },
      cons_str,
      "segment",
      false,
      byte_range,
      [this](size_t ms) {
          _materialized->get_read_path_probe().download_throttled(ms);
      });
}

ss::future<download_result> remote::download_index(
  const cloud_storage_clients::bucket_name& bucket,
  const remote_segment_path& index_path,
  offset_index& ix,
  retry_chain_node& parent) {
    iobuf buffer;
    const auto dl_result = co_await io().download_object(
      {.transfer_details = {
         .bucket = bucket,
         .key = cloud_storage_clients::object_key{index_path},
         .parent_rtc = parent,
         .success_cb = [this]() { _probe.index_download(); },
         .failure_cb = [this]() { _probe.failed_index_download(); },
         .backoff_cb = [this]() { _probe.download_backoff(); },
         .on_req_cb = make_notify_cb(
           api_activity_type::object_download, parent),
       },
       .display_str = to_string(download_type::segment_index),
       .payload = buffer});
    if (dl_result == download_result::success) {
        ix.from_iobuf(std::move(buffer));
    }
    co_return dl_result;
}

ss::future<download_result>
remote::download_object(download_request download_request) {
    auto details = std::move(download_request.transfer_details);
    if (!details.on_req_cb.has_value()) {
        details.on_req_cb = make_notify_cb(
          api_activity_type::object_download, details.parent_rtc);
    }
    return io().download_object({
      .transfer_details = std::move(details),
      .display_str = to_string(download_request.type),
      .payload = download_request.payload,
    });
}

ss::future<download_result> remote::object_exists(
  const cloud_storage_clients::bucket_name& bucket,
  const cloud_storage_clients::object_key& path,
  retry_chain_node& parent,
  existence_check_type object_type) {
    co_return co_await io().object_exists(
      bucket, path, parent, fmt::to_string(object_type));
}

ss::future<download_result> remote::segment_exists(
  const cloud_storage_clients::bucket_name& bucket,
  const remote_segment_path& segment_path,
  retry_chain_node& parent) {
    co_return co_await io().object_exists(
      bucket,
      cloud_storage_clients::object_key{segment_path},
      parent,
      fmt::to_string(existence_check_type::segment));
}

ss::future<upload_result> remote::delete_object(
  const cloud_storage_clients::bucket_name& bucket,
  const cloud_storage_clients::object_key& path,
  retry_chain_node& parent) {
    return io().delete_object({
      .bucket = bucket,
      .key = path,
      .parent_rtc = parent,
      .on_req_cb = make_notify_cb(api_activity_type::segment_delete, parent),
    });
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
    return io().delete_objects(
      bucket,
      std::move(keys),
      parent,
      make_notify_cb(api_activity_type::segment_delete, parent));
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

ss::future<remote::list_result> remote::list_objects(
  const cloud_storage_clients::bucket_name& bucket,
  retry_chain_node& parent,
  std::optional<cloud_storage_clients::object_key> prefix,
  std::optional<char> delimiter,
  std::optional<cloud_storage_clients::client::item_filter> item_filter,
  std::optional<size_t> max_keys,
  std::optional<ss::sstring> continuation_token) {
    return io().list_objects(
      bucket,
      parent,
      prefix,
      delimiter,
      item_filter,
      max_keys,
      continuation_token);
}

ss::future<upload_result> remote::upload_object(upload_request req) {
    auto details = std::move(req.transfer_details);
    if (!details.on_req_cb.has_value()) {
        details.on_req_cb = make_notify_cb(
          api_activity_type::object_upload, details.parent_rtc);
    }
    return io().upload_object({
      .transfer_details = std::move(details),
      .display_str = to_string(req.type),
      .payload = std::move(req.payload),
      .accept_no_content_response = false,
    });
}

ss::future<api_activity_notification>
remote::subscribe(remote::event_filter& filter) {
    vassert(filter._hook.is_linked() == false, "Filter is already in use");
    filter._hook = {};
    _filters.push_back(filter);
    filter._promise.emplace();
    return filter._promise->get_future();
}

std::function<void(size_t)>
remote::make_notify_cb(api_activity_type t, retry_chain_node& retry) {
    return [this, t, &retry](size_t attempt_num) {
        notify_external_subscribers(
          api_activity_notification{.type = t, .is_retry = attempt_num > 1},
          retry);
    };
}

} // namespace cloud_storage
