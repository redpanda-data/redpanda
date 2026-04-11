/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/metastore/manifest_io.h"

#include "cloud_storage_clients/types.h"
#include "serde/rw/rw.h"
#include "ssx/future-util.h"
#include "utils/retry_chain_node.h"

#include <seastar/coroutine/as_future.hh>

#include <fmt/format.h>

using namespace std::chrono_literals;

namespace cloud_topics::l1 {

namespace {
cloud_storage_clients::object_key
manifest_path(const cloud_storage::remote_label& remote_label) {
    return cloud_storage_clients::object_key{fmt::format(
      "level_one/meta/metastore/{}/manifest.bin", remote_label.cluster_uuid)};
}
} // namespace

manifest_io::manifest_io(
  cloud_io::remote& io, cloud_storage_clients::bucket_name bucket)
  : io_(io)
  , bucket_(std::move(bucket)) {}

ss::future<std::expected<size_t, manifest_io::error>>
manifest_io::upload_metastore_manifest(
  const cloud_storage::remote_label& remote_label,
  metastore_manifest manifest) {
    retry_chain_node retry(
      io_.as(), ss::lowres_clock::duration{30s}, 10ms, retry_strategy::backoff);

    auto buf = serde::to_iobuf(std::move(manifest));
    auto uploaded_size_bytes = buf.size_bytes();
    auto res = co_await ss::coroutine::as_future(io_.upload_object({
      .transfer_details =
        {
          .bucket = bucket_,
          .key = manifest_path(remote_label),
          .parent_rtc = retry,
        },
      .display_str = "l1::metastore_manifest",
      .payload = std::move(buf),
    }));

    if (res.failed()) {
        auto ex = res.get_exception();
        auto ec = ssx::is_shutdown_exception(ex) ? errc::shutting_down
                                                 : errc::failed;
        co_return std::unexpected(
          error(ec, "Exception while uploading metastore manifest: {}", ex));
    }

    switch (res.get()) {
        using enum cloud_io::upload_result;
    case success:
        co_return uploaded_size_bytes;
    case cancelled:
        co_return std::unexpected(error(errc::shutting_down));
    case failed:
        co_return std::unexpected(error(errc::failed));
    case timedout:
        co_return std::unexpected(error(errc::timedout));
    }
}

ss::future<std::expected<metastore_manifest, manifest_io::error>>
manifest_io::download_metastore_manifest(
  const cloud_storage::remote_label& remote_label) {
    retry_chain_node retry(
      io_.as(), ss::lowres_clock::duration{30s}, 10ms, retry_strategy::backoff);

    iobuf payload;
    auto res = co_await ss::coroutine::as_future(io_.download_object({
      .transfer_details =
        {
          .bucket = bucket_,
          .key = manifest_path(remote_label),
          .parent_rtc = retry,
        },
      .display_str = "l1::metastore_manifest",
      .payload = payload,
    }));

    if (res.failed()) {
        auto ex = res.get_exception();
        auto ec = ssx::is_shutdown_exception(ex) ? errc::shutting_down
                                                 : errc::failed;
        co_return std::unexpected(
          error(ec, "Exception while downloading metastore manifest: {}", ex));
    }

    switch (res.get()) {
        using enum cloud_io::download_result;
    case success:
        break;
    case notfound:
        co_return std::unexpected(error(errc::not_found));
    case failed:
        co_return std::unexpected(error(errc::failed));
    case timedout:
        co_return std::unexpected(error(errc::timedout));
    }

    // Deserialize the manifest
    auto manifest = serde::from_iobuf<metastore_manifest>(std::move(payload));
    co_return manifest;
}

} // namespace cloud_topics::l1

auto fmt::formatter<cloud_topics::l1::manifest_io::errc>::format(
  const cloud_topics::l1::manifest_io::errc& err,
  fmt::format_context& ctx) const -> decltype(ctx.out()) {
    std::string_view name = "unknown";
    switch (err) {
    case cloud_topics::l1::manifest_io::errc::timedout:
        name = "timedout";
        break;
    case cloud_topics::l1::manifest_io::errc::failed:
        name = "failed";
        break;
    case cloud_topics::l1::manifest_io::errc::shutting_down:
        name = "shutting_down";
        break;
    case cloud_topics::l1::manifest_io::errc::not_found:
        name = "not_found";
        break;
    }
    return fmt::format_to(
      ctx.out(), "cloud_topics::l1::manifest_io::errc::{}", name);
}
