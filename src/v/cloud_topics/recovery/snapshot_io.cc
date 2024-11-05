// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_topics/recovery/snapshot_io.h"

#include "cloud_io/io_result.h"
#include "cloud_io/remote.h"
#include "cloud_io/transfer_details.h"
#include "cloud_storage_clients/types.h"
#include "cloud_topics/dl_version.h"
#include "cloud_topics/recovery/logger.h"
#include "utils/retry_chain_node.h"

#include <stdexcept>

namespace experimental::cloud_topics::recovery {

namespace {
constexpr std::string_view dot_snap_suffix = ".snap";
}

std::optional<ntp_snapshot_id>
ntp_snapshot_id::parse(ntp_snapshot_prefix prefix, std::string_view str) {
    auto str_prefix = std::string(prefix);

    if (unlikely(
          !str.starts_with(str_prefix) || !str.ends_with(dot_snap_suffix))) {
        return std::nullopt;
    }

    auto version_str = str.substr(
      str_prefix.size(),
      str.size() - str_prefix.size() - dot_snap_suffix.size());

    try {
        auto version = boost::lexical_cast<int64_t>(version_str);
        return ntp_snapshot_id(std::move(prefix), dl_version(version));
    } catch (const boost::bad_lexical_cast&) {
        return std::nullopt;
    }
}

snapshot_io::snapshot_io(
  cloud_io::remote* cloud_io, cloud_storage_clients::bucket_name bucket)
  : _cloud_io(cloud_io)
  , _bucket(std::move(bucket)) {}

ss::future<> snapshot_io::upload_snapshot(
  ntp_snapshot_id id, iobuf payload, retry_chain_node& rtc) {
    auto key = id.object_key();

    auto r = co_await _cloud_io->upload_object(cloud_io::upload_request{
      .transfer_details = cloud_io::transfer_details{
        .bucket = _bucket,
        .key = key,
        .parent_rtc = rtc,
      },
      .display_str = "ct-partition-snapshot",
      .payload = std::move(payload),
    });

    if (r != cloud_io::upload_result::success) {
        throw std::runtime_error(fmt::format(
          "Failed to upload snapshot of partition {}: {}", id.object_key(), r));
    }
}

ss::future<> snapshot_io::delete_snapshots_before(
  ntp_snapshot_id id, retry_chain_node& rtc) {
    const auto& prefix = id.prefix();

    vlog(
      lg.debug,
      "Deleting snapshots for prefix {} before {}",
      prefix.object_key(),
      id.object_key());

    auto list_result = co_await _cloud_io->list_objects(
      _bucket, rtc, prefix.object_key());

    if (!list_result) {
        throw std::runtime_error(fmt::format(
          "Failed to list prefix {}: {}",
          prefix.object_key(),
          list_result.error()));

        co_return;
    }

    vlog(
      lg.debug,
      "Found {} snapshots for prefix {}",
      list_result.value().contents.size(),
      prefix.object_key());

    for (const auto& item : list_result.value().contents) {
        auto item_id = ntp_snapshot_id::parse(prefix, item.key);
        if (!item_id) {
            vlog(
              lg.warn,
              "Failed to parse snapshot id from key: {}. Skipping.",
              item.key);
            continue;
        }

        if (*item_id < id) {
            vlog(lg.debug, "Deleting snapshot {}", item.key);

            auto delete_result = co_await _cloud_io->delete_object(
              cloud_io::transfer_details{
                .bucket = _bucket,
                .key = cloud_storage_clients::object_key{item.key},
                .parent_rtc = rtc,
              });

            if (delete_result != cloud_io::upload_result::success) {
                throw std::runtime_error(
                  fmt::format("Failed to delete snapshot {}", item.key));
            }
        } else {
            vlog(
              lg.trace,
              "Skipping snapshot {} as it is not older than {}",
              item.key,
              id.object_key());
        }
    }
}

ss::future<std::optional<ntp_snapshot_id>> snapshot_io::find_latest_snapshot(
  ntp_snapshot_prefix prefix, retry_chain_node& rtc) {
    vlog(
      lg.debug, "Finding latest snapshot for prefix {}", prefix.object_key());

    auto list_result = co_await _cloud_io->list_objects(
      _bucket, rtc, prefix.object_key());

    if (!list_result) {
        throw std::runtime_error(fmt::format(
          "Failed to list prefix {}: {}",
          prefix.object_key(),
          list_result.error()));
    }

    vlog(
      lg.debug,
      "Found {} snapshots for prefix {}",
      list_result.value().contents.size(),
      prefix.object_key());

    ntp_snapshot_id latest_id{prefix, dl_version{}};

    for (const auto& item : list_result.value().contents) {
        auto item_id = ntp_snapshot_id::parse(prefix, item.key);
        if (!item_id) {
            vlog(
              lg.warn,
              "Failed to parse snapshot id from key: {}. Skipping.",
              item.key);

            throw std::runtime_error(fmt::format(
              "Failed to parse snapshot id from key: {}", item.key));
        }

        if (latest_id < *item_id) {
            latest_id = *item_id;
        }
    }

    if (latest_id.version() == dl_version{}) {
        vlog(lg.debug, "No snapshots found for prefix {}", prefix.object_key());
        co_return std::nullopt;
    }

    vlog(
      lg.debug,
      "Latest snapshot for prefix {} is {}",
      prefix.object_key(),
      latest_id.object_key());

    co_return latest_id;
}

ss::future<iobuf>
snapshot_io::download_snapshot(ntp_snapshot_id id, retry_chain_node& rtc) {
    auto key = id.object_key();

    vlog(lg.debug, "Downloading snapshot {}", key);
    iobuf snapshot_buf;

    auto result = co_await _cloud_io->download_object(cloud_io::download_request{
      .transfer_details = cloud_io::transfer_details{
        .bucket = _bucket,
        .key = key,
        .parent_rtc = rtc,
      },
      .display_str = "ct-partition-snapshot",
      .payload = snapshot_buf,
    });

    if (result != cloud_io::download_result::success) {
        throw std::runtime_error(
          fmt::format("Failed to download snapshot of partition {}", key));
    }

    co_return std::move(snapshot_buf);
}

} // namespace experimental::cloud_topics::recovery
