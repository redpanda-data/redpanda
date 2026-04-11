/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "cloud_io/remote.h"
#include "cloud_storage/remote_label.h"
#include "cloud_topics/level_one/metastore/metastore_manifest.h"
#include "model/fundamental.h"
#include "utils/detailed_error.h"

#include <seastar/core/future.hh>

#include <expected>

namespace cloud_topics::l1 {

class manifest_io {
public:
    enum class errc {
        timedout,
        failed,
        shutting_down,
        not_found,
    };
    using error = detailed_error<errc>;

    manifest_io(
      cloud_io::remote& io, cloud_storage_clients::bucket_name bucket);

    // Uploads manifest to:
    // level_one/meta/metastore/{cluster_uuid}/manifest.bin
    // Uses the local cluster's UUID from storage::api.
    ss::future<std::expected<size_t, error>> upload_metastore_manifest(
      const cloud_storage::remote_label& remote_label,
      metastore_manifest manifest);

    // Downloads manifest from:
    // level_one/meta/metastore/{cluster_uuid}/manifest.bin
    ss::future<std::expected<metastore_manifest, error>>
    download_metastore_manifest(
      const cloud_storage::remote_label& remote_label);

private:
    cloud_io::remote& io_;
    cloud_storage_clients::bucket_name bucket_;
};

} // namespace cloud_topics::l1

template<>
struct fmt::formatter<cloud_topics::l1::manifest_io::errc>
  : fmt::formatter<std::string_view> {
    auto format(
      const cloud_topics::l1::manifest_io::errc&,
      fmt::format_context& ctx) const -> decltype(ctx.out());
};
