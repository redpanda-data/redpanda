/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/outcome.h"
#include "base/seastarx.h"
#include "cloud_storage/base_manifest.h"
#include "cluster/cloud_metadata/error_outcome.h"
#include "cluster/cloud_metadata/types.h"
#include "model/fundamental.h"
#include "serde/envelope.h"
#include "utils/named_type.h"

#include <seastar/core/lowres_clock.hh>

#include <rapidjson/document.h>

#include <chrono>

namespace cluster::cloud_metadata {

// Manifest containing information about cluster metadata.
struct cluster_metadata_manifest
  : public serde::envelope<
      cluster_metadata_manifest,
      serde::version<0>,
      serde::compat_version<0>>
  , public cloud_storage::base_manifest {
    using duration = std::chrono::milliseconds;

    auto serde_fields() {
        return std::tie(
          upload_time_since_epoch,
          cluster_uuid,
          metadata_id,
          controller_snapshot_offset,
          controller_snapshot_path,
          offsets_snapshots_by_partition);
    }

    // Upload time in milliseconds since the epoch. Informational only.
    duration upload_time_since_epoch{0};

    // Cluster UUID as determined at cluster bootstrap.
    model::cluster_uuid cluster_uuid{};

    // Monotonically increasing counter. Note that we can't use offsets specific
    // to clusters (e.g. controller offsets) since we need to distinguish
    // snapshots that were potentially written by different clusters.
    //
    // Consider what happens if we restore, upload new metadata from the new
    // cluster, and crash before deleting the old metadata. This counter ensures
    // we can distinguish the latest snapshot.
    cluster_metadata_id metadata_id{};

    // Controller log offset at which the last controller snapshot was written.
    // Useful in determining whether a local snapshot is behind a remote
    // snapshot (e.g. if locally the node just became a leader).
    model::offset controller_snapshot_offset{};

    // E.g. cluster_metadata/<cluster_uuid>/<id>/controller.snapshot
    // NOTE: <id> may be different than metadata_id if the controller has not
    // changed since the last snapshot.
    ss::sstring controller_snapshot_path;

    std::vector<std::vector<ss::sstring>> offsets_snapshots_by_partition;

    ss::future<> update(ss::input_stream<char> is) override;
    ss::future<iobuf> serialize_buf() const override;
    cloud_storage::remote_manifest_path get_manifest_path() const;
    cloud_storage::manifest_type get_manifest_type() const override {
        return cloud_storage::manifest_type::cluster_metadata;
    }

    bool operator==(const cluster_metadata_manifest& other) const {
        return std::tie(
                 upload_time_since_epoch,
                 cluster_uuid,
                 metadata_id,
                 controller_snapshot_offset,
                 controller_snapshot_path,
                 offsets_snapshots_by_partition)
               == std::tie(
                 other.upload_time_since_epoch,
                 other.cluster_uuid,
                 other.metadata_id,
                 other.controller_snapshot_offset,
                 other.controller_snapshot_path,
                 other.offsets_snapshots_by_partition);
    }

private:
    void load_from_json(const rapidjson::Document& doc);
    void to_json(std::ostream& out) const;
};

std::ostream& operator<<(std::ostream&, const cluster_metadata_manifest&);

using cluster_manifest_result
  = result<cluster_metadata_manifest, error_outcome>;

} // namespace cluster::cloud_metadata
