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

#include "cloud_storage/types.h"
#include "cloud_storage_clients/types.h"
#include "cluster/cloud_metadata/types.h"
#include "model/fundamental.h"

#include <string>

namespace cluster::cloud_metadata {

// E.g. /cluster_metadata/<cluster_uuid>
ss::sstring cluster_uuid_prefix(const model::cluster_uuid&);

// E.g. /cluster_metadata/<cluster_uuid>/manifests
ss::sstring cluster_manifests_prefix(const model::cluster_uuid&);

// E.g. /cluster_metadata/<cluster_uuid>/manifests/cluster_manifest.json
cloud_storage::remote_manifest_path
cluster_manifest_key(const model::cluster_uuid&, const cluster_metadata_id&);

// E.g. /cluster_metadata/<cluster_uuid>/<offset>/controller.snapshot
cloud_storage::remote_segment_path
controller_snapshot_key(const model::cluster_uuid&, const model::offset&);

// E.g. /cluster_metadata/<cluster_uuid>/<meta_id>
ss::sstring
cluster_metadata_prefix(const model::cluster_uuid&, const cluster_metadata_id&);

// E.g. /cluster_metadata/<cluster_uuid>/<meta_id>/offsets/<pid>/<idx>.snapshot
cloud_storage_clients::object_key offsets_snapshot_key(
  const model::cluster_uuid&,
  const cluster_metadata_id&,
  const model::partition_id&,
  size_t snapshot_idx);

} // namespace cluster::cloud_metadata
