/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "cluster/cloud_metadata/cluster_manifest.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "serde/envelope.h"

#include <seastar/util/bool_class.hh>

namespace cluster {

using wait_for_nodes = ss::bool_class<struct wait_tag>;

// Tracks the state of an on-going cluster recovery. This only exposes an
// interface to expose and modify the current status of recovery; an external
// caller should use these to actually drive recovery.
//
// This only tracks the parts of recovery that are not already tracked by
// another subsystem of the controller. I.e. topics, configs, etc. are tracked
// in other controller tables; but consumer offsets pending recovery are
// tracked here.
struct cluster_recovery_state
  : public serde::envelope<
      cluster_recovery_state,
      serde::version<0>,
      serde::compat_version<0>> {
public:
    cluster_recovery_state() = default;
    explicit cluster_recovery_state(
      cloud_metadata::cluster_metadata_manifest manifest,
      cloud_storage_clients::bucket_name bucket,
      wait_for_nodes wait)
      : manifest(std::move(manifest))
      , bucket(std::move(bucket))
      , wait_for_nodes(wait) {}
    auto serde_fields() {
        return std::tie(stage, manifest, bucket, wait_for_nodes, error_msg);
    }

    bool is_active() const {
        return !(
          stage == recovery_stage::complete || stage == recovery_stage::failed);
    }

    friend bool
    operator==(const cluster_recovery_state&, const cluster_recovery_state&)
      = default;

    // Current stage of this recovery.
    recovery_stage stage{recovery_stage::initialized};

    // Manifest that defines the desired end state of this recovery.
    cloud_metadata::cluster_metadata_manifest manifest{};

    // Bucket being recovered from.
    cloud_storage_clients::bucket_name bucket;

    // Whether the recovery should wait for the cluster to become at least the
    // size of the original cluster before proceeding with recovery.
    //
    // This may be desirable when the recovery was started as a part of cluster
    // bootstrap.
    wait_for_nodes wait_for_nodes;

    // Only applicable when failed.
    std::optional<ss::sstring> error_msg;
};

} // namespace cluster
