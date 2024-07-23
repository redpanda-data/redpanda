// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "model/fundamental.h"
#include "serde/rw/envelope.h"
#include "serde/rw/uuid.h"

namespace cloud_storage {

// Label that can be used to uniquely identify objects written by a cluster.
struct remote_label
  : serde::envelope<remote_label, serde::version<0>, serde::compat_version<0>> {
    auto serde_fields() { return std::tie(cluster_uuid); }
    friend bool operator==(const remote_label&, const remote_label&) = default;
    remote_label() = default;
    explicit remote_label(model::cluster_uuid id)
      : cluster_uuid(id) {}

    // TODO: add a user-defined label.

    // The cluster UUID of a given cluster. This is critical in avoiding
    // collisions when multiple clusters use the same bucket.
    model::cluster_uuid cluster_uuid{};

    friend std::ostream&
    operator<<(std::ostream& os, const remote_label& label) {
        fmt::print(os, "{{cluster_uuid: {}}}", label.cluster_uuid);
        return os;
    }
};

} // namespace cloud_storage
